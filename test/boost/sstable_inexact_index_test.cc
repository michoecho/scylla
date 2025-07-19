#include <seastar/testing/test_case.hh>
#include "sstables/mx/reader.hh"
#include "test/lib/mutation_reader_assertions.hh"
#include "test/lib/simple_schema.hh"
#include "test/lib/sstable_utils.hh"
#include "utils/assert.hh"
#include "test/lib/sstable_test_env.hh"
#include "partition_slice_builder.hh"

class nondeterministic_choice_stack {
    std::vector<std::pair<int, bool>> choices;
    int next_frame = 0;
public:
    int choose() {
        if (next_frame >= int(choices.size())) {
            choices.emplace_back(0, false);
        } else if (next_frame + 1 == int(choices.size())) {
            choices.back().first += 1;
        }
        return choices[next_frame++].first;
    }
    int choose_up_to(int n) {
        int result = choose();
        if (result >= n) {
            mark_last_choice();
        }
        return result;
    }
    void mark_last_choice() {
        SCYLLA_ASSERT(next_frame > 0);
        if (next_frame != int(choices.size())) {
            SCYLLA_ASSERT(choices[next_frame - 1].second);
        }
        choices[next_frame - 1].second = true;
    }
    bool rewind() {
        while (!choices.empty() && choices.back().second) {
            choices.pop_back();
        }
        next_frame = 0;
        return choices.size() > 0;
    }
};

std::vector<uint64_t> get_ck_positions(shared_sstable sst, reader_permit permit) {
    std::vector<uint64_t> result;
    auto abstract_r = sst->make_index_reader(permit, nullptr, use_caching::no, false);
    auto& r = dynamic_cast<index_reader&>(*abstract_r);
    r.advance_to(dht::partition_range::make_open_ended_both_sides()).get();
    r.read_partition_data().get();
    auto close_ir = deferred_close(r);

    while (!r.eof()) {
        auto partition_pos = r.data_file_positions().start;
        sstables::clustered_index_cursor* cur = r.current_clustered_cursor();
        while (auto ei_opt = cur->next_entry().get()) {
            result.push_back(ei_opt.value().offset + partition_pos);
        }
        r.advance_to_next_partition().get();
    }
    return result;
}

std::vector<uint64_t> get_partition_positions(shared_sstable sst, reader_permit permit) {
    std::vector<uint64_t> result;
    {
        auto ir = sst->make_index_reader(permit, nullptr, use_caching::no, false);
        ir->advance_to(dht::partition_range::make_open_ended_both_sides()).get();
        ir->read_partition_data().get();
        auto close_ir = deferred_close(*ir);
        while (!ir->eof()) {
            result.push_back(ir->data_file_positions().start);
            ir->advance_to_next_partition().get();
        }
    }
    return result;
}

struct inexact_partition_index : abstract_index_reader {
    nondeterministic_choice_stack& _ncs;
    std::vector<uint64_t> _positions;
    std::span<const uint64_t> _pk_positions;
    std::span<const uint64_t> _ck_positions;
    std::span<const dht::decorated_key> _pks;
    std::span<const clustering_key> _cks;
    schema_ptr _s;

    int _lower = 0;
    int _upper = 0;

    inexact_partition_index(
        nondeterministic_choice_stack& ncs,
        std::span<const uint64_t> pk_positions,
        std::span<const uint64_t> ck_positions,
        std::span<const dht::decorated_key> pks,
        std::span<const clustering_key> cks,
        schema_ptr s)
    : _ncs(ncs)
    , _pk_positions(pk_positions)
    , _ck_positions(ck_positions)
    , _pks(pks)
    , _cks(cks)
    , _s(s) {
        _positions.insert(_positions.end(), ck_positions.begin(), ck_positions.end());
        _positions.insert(_positions.end(), pk_positions.begin(), pk_positions.end());
        std::ranges::sort(_positions);
    }

    future<> close() noexcept override {
        return make_ready_future<>();
    }
    data_file_positions_range data_file_positions() const override {
        return {_positions[_lower], _positions[_upper]};
    }
    future<std::optional<uint64_t>> last_block_offset() override {
        abort();
    }
    future<bool> advance_lower_and_check_if_present(dht::ring_position_view key) override {
        abort();
    }
    future<> advance_upper_past(position_in_partition_view pos) override {
        auto cmp = position_in_partition::less_compare(*_s);
        auto pk_idx = pos_idx_to_pk_idx(_lower);
        _upper = pk_idx_to_pos_idx(pk_idx);
        size_t ck_idx = std::ranges::upper_bound(_cks, pos, cmp) - _cks.begin();
        _upper = (ck_idx == _cks.size()) ? _upper + 1 : _upper + ck_idx;
        testlog.trace("inexact_partition_index/advance_upper_past: _lower={}, _upper={}, pk_idx={}", _lower, _upper, pk_idx);
        return make_ready_future<>();
    }
    future<> advance_to_next_partition() override {
        auto pk_idx = pos_idx_to_pk_idx(_lower);
        testlog.trace("&inexact_partition_index/advance_to_next_partition: _lower={}, _upper={}, pk_idx={}", _lower, _upper, pk_idx);
        pk_idx += 1;
        _lower = pk_idx_to_pos_idx(pk_idx);
        testlog.trace("inexact_partition_index/advance_to_next_partition: _lower={}, _upper={}, pk_idx={}", _lower, _upper, pk_idx);
        return make_ready_future<>();
    }
    indexable_element element_kind() const override {
        if (eof()) {
            return indexable_element::partition;
        }
        if (_positions[_lower] == *std::ranges::lower_bound(_pk_positions, _positions[_lower])) {
            return indexable_element::partition;
        } else {
            return indexable_element::cell;
        }
    }
    uint64_t pk_idx_to_pos_idx(uint64_t pk_idx) {
        return std::ranges::lower_bound(_positions, _pk_positions[pk_idx]) - _positions.begin();
    }
    uint64_t pos_idx_to_pk_idx(uint64_t pos_idx) {
        return std::ranges::upper_bound(_pk_positions, _positions[pos_idx]) - _pk_positions.begin() - 1;
    }
    future<> advance_past_definitely_present_partition(const dht::decorated_key& dk) override {
        auto cmp = dht::ring_position_less_comparator(*_s);
        size_t pk_idx = std::ranges::lower_bound(_pks, dk, cmp) - _pks.begin() + 1;
        _lower = pk_idx_to_pos_idx(pk_idx);
        testlog.trace("inexact_partition_index/advance_past_definitely_present_partition: _lower={}, _upper={}, pk_idx={}", _lower, _upper, pk_idx);
        return make_ready_future<>();
    }
    future<> advance_to_definitely_present_partition(const dht::decorated_key& dk) override {
        auto cmp = dht::ring_position_less_comparator(*_s);
        size_t pk_idx = std::ranges::lower_bound(_pks, dk, cmp) - _pks.begin();
        _lower = pk_idx_to_pos_idx(pk_idx);
        testlog.trace("inexact_partition_index/advance_to_definitely_present_partition: _lower={}, _upper={}, pk_idx={}", _lower, _upper, pk_idx);
        return make_ready_future<>();
    }
    future<> advance_to(position_in_partition_view pos) override {
        auto cmp = position_in_partition::less_compare(*_s);
        auto pk_idx = pos_idx_to_pk_idx(_lower);
        testlog.trace("@inexact_partition_index/advance_to(pipv={}): _lower={}, _upper={}, pk_idx={}", pos, _lower, _upper, pk_idx);
        size_t ck_idx = std::max<int>(0, std::ranges::upper_bound(_cks, pos, cmp) - _cks.begin() - 1);
        _lower = pk_idx * (_cks.size() + 1) + ck_idx;
        testlog.trace("inexact_partition_index/advance_to(pipv={}): _lower={}, _upper={}, pk_idx={}", pos, _lower, _upper, pk_idx);
        return make_ready_future<>();
    }
    std::optional<sstables::deletion_time> partition_tombstone() override {
        return sstables::deletion_time {
            std::numeric_limits<int32_t>::max(),
            std::numeric_limits<int64_t>::min(),
        };
    }
    std::optional<partition_key> get_partition_key() override {
        switch (_ncs.choose()) {
            case 0:
                return std::nullopt;
            default:
                _ncs.mark_last_choice();
                return _pks[pos_idx_to_pk_idx(_lower)].key();
        }
    }
    bool partition_data_ready() const override {
        return true;
    }
    future<> read_partition_data() override {
        return make_ready_future<>();
    }
    future<> advance_reverse(position_in_partition_view pos) override {
        abort();
    }
    future<> advance_to(const dht::partition_range& pr) override {
        auto cmp = dht::ring_position_less_comparator(*_s);
        if (pr.start()) {
            auto rpv = dht::ring_position_view::for_range_start(pr);
            auto pk_idx = std::ranges::lower_bound(_pks, rpv, cmp) - _pks.begin();
            if (pk_idx != 0) {
                switch (_ncs.choose_up_to(2)) {
                    case 0:
                        break;
                    case 1:
                        pk_idx = std::clamp<int>(pk_idx - 1, 0, _pks.size());
                        break;
                }
            }
            _lower = pk_idx_to_pos_idx(pk_idx);
        } else {
            _lower = 0;
        }
        if (pr.end()) {
            auto rpv = dht::ring_position_view::for_range_end(pr);
            size_t pk_idx = std::ranges::upper_bound(_pks, rpv, cmp) - _pks.begin();
            if (pk_idx != _pks.size()) {
                switch (_ncs.choose_up_to(2)) {
                    case 0:
                        break;
                    case 1:
                        pk_idx = std::clamp<int>(pk_idx + 1, 0, _pks.size());
                        break;
                }
            }
            _upper = pk_idx_to_pos_idx(pk_idx);
        } else {
            _upper = _positions.size() - 1;
        }
        testlog.trace("inexact_partition_index/advance_to(pr={}), _lower={}, _upper={}", pr, _lower, _upper);
        return make_ready_future<>();
    }
    future<> advance_reverse_to_next_partition() override {
        abort();
    }
    std::optional<open_rt_marker> end_open_marker() const override {
        return std::nullopt;
    }
    std::optional<open_rt_marker> reverse_end_open_marker() const override {
        abort();
    }
    future<> prefetch_lower_bound(position_in_partition_view pos) override {
        return make_ready_future<>();
    }
    bool eof() const override {
        return _lower == int(_positions.size());
    }
};

SEASTAR_TEST_CASE(test_inexact_partition_index) {
    return sstables::test_env::do_with_async([] (sstables::test_env& env) {
        env.manager().set_promoted_index_block_size(1); // force entry for each row
        auto row_value = std::string(1024, 'a');
        simple_schema table;
        auto permit = env.make_reader_permit();

        std::vector<dht::decorated_key> dks;
        for (size_t i = 0; i < 7; ++i) {
            auto pk = partition_key::from_single_value(*table.schema(), serialized(format("pk{:010d}", i)));
            dks.push_back(dht::decorate_key(*table.schema(), pk));
        }
        std::ranges::sort(dks, dht::ring_position_less_comparator(*table.schema()));

        utils::chunked_vector<mutation> muts;
        std::vector<int> filled_dk_indices = {1, 3, 5};
        auto cks = table.make_ckeys(3);
        std::vector<dht::decorated_key> filled_dks;
        for (const auto& i : filled_dk_indices) {
            filled_dks.push_back(dks[i]);
            mutation m(table.schema(), dks[i]);
            for (const auto& ck : cks) {
                table.add_row(m, ck, row_value);
            }
            muts.push_back(std::move(m));
        }
        auto sst = make_sstable_containing(env.make_sstable(table.schema()), muts);
        std::vector<uint64_t> partition_positions = get_partition_positions(sst, permit);
        std::vector<uint64_t> ck_positions = get_ck_positions(sst, permit);
        partition_positions.push_back(sst->data_size());
        {
            testlog.debug("sstable initialized");
            size_t i = 0;
            size_t j = 0;
            for (const auto& dk : filled_dk_indices) {
                testlog.debug("{}: {} (dk={})", dk, partition_positions[i], dks[dk]);
                while (j < ck_positions.size() && ck_positions[j] < partition_positions[i + 1]) {
                    testlog.debug(" ck at {}", ck_positions[j]);
                    ++j;
                }
                ++i;
            }
            testlog.debug("eof: {}", partition_positions.back());
        }
        nondeterministic_choice_stack ncs;
        int test_cases = 0;
        dht::partition_range pr;
        do {
            testlog.debug("Starting test case {}", test_cases);
            // auto slice = bool(ncs.choose_up_to(1))
            //     ? table.schema()->full_slice()
            //     : partition_slice_builder(*table.schema())
            //         .with_range(query::clustering_range::make_singular(cks[1]))
            //         .build();
            auto range = query::clustering_range::make_singular(cks[1]);
            auto slice = partition_slice_builder(*table.schema())
                     .with_range(range)
                     .build();
            ++test_cases;
            std::optional<mutation_reader_assertions> reader;
            auto max_ranges = 1 + ncs.choose_up_to(1);
            int last_dk = -1;
            bool last_inclusive = false;
            for (int range_index = 0; range_index <= max_ranges; ++range_index) {
                auto start_dk = last_dk;
                if (last_inclusive) {
                    start_dk += 1;
                }
                if (start_dk >= int(dks.size())) {
                    break;
                }
                start_dk += ncs.choose_up_to(dks.size() - 1 - start_dk);
                bool start_inclusive;
                if (start_dk == -1 || (start_dk == last_dk && last_inclusive)) {
                    start_inclusive = true;
                } else {
                    start_inclusive = bool(ncs.choose_up_to(1));
                }
                int end_dk = start_dk;
                if (!start_inclusive || start_dk == -1) {
                    end_dk += 1;
                }
                end_dk += ncs.choose_up_to(dks.size() - end_dk);
                bool end_inclusive;
                if (end_dk == int(dks.size()) || (end_dk == start_dk)) {
                    end_inclusive = true;
                } else {
                    end_inclusive = bool(ncs.choose_up_to(1));
                }
                if (start_dk == -1) {
                    if (end_dk == int(dks.size())) {
                        pr = dht::partition_range::make_open_ended_both_sides();
                    } else {
                        pr = dht::partition_range::make_ending_with(dht::partition_range::bound(dks[end_dk], end_inclusive));
                    }
                } else if (end_dk == int(dks.size())) {
                    pr = dht::partition_range::make_starting_with(dht::partition_range::bound(dks[start_dk], start_inclusive));
                } else {
                    pr = dht::partition_range::make(
                        dht::partition_range::bound(dks[start_dk], start_inclusive),
                        dht::partition_range::bound(dks[end_dk], end_inclusive)
                    );
                }
                if (reader) {
                    testlog.debug("Forward to {}{}, {}{} (pr={})",
                        start_inclusive ? '[' : '(',
                        start_dk,
                        end_dk,
                        end_inclusive ? ']' : ')',
                        pr
                    );
                    reader->fast_forward_to(pr);
                } else {
                    testlog.debug("Create for {}{}, {}{} (pr={})",
                        start_inclusive ? '[' : '(',
                        start_dk,
                        end_dk,
                        end_inclusive ? ']' : ')',
                        pr
                    );
                    reader = assert_that(sstables::mx::make_reader_with_index_reader(
                        sst,
                        table.schema(),
                        permit,
                        pr,
                        slice,
                        nullptr,
                        streamed_mutation::forwarding::no,
                        mutation_reader::forwarding::yes,
                        default_read_monitor(),
                        sstables::integrity_check::no,
                        std::make_unique<inexact_partition_index>(ncs, partition_positions, ck_positions, filled_dks, cks, table.schema())
                    ));
                }
                for (int i = start_dk + !start_inclusive; i < end_dk + end_inclusive; ++i) {
                    if (auto it = std::ranges::find(filled_dk_indices, i); it != filled_dk_indices.end()) {
                        testlog.debug("Check produces {} (dk={})", i, muts[it - filled_dk_indices.begin()].decorated_key());
                        reader->produces(muts[it - filled_dk_indices.begin()].sliced({range}));
                    }
                }
                last_dk = end_dk;
                last_inclusive = end_inclusive;
            }
            testlog.debug("Check produces end of stream");
            reader->produces_end_of_stream();
        } while (ncs.rewind());
    });
}
