#include <seastar/testing/test_case.hh>
#include "sstables/mx/reader.hh"
#include "test/lib/mutation_reader_assertions.hh"
#include "test/lib/simple_schema.hh"
#include "test/lib/sstable_utils.hh"
#include "utils/assert.hh"
#include "test/lib/sstable_test_env.hh"

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

std::vector<uint64_t> get_partition_positions(shared_sstable sst, reader_permit permit) {
    std::vector<uint64_t> result;
    {
        auto ir = make_index_reader(sst, permit, nullptr, use_caching::no, false);
        ir->advance_to(dht::partition_range::make_open_ended_both_sides()).get();
        ir->read_partition_data().get();
        auto close_ir = deferred_close(*ir);
        while (!ir->eof()) {
            result.push_back(ir->get_data_file_position());
            ir->advance_to_next_partition().get();
        }
    }
    return result;
}

struct inexact_partition_index : index_reader {
    nondeterministic_choice_stack& _ncs;
    std::span<const uint64_t> _positions;
    std::span<const dht::decorated_key> _keys;
    schema_ptr _s;

    int _lower = 0;
    int _upper = 0;

    inexact_partition_index(
        nondeterministic_choice_stack& ncs,
        std::span<const uint64_t> positions,
        std::span<const dht::decorated_key> keys,
        schema_ptr s)
    : _ncs(ncs)
    , _positions(positions)
    , _keys(keys)
    , _s(s) {

    }

    virtual future<> close() noexcept {
        return make_ready_future<>();
    }
    virtual data_file_positions_range data_file_positions() const {
        return {_positions[_lower], _positions[_upper]};
    }
    virtual future<std::optional<uint64_t>> last_block_offset() {
        abort();
    }
    virtual future<bool> advance_lower_and_check_if_present(dht::ring_position_view key) {
        abort();
    }
    virtual future<> advance_upper_past(position_in_partition_view pos) {
        abort();
    }
    virtual future<> advance_to_next_partition() {
        _lower += 1;
        return make_ready_future<>();
    }
    virtual indexable_element element_kind() const {
        return indexable_element::partition;
    }
    virtual future<> advance_to(dht::ring_position_view pos) {
        auto cmp = dht::ring_position_less_comparator(*_s);
        _lower = std::ranges::lower_bound(_keys, pos, cmp) - _keys.begin();
        switch (_ncs.choose()) {
            case 0:
                break;
            default:
                _lower = std::clamp<int>(_lower - 1, 0, _keys.size());
                _ncs.mark_last_choice();
                break;
        }
        return make_ready_future<>();
    }
    virtual future<> advance_after_existing(const dht::decorated_key& dk) {
        auto cmp = dht::ring_position_less_comparator(*_s);
        _lower = std::ranges::lower_bound(_keys, dk, cmp) - _keys.begin() + 1;
        return make_ready_future<>();
    }
    virtual future<> advance_to(position_in_partition_view pos) {
        abort();
    }
    virtual std::optional<sstables::deletion_time> partition_tombstone() {
        return std::nullopt;
    }
    virtual std::optional<partition_key> get_partition_key() {
        return std::nullopt;
    }
    virtual partition_key get_partition_key_prefix() {
        abort();
    }
    virtual bool partition_data_ready() const {
        return false;
    }
    virtual future<> read_partition_data() {
        return make_ready_future<>();
    }
    virtual future<> advance_reverse(position_in_partition_view pos) {
        abort();
    }
    virtual future<> advance_to(const dht::partition_range& pr) {
        auto cmp = dht::ring_position_less_comparator(*_s);
        if (pr.start()) {
            auto p = dht::ring_position_view(pr.start()->value(),
                    dht::ring_position_view::after_key(!pr.start()->is_inclusive()));
            _lower = std::ranges::lower_bound(_keys, p, cmp) - _keys.begin();
            switch (_ncs.choose_up_to(2)) {
                case 0:
                    break;
                case 1:
                    _lower = std::clamp<int>(_lower - 1, 0, _keys.size());
                    break;
            }   
        }
        if (pr.end()) {
            auto p = dht::ring_position_view(pr.end()->value(),
                    dht::ring_position_view::after_key(pr.end()->is_inclusive()));
            _upper = std::ranges::lower_bound(_keys, p, cmp) - _keys.begin();
            switch (_ncs.choose_up_to(2)) {
                case 0:
                    break;
                case 1:
                    _upper = std::clamp<int>(_upper + 1, 0, _keys.size());
                    break;
            }  
        } else {
            _upper = _keys.size();
        }
        testlog.trace("inexact_partition_index/advance_to: pr={}, _lower={}, _upper={}", pr, _lower, _upper);
        return make_ready_future<>();
    }
    virtual future<> advance_reverse_to_next_partition() {
        abort();
    }

    virtual std::optional<open_rt_marker> end_open_marker() const {
        abort();
    }
    virtual std::optional<open_rt_marker> reverse_end_open_marker() const {
        abort();
    }
    virtual clustered_index_cursor* current_clustered_cursor() {
        abort();
    }
    virtual future<> reset_clustered_cursor() {
        abort();
    }
    virtual uint64_t get_data_file_position() {
        return _positions[_lower];
    }
    virtual uint64_t get_promoted_index_size() {
        abort();
    }
    virtual bool eof() const {
        return _lower == int(_keys.size());
    }
};

SEASTAR_TEST_CASE(test_inexact_partition_index) {
    return sstables::test_env::do_with_async([] (sstables::test_env& env) {
        simple_schema table;
        auto permit = env.make_reader_permit();
        auto dks = table.make_pkeys(7);
        auto cks = table.make_ckeys(1);
        std::vector<mutation> muts;
        std::vector<int> filled_dks = {1, 3, 5};
        std::vector<dht::decorated_key> filled_dk_array;
        for (const auto& i : filled_dks) {
            filled_dk_array.push_back(dks[i]);
            mutation m(table.schema(), dks[i]);
            table.add_row(m, cks[0], "value");
            muts.push_back(std::move(m));
        }
        auto sst = make_sstable_containing(env.make_sstable(table.schema()), muts);
        std::vector<uint64_t> partition_positions = get_partition_positions(sst, permit);
        partition_positions.push_back(sst->data_size());
        {
            testlog.debug("sstable initialized");
            int i = 0;
            for (const auto& dk : filled_dks) {
                testlog.debug("{}: {} (dk={})", dk, partition_positions[i], dks[dk]);
                ++i;
            }
            testlog.debug("eof: {}", partition_positions.back());
        }
        nondeterministic_choice_stack ncs;
        int test_cases = 0;
        dht::partition_range pr;
        do {
            testlog.debug("Starting test case {}", test_cases);
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
                        table.schema()->full_slice(),
                        nullptr,
                        streamed_mutation::forwarding::no,
                        mutation_reader::forwarding::yes,
                        default_read_monitor(),
                        sstables::integrity_check::no,
                        std::make_unique<inexact_partition_index>(ncs, partition_positions, filled_dk_array, table.schema())
                    ));
                }
                for (int i = start_dk + !start_inclusive; i < end_dk + end_inclusive; ++i) {
                    if (auto it = std::ranges::find(filled_dks, i); it != filled_dks.end()) {
                        testlog.debug("Check produces {} (dk={})", i, muts[it - filled_dks.begin()].decorated_key());
                        reader->produces(muts[it - filled_dks.begin()]);
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
