/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include <source_location>

#include <boost/range/irange.hpp>
#include <boost/range/adaptor/uniqued.hpp>

#include <seastar/core/thread.hh>

#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/thread_test_case.hh>
#include "test/lib/test_services.hh"
#include "test/lib/mutation_source_test.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/dummy_sharder.hh"
#include "test/lib/reader_lifecycle_policy.hh"
#include "test/lib/log.hh"

#include "dht/sharder.hh"
#include "schema/schema_registry.hh"
#include "readers/forwardable_v2.hh"

// It has to be a container that does not invalidate pointers
static std::list<dummy_sharder> keep_alive_sharder;

// Filters out range tombstone changes which are redundant due to being
// followed by another range tombstone change with equal position.
//
// I.e. if the source reader emits multiple consecutive range range tombstone
// changes with equal positions, only the last will be ultimately emitted.
class trivial_rtc_removing_reader : public flat_mutation_reader_v2::impl {
    // Source reader.
    flat_mutation_reader_v2 _rd;
    // Stores the latest mutation fragment, if it was a range tombstone change.
    mutation_fragment_v2_opt _just_seen_rtc;
public:
    trivial_rtc_removing_reader(flat_mutation_reader_v2 rd)
        : impl(rd.schema(), rd.permit())
        , _rd(std::move(rd))
    {}
    virtual future<> fill_buffer() override {
        // The logic of this reader, in pseudocode:
        // for each mf in _rd:
        //     if mf is a range tombstone change with equal position to _just_seen_rtc:
        //         discard _just_seen_rtc
        //         put mf into _just_seen_rtc
        //     else if mf is a range tombstone change with different position to _just_seen_rtc (or _just_seen_rtc is empty):
        //         emit _just_seen_rtc if present
        //         put mf into _just_seen_rtc
        //     else:
        //         emit _just_seen_rtc if present
        //         emit mf
        while (!is_buffer_full()) {
            if (!_rd.is_buffer_empty()) {
                // for each mf in _rd:
                auto mf = _rd.pop_mutation_fragment();
                if (mf.is_range_tombstone_change()) {
                // if mf is a range_tombstone_change...
                    if (_just_seen_rtc
                        && position_in_partition::equal_compare(*_schema)(
                            mf.as_range_tombstone_change().position(),
                            _just_seen_rtc->as_range_tombstone_change().position())
                    ) {
                    // ...with equal position to _just_seen_rtc:
                        // discard _just_seen_rtc
                        // (It will be overwritten by mf later).
                    } else {
                    // ...with different position to _just_seen_rtc (or _just_seen_rtc is empty):
                        // emit _just_seen_rtc if present
                        if (_just_seen_rtc) {
                            push_mutation_fragment(std::move(*std::exchange(_just_seen_rtc, {})));
                        }
                    }
                    // put mf into _just_seen_rtc
                    _just_seen_rtc = std::move(mf);
                } else {
                // else:
                    // emit _just_seen_rtc if present
                    if (_just_seen_rtc) {
                        push_mutation_fragment(std::move(*std::exchange(_just_seen_rtc, {})));
                    }
                    // emit mf
                    push_mutation_fragment(std::move(mf));
                }
            } else if (!_rd.is_end_of_stream()) {
                co_await _rd.fill_buffer();
            } else if (_just_seen_rtc) {
                // If _just_seen_rtc was the last element in the stream, emit it.
                push_mutation_fragment(std::move(*std::exchange(_just_seen_rtc, {})));
            } else {
                _end_of_stream = true;
                break;
            }
            co_await coroutine::maybe_yield();
        }
    }
    virtual future<> next_partition() override {
        clear_buffer_to_next_partition();
        _just_seen_rtc = {};
        _end_of_stream = false;
        return _rd.next_partition();
    }
    virtual future<> fast_forward_to(const dht::partition_range& pr) override {
        clear_buffer();
        _just_seen_rtc = {};
        _end_of_stream = false;
        return _rd.fast_forward_to(pr);
    }
    virtual future<> fast_forward_to(position_range pr) override {
        clear_buffer();
        _just_seen_rtc = {};
        _end_of_stream = false;
        return _rd.fast_forward_to(std::move(pr));
    }
    virtual future<> close() noexcept override {
        return _rd.close();
    }
};

static auto make_populate(bool evict_paused_readers, bool single_fragment_buffer) {
    return [evict_paused_readers, single_fragment_buffer] (schema_ptr s, const std::vector<mutation>& mutations, gc_clock::time_point) mutable {
        // We need to group mutations that have the same token so they land on the same shard.
        std::map<dht::token, std::vector<frozen_mutation>> mutations_by_token;

        for (const auto& mut : mutations) {
            mutations_by_token[mut.token()].push_back(freeze(mut));
        }

        dummy_sharder sharder(s->get_sharder(), mutations_by_token);

        auto merged_mutations = boost::copy_range<std::vector<std::vector<frozen_mutation>>>(mutations_by_token | boost::adaptors::map_values);

        auto remote_memtables = make_lw_shared<std::vector<foreign_ptr<lw_shared_ptr<replica::memtable>>>>();
        for (unsigned shard = 0; shard < sharder.shard_count(); ++shard) {
            auto remote_mt = smp::submit_to(shard, [shard, gs = global_schema_ptr(s), &merged_mutations, sharder] {
                auto s = gs.get();
                auto mt = make_lw_shared<replica::memtable>(s);

                for (unsigned i = shard; i < merged_mutations.size(); i += sharder.shard_count()) {
                    for (auto& mut : merged_mutations[i]) {
                        mt->apply(mut.unfreeze(s));
                    }
                }

                return make_foreign(mt);
            }).get0();
            remote_memtables->emplace_back(std::move(remote_mt));
        }
        keep_alive_sharder.push_back(sharder);

        return mutation_source([&, remote_memtables, evict_paused_readers, single_fragment_buffer] (schema_ptr s,
                reader_permit permit,
                const dht::partition_range& range,
                const query::partition_slice& slice,
                tracing::trace_state_ptr trace_state,
                streamed_mutation::forwarding fwd_sm,
                mutation_reader::forwarding fwd_mr) mutable {
            auto factory = [remote_memtables, single_fragment_buffer] (
                    schema_ptr s,
                    reader_permit permit,
                    const dht::partition_range& range,
                    const query::partition_slice& slice,
                    tracing::trace_state_ptr trace_state,
                    mutation_reader::forwarding fwd_mr) {
                    auto reader = remote_memtables->at(this_shard_id())->make_flat_reader(s, std::move(permit), range, slice, std::move(trace_state),
                            streamed_mutation::forwarding::no, fwd_mr);
                    if (single_fragment_buffer) {
                        reader.set_max_buffer_size(1);
                    }
                    return reader;
            };

            auto lifecycle_policy = seastar::make_shared<test_reader_lifecycle_policy>(std::move(factory), evict_paused_readers);
            auto mr = make_multishard_combining_reader_v2_for_tests(keep_alive_sharder.back(), std::move(lifecycle_policy), s,
                    std::move(permit), range, slice, trace_state, fwd_mr);
            if (fwd_sm == streamed_mutation::forwarding::yes) {
                mr = make_forwardable(std::move(mr));
            }
            if (single_fragment_buffer) {
                // Recreating the evictable_reader conservatively closes the active range tombstone
                // (because it could have disappeared in the meantime), and -- if the data didn't
                // change after all -- reopens it immediately.
                //
                // The single_fragment_buffer variant of the test causes such recreations,
                // adding redundant range tombstone change pairs to the mutation fragment stream.
                // Some tests expect a particular stream, so they reject those redundant RTCs.
                //
                // We could modify the tests to check the semantics of the stream instead of its
                // exact form, but catching redundancy is a good thing in general,
                // so we opt for removing the redundant pairs only here, where they are expected.
                mr = make_flat_mutation_reader_v2<trivial_rtc_removing_reader>(std::move(mr));
            }
            return mr;
        });
    };
}

// Best run with SMP >= 2
SEASTAR_THREAD_TEST_CASE(test_multishard_combining_reader) {
    if (smp::count < 2) {
        std::cerr << "Cannot run test " << get_name() << " with smp::count < 2" << std::endl;
        return;
    }

    do_with_cql_env_thread([&] (cql_test_env& env) -> future<> {
        run_mutation_source_tests(make_populate(false, false));
        return make_ready_future<>();
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_multishard_combining_reader_evict_paused) {
    if (smp::count < 2) {
        std::cerr << "Cannot run test " << get_name() << " with smp::count < 2" << std::endl;
        return;
    }

    do_with_cql_env_thread([&] (cql_test_env& env) -> future<> {
        run_mutation_source_tests(make_populate(true, false));
        return make_ready_future<>();
    }).get();
}

// Single fragment buffer tests are extremely slow, so the
// run_mutation_source_tests execution is split

SEASTAR_THREAD_TEST_CASE(test_multishard_combining_reader_with_tiny_buffer) {
    if (smp::count < 2) {
        std::cerr << "Cannot run test " << get_name() << " with smp::count < 2" << std::endl;
        return;
    }

    do_with_cql_env_thread([&] (cql_test_env& env) -> future<> {
        run_mutation_source_tests_plain(make_populate(true, true), true);
        return make_ready_future<>();
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_multishard_combining_reader_with_tiny_buffer_reverse) {
    if (smp::count < 2) {
        std::cerr << "Cannot run test " << get_name() << " with smp::count < 2" << std::endl;
        return;
    }

    do_with_cql_env_thread([&] (cql_test_env& env) -> future<> {
        run_mutation_source_tests_reverse(make_populate(true, true), true);
        return make_ready_future<>();
    }).get();
}
