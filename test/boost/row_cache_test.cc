/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include <boost/test/unit_test.hpp>
#include <seastar/core/sleep.hh>
#include <seastar/util/backtrace.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <boost/algorithm/cxx11/any_of.hpp>
#include <seastar/util/closeable.hh>

#include <seastar/testing/test_case.hh>
#include "test/lib/mutation_assertions.hh"
#include "test/lib/flat_mutation_reader_assertions.hh"
#include "test/lib/mutation_source_test.hh"

#include "schema_builder.hh"
#include "test/lib/simple_schema.hh"
#include "row_cache.hh"
#include <seastar/core/thread.hh>
#include "replica/memtable.hh"
#include "partition_slice_builder.hh"
#include "mutation_rebuilder.hh"
#include "service/migration_manager.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/memtable_snapshot_source.hh"
#include "test/lib/log.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "test/lib/random_utils.hh"

#include <boost/range/algorithm/min_element.hpp>
#include "readers/from_mutations_v2.hh"
#include "readers/delegating_v2.hh"
#include "readers/empty_v2.hh"

using namespace std::chrono_literals;

static thread_local api::timestamp_type next_timestamp = 1;

snapshot_source make_decorated_snapshot_source(snapshot_source src, std::function<mutation_source(mutation_source)> decorator) {
    return snapshot_source([src = std::move(src), decorator = std::move(decorator)] () mutable {
        return decorator(src());
    });
}

mutation_source make_source_with(mutation m) {
    return mutation_source([m] (schema_ptr s, reader_permit permit, const dht::partition_range&, const query::partition_slice&, const io_priority_class&, tracing::trace_state_ptr, streamed_mutation::forwarding fwd) {
        assert(m.schema() == s);
        return make_flat_mutation_reader_from_mutations_v2(s, std::move(permit), {m}, std::move(fwd));
    });
}

// It is assumed that src won't change.
snapshot_source snapshot_source_from_snapshot(mutation_source src) {
    return snapshot_source([src = std::move(src)] {
        return src;
    });
}

bool has_key(row_cache& cache, const dht::decorated_key& key) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto range = dht::partition_range::make_singular(key);
    auto reader = cache.make_reader(cache.schema(), semaphore.make_permit(), range);
    auto close_reader = deferred_close(reader);
    auto mo = read_mutation_from_flat_mutation_reader(reader).get0();
    if (!bool(mo)) {
        return false;
    }
    return !mo->partition().empty();
}

void verify_has(row_cache& cache, const dht::decorated_key& key) {
    BOOST_REQUIRE(has_key(cache, key));
}

void verify_does_not_have(row_cache& cache, const dht::decorated_key& key) {
    BOOST_REQUIRE(!has_key(cache, key));
}

dht::partition_range make_single_partition_range(schema_ptr& s, int pkey) {
    auto pk = partition_key::from_exploded(*s, { int32_type->decompose(pkey) });
    auto dk = dht::decorate_key(*s, pk);
    return dht::partition_range::make_singular(dk);
}

SEASTAR_TEST_CASE(test_concurrent_reads_and_eviction) {
    setup_preempt_fd();
    std::string filepath = fmt::format("{}.{}.preempt", getpid(), this_shard_id());
    fmt::print("{}\n", filepath);
    return seastar::async([] {
        random_mutation_generator gen(random_mutation_generator::generate_counters::no);
        gen.set_key_cardinality(16);
        memtable_snapshot_source underlying(gen.schema());
        schema_ptr s = gen.schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;

        auto m0 = gen();
        m0.partition().make_fully_continuous();
        circular_buffer<mutation> versions;
        size_t last_generation = 0;
        size_t cache_generation = 0; // cache contains only versions >= than this
        underlying.apply(m0);
        versions.emplace_back(m0);

        cache_tracker tracker;
        row_cache cache(s, snapshot_source([&] { return underlying(); }), tracker);

        auto pr = dht::partition_range::make_singular(m0.decorated_key());
        auto make_reader = [&] (const query::partition_slice& slice) {
            auto rd = cache.make_reader(s, semaphore.make_permit(), pr, slice);
            return rd;
        };

        const int n_readers = 1;
        std::vector<size_t> generations(n_readers);
        auto gc_versions = [&] {
            auto n_live = last_generation - *boost::min_element(generations) + 1;
            while (versions.size() > n_live) {
                versions.pop_front();
            }
        };

        bool done = false;
        auto readers = parallel_for_each(boost::irange(0, n_readers), [&] (auto id) {
            generations[id] = last_generation;
            return seastar::async([&, id] {
                while (!done) {
                    auto oldest_generation = cache_generation;
                    generations[id] = oldest_generation;
                    gc_versions();

                    auto fwd_ranges = gen.make_random_ranges(1);
                    auto slice = partition_slice_builder(*s)
                        .with_ranges(fwd_ranges)
                        .build();

                    auto native_slice = slice;

                    auto rd = make_reader(slice);
                    auto desc = 0;
                    auto close_rd = deferred_close(rd);
                    mutation_opt actual_opt;
                    try {
                        actual_opt = read_mutation_from_flat_mutation_reader(rd).get0();
                    } catch (const std::exception& e) {
                        throw;
                    }
                    BOOST_REQUIRE(actual_opt);
                    auto actual = *actual_opt;

                    auto&& ranges = native_slice.row_ranges(*rd.schema(), actual.key());
                    actual.partition().mutable_row_tombstones().trim(*rd.schema(), ranges);
                    actual = std::move(actual).compacted();

                    auto n_to_consider = last_generation - oldest_generation + 1;
                    auto possible_versions = boost::make_iterator_range(versions.end() - n_to_consider, versions.end());
                    if (!boost::algorithm::any_of(possible_versions, [&] (const mutation& m) {
                        auto m2 = m.sliced(fwd_ranges);
                        m2 = std::move(m2).compacted();
                        if (n_to_consider == 1) {
                            assert_that(actual).is_equal_to(m2);
                        }
                        return m2 == actual;
                    })) {
                        BOOST_FAIL(format("Mutation read doesn't match any expected version, slice: {}, read: {}\nexpected: [{}]",
                            slice, actual, ::join(",\n", possible_versions)));
                    }
                }
            }).finally([&, id] {
                done = true;
            });
        });

        int n_updates = 100;
        while (!done && n_updates--) {
            auto m2 = gen();
            m2.partition().make_fully_continuous();

            auto mt = make_lw_shared<replica::memtable>(m2.schema());
            mt->apply(m2);
            cache.update(row_cache::external_updater([&] () noexcept {
                auto snap = underlying();
                underlying.apply(m2);
                auto new_version = versions.back() + m2;
                versions.emplace_back(std::move(new_version));
                ++last_generation;
            }), *mt).get();
            cache_generation = last_generation;

            yield().get();
            tracker.region().evict_some();

            // Don't allow backlog to grow too much to avoid bad_alloc
            const auto max_active_versions = 7;
            while (!done && versions.size() > max_active_versions) {
                yield().get();
            }
        }

        done = true;
        readers.get();

        assert_that(cache.make_reader(s, semaphore.make_permit()))
            .produces(versions.back());
        desetup_preempt_fd();
    });
}
