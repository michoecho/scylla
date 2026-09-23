/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Tests of paging and reconciliation. They read from simulated replicas
// through the production read path, and compare the pages with the complete
// answer of the read model. See test/lib/paged_read.hh.

#undef SEASTAR_TESTING_MAIN

#include <cstdlib>
#include <map>

#include <boost/test/unit_test.hpp>
#include <fmt/ranges.h>
#include <seastar/testing/thread_test_case.hh>

#include "db/config.hh"
#include "db/extensions.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/log.hh"
#include "test/lib/paged_read.hh"
#include "test/lib/random_utils.hh"
#include "tombstone_gc_extension.hh"

using namespace tests::read_model;
using namespace tests::paged_read;

BOOST_AUTO_TEST_SUITE(paged_read_test)

namespace {

const std::string_view keyspace = "paged_read";

// Registers the tombstone_gc extension, so that a table can disable
// tombstone GC.
cql_test_config config_with_tombstone_gc_extension() {
    auto ext = std::make_shared<db::extensions>();
    ext->add_schema_extension<tombstone_gc_extension>(tombstone_gc_extension::NAME);
    return cql_test_config(seastar::make_shared<db::config>(ext));
}

// Creates a keyspace and a table of the read model, and runs `f` with a
// harness of the table.
void with_harness(std::function<void(harness&)> f) {
    do_with_cql_env_thread([f = std::move(f)] (cql_test_env& env) {
        env.execute_cql(fmt::format("CREATE KEYSPACE {} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}"
                " AND tablets = {{'enabled': 'false'}}", keyspace)).get();
        harness h(env, keyspace, "cf");
        f(h);
    }, config_with_tombstone_gc_extension()).get();
}

// Runs `c`, and reports a test error for the properties which the run
// violates. Returns the outcome of the run.
outcome run_and_check(harness& hs, const read_case& c) {
    const auto expected = evaluate(*hs.schema(), complete_history(c.history), c.query);
    testlog.debug("Running {}", describe(c));
    auto o = hs.run(c);
    const auto violations = check(o, expected);
    if (!violations.empty()) {
        BOOST_ERROR(report(c, o, expected, violations));
    }
    return o;
}

// The number of lines of the traces of `o` which contain `text`.
size_t count_trace_lines(const outcome& o, std::string_view text) {
    size_t count = 0;
    for (const auto& p : o.pages) {
        count += std::ranges::count_if(p.trace, [&] (const std::string& line) { return line.find(text) != std::string::npos; });
    }
    return count;
}

// Partition 1 has a static cell and rows 1 to 4, and row 3 is deleted.
// Partition 2 has only a static cell. Partition 3 has rows 1 to 3, and a
// range deletion of [2, 3) covers row 2. Partition 4 has only a deleted row.
history mixed_partitions() {
    return {
        static_cell_write{1, 5, 1},
        regular_cell_write{1, 1, regular_column::v1, 11, 2},
        regular_cell_write{1, 2, regular_column::v1, 12, 3},
        regular_cell_write{1, 3, regular_column::v1, 13, 4},
        regular_cell_write{1, 4, regular_column::v2, 14, 5},
        row_deletion{1, 3, 6},

        static_cell_write{2, 6, 7},

        row_marker_write{3, 1, 8},
        regular_cell_write{3, 2, regular_column::v1, 32, 9},
        regular_cell_write{3, 3, regular_column::v2, 33, 10},
        range_deletion{3, bound{2, true}, bound{3, false}, 11},

        regular_cell_write{4, 1, regular_column::v1, 41, 12},
        row_deletion{4, 1, 13},
    };
}

std::vector<select_query> paging_queries() {
    return {
        select_query{},
        select_query{.select_s = false},
        select_query{.partitions = std::vector<int32_t>{1, 3}},
        select_query{.partitions = std::vector<int32_t>{1}, .reversed = true},
        select_query{.ck_start = bound{2, true}},
        select_query{.distinct = true, .select_v1 = false, .select_v2 = false},
        select_query{.filter = {{column::v1, comparison::gt, 11}}},
        select_query{.per_partition_limit = 1},
        select_query{.limit = 3},
    };
}

// Runs every paging query with every page size in `page_sizes`.
void run_paging_queries(harness& hs, const placed_history& h, read_options opts, std::initializer_list<int32_t> page_sizes) {
    for (const auto& q : paging_queries()) {
        for (auto page_size : page_sizes) {
            opts.page_size = page_size;
            run_and_check(hs, read_case{h, q, opts});
        }
    }
}

} // anonymous namespace

// A single replica has nothing to reconcile.
SEASTAR_THREAD_TEST_CASE(test_single_replica) {
    with_harness([] (harness& hs) {
        run_paging_queries(hs, on_replicas(mixed_partitions(), 0b1), read_options{.replica_count = 1}, {0, 1, 2, 3, 100});
    });
}

// Identical replicas have matching digests.
SEASTAR_THREAD_TEST_CASE(test_identical_replicas) {
    with_harness([] (harness& hs) {
        run_paging_queries(hs, on_replicas(mixed_partitions(), 0b11), read_options{}, {0, 1, 2, 3, 100});
    });
}

// Identical replicas give the complete answer with any schedule of the
// coordinator. The test also requires that the schedules split some scans,
// and that some rounds merge their contiguous ranges and some do not.
SEASTAR_THREAD_TEST_CASE(test_identical_replicas_with_schedules) {
    with_harness([] (harness& hs) {
        size_t merged = 0;
        size_t apart = 0;
        for (uint32_t seed = 1; seed <= 10; ++seed) {
            for (const auto& q : paging_queries()) {
                for (auto page_size : {1, 3, 100}) {
                    const read_options opts{.replica_count = 3, .extra_replicas = 1, .page_size = page_size, .schedule_seed = seed};
                    const auto o = run_and_check(hs, read_case{on_replicas(mixed_partitions(), 0b111), q, opts});
                    // The trace of a split scan says whether it merges ranges.
                    merged += count_trace_lines(o, "merging contiguous ranges");
                    apart += count_trace_lines(o, "without merging ranges");
                }
            }
        }
        BOOST_REQUIRE_GT(merged, 0);
        BOOST_REQUIRE_GT(apart, 0);
    });
}

// Without native_reverse_queries, the replicas other than the coordinator's
// own receive reversed reads in the legacy reversed format.
SEASTAR_THREAD_TEST_CASE(test_identical_replicas_with_legacy_reversed_format) {
    with_harness([] (harness& hs) {
        size_t legacy_reads = 0;
        for (uint32_t seed = 1; seed <= 5; ++seed) {
            for (auto page_size : {1, 3, 100}) {
                const read_options opts{.page_size = page_size, .native_reverse_queries = false, .read_frontiers = false, .schedule_seed = seed};
                const auto q = select_query{.partitions = std::vector<int32_t>{1}, .reversed = true};
                const auto o = run_and_check(hs, read_case{on_replicas(mixed_partitions(), 0b11), q, opts});
                legacy_reads += count_trace_lines(o, "legacy reversed format");
            }
        }
        BOOST_REQUIRE_GT(legacy_reads, 0);
    });
}

// The replica continues each page with the querier which it cached on the
// previous page. Without a schedule, no replica evicts queriers.
SEASTAR_THREAD_TEST_CASE(test_single_replica_with_querier_cache) {
    with_harness([] (harness& hs) {
        run_paging_queries(hs, on_replicas(mixed_partitions(), 0b1), read_options{.replica_count = 1, .querier_cache = true}, {1, 2, 3, 100});
    });
}

SEASTAR_THREAD_TEST_CASE(test_identical_replicas_with_querier_cache) {
    with_harness([] (harness& hs) {
        run_paging_queries(hs, on_replicas(mixed_partitions(), 0b11), read_options{.querier_cache = true}, {1, 2, 3, 100});
    });
}

// A page size in bytes of 1 stops a replica's page after its first live row.
SEASTAR_THREAD_TEST_CASE(test_single_replica_with_a_small_page_size_in_bytes) {
    with_harness([] (harness& hs) {
        run_paging_queries(hs, on_replicas(mixed_partitions(), 0b1), read_options{.replica_count = 1, .page_size_in_bytes = 1}, {1, 3, 100});
    });
}

// Without read_frontiers, master's resolver can return a short page without a
// partition and without a cursor. Replica 0 holds the deletion of partition
// 1, and replica 1 holds a row which it shadows. Both mutation pages stop
// short on the byte limit. The resolver takes replica 0's page, which has no
// clustering row, to stop before the partition's rows, and trims the
// partition away. The pager then fails the page with an internal error,
// which the harness allows without the feature.
SEASTAR_THREAD_TEST_CASE(test_page_without_partition_or_cursor_fails) {
    with_harness([] (harness& hs) {
        auto o = run_and_check(hs, read_case{
            placed_history{
                {regular_cell_write{1, 1, regular_column::v1, std::nullopt, 12, lifetime::permanent}, 0b10},
                {range_deletion{3, std::nullopt, std::nullopt, 13}, 0b1},
                {partition_deletion{1, 21}, 0b1},
            },
            select_query{.select_s = false, .select_v1 = false, .select_v2 = false},
            read_options{.replica_count = 2, .page_size = 2, .page_size_in_bytes = 3, .read_frontiers = false},
        });
        BOOST_REQUIRE(o.allowed_error);
    });
}

// A static-only row depends on the whole partition. Partition 4 has a live
// static cell, a live row 4, and a deleted row 2. Replica 0 stops its data
// page and its mutation page inside the partition, after row 2. The
// reconciled page may not hold the static-only row, because row 4 cancels
// it. The next page reads the rest of the partition.
SEASTAR_THREAD_TEST_CASE(test_reconciled_page_leaves_out_undecided_static_row) {
    with_harness([] (harness& hs) {
        auto o = run_and_check(hs, read_case{
            placed_history{
                {range_deletion{4, bound{0, true}, bound{6, true}, 16}, 0b10},
                {regular_cell_write{4, 2, regular_column::v1, 2, 13, lifetime::permanent}, 0b1},
                {regular_cell_write{4, 4, regular_column::v1, 5, 22, lifetime::permanent}, 0b1},
                {static_cell_write{4, 4, 6, lifetime::permanent}, 0b1},
            },
            select_query{.select_s = false, .select_v1 = false, .select_v2 = false},
            read_options{.replica_count = 2, .page_size = 3, .page_size_in_bytes = 41},
        });
        BOOST_REQUIRE_GE(count_trace_lines(o, "row of the partition pending"), 1);
    });
}

// A single replica stops its data page on the tombstone limit, after the
// live static row and the deleted row 1 of partition 1. Its page holds the
// static-only row, but row 2 cancels it. The coordinator reconciles, and
// the reconciled page leaves the row out.
SEASTAR_THREAD_TEST_CASE(test_data_page_with_undecided_static_row_is_reconciled) {
    with_harness([] (harness& hs) {
        auto o = run_and_check(hs, read_case{
            on_replicas({
                static_cell_write{1, 5, 1},
                row_deletion{1, 1, 2},
                row_marker_write{1, 2, 3},
            }, 0b1),
            select_query{},
            read_options{.replica_count = 1, .tombstone_limit = 1},
        });
        BOOST_REQUIRE_GE(count_trace_lines(o, "reconciling"), 1);
    });
}

// A DISTINCT row of a partition without a live static row is decided at its
// first live clustering row. Replica 0 stops its mutation page in partition
// 4, after the deleted row 1 and before the live row 4. The next page
// continues the partition, although a DISTINCT query has no clustering key.
SEASTAR_THREAD_TEST_CASE(test_distinct_page_continues_undecided_partition) {
    with_harness([] (harness& hs) {
        auto o = run_and_check(hs, read_case{
            placed_history{
                {range_deletion{1, std::nullopt, bound{2, false}, 3}, 0b1},
                {regular_cell_write{4, 1, regular_column::v1, std::nullopt, 1, lifetime::permanent}, 0b1},
                {row_marker_write{4, 4, 2, lifetime::permanent}, 0b1},
            },
            select_query{.distinct = true, .select_v1 = false, .select_v2 = false},
            read_options{.replica_count = 2, .page_size = 5, .page_size_in_bytes = 427},
        });
        BOOST_REQUIRE_GE(count_trace_lines(o, "row of the partition pending"), 1);
    });
}

// PER PARTITION LIMIT counts the rows of the cursor's partition in the
// paging state. Page 0 returns row 5 of partition 2, and stops short on the
// tombstone limit inside partition 4, before its row 3. The count must be 0
// for partition 4, not the count of partition 2.
SEASTAR_THREAD_TEST_CASE(test_per_partition_limit_counts_rows_of_cursor_partition) {
    with_harness([] (harness& hs) {
        for (bool read_frontiers : {true, false}) {
            read_options opts{.replica_count = 1, .page_size = 100, .tombstone_limit = 4};
            opts.read_frontiers = read_frontiers;
            run_and_check(hs, read_case{
                placed_history{
                    {regular_cell_write{2, 5, regular_column::v2, 1, 13, lifetime::permanent}, 0b1},
                    {regular_cell_write{4, 3, regular_column::v1, 3, 20, lifetime::permanent}, 0b1},
                    {range_deletion{1, bound{1, true}, bound{5, true}, 14}, 0b1},
                    {range_deletion{4, std::nullopt, bound{2, true}, 8}, 0b1},
                    {row_marker_write{3, 1, 16, lifetime::permanent}, 0b1},
                },
                select_query{.select_v1 = false, .select_v2 = false, .per_partition_limit = 1},
                opts,
            });
        }
    });
}

// Page 0 returns row 1 of partition 4. Page 1 is empty, and stops short
// inside the partition. It must keep the count of partition 4, so that page
// 2 does not return row 5 beyond the limit.
SEASTAR_THREAD_TEST_CASE(test_per_partition_limit_keeps_count_over_empty_page) {
    with_harness([] (harness& hs) {
        for (bool read_frontiers : {true, false}) {
            read_options opts{.replica_count = 2, .page_size = 3, .page_size_in_bytes = 22};
            opts.read_frontiers = read_frontiers;
            run_and_check(hs, read_case{
                placed_history{
                    {regular_cell_write{4, 4, regular_column::v1, 1, 4, lifetime::expiring}, 0b10},
                    {row_marker_write{4, 1, 10, lifetime::expiring}, 0b1},
                    {regular_cell_write{4, 5, regular_column::v1, 2, 7, lifetime::permanent}, 0b1},
                    {range_deletion{4, bound{0, true}, bound{5, false}, 9}, 0b1},
                },
                select_query{.select_v1 = false, .per_partition_limit = 1},
                opts,
            });
        }
    });
}

// Without native_reverse_queries, a replica converts a reversed read from the
// legacy format with partition_slice_builder, which must keep the
// per-partition limit. Otherwise the replica returns both rows of partition 1.
SEASTAR_THREAD_TEST_CASE(test_legacy_reversed_read_keeps_per_partition_limit) {
    with_harness([] (harness& hs) {
        run_and_check(hs, read_case{
            on_replicas({
                regular_cell_write{1, 5, regular_column::v2, 9, 17},
                regular_cell_write{1, 3, regular_column::v2, 9, 13},
            }, 0b1),
            select_query{.partitions = std::vector<int32_t>{1}, .reversed = true, .per_partition_limit = 1},
            read_options{.replica_count = 1, .page_size = 5, .native_reverse_queries = false, .read_frontiers = false, .schedule_seed = 695175667},
        });
    });
}

// The shrunk witnesses of the first attempt of the paging fixes, which
// progress.md records under the titles in the comments. Each runs with all
// cluster features. Ten of the twelve witnesses which disable a feature
// still fail with it disabled, because the coordinator then runs master's
// code.
SEASTAR_THREAD_TEST_CASE(test_witnesses_of_first_attempt) {
    const std::vector<read_case> witnesses{
        // Fixed: a DISTINCT page whose replicas disagree about the first live row
        read_case{
            placed_history{
                {range_deletion{1, bound{1, true}, bound{3, true}, 12}, 0b10},
                {regular_cell_write{1, 5, regular_column::v1, 4, 10, lifetime::expiring}, 0b10},
                {row_marker_write{1, 2, 6, lifetime::expiring}, 0b1},
                {range_deletion{1, bound{4, true}, std::nullopt, 11}, 0b1},
            },
            select_query{.distinct = true, .select_v1 = false, .select_v2 = false},
            read_options{.replica_count = 2, .page_size = 2},
        },
        // Fixed: a resumed page which drops a partition tombstone
        read_case{
            placed_history{
                {partition_deletion{1, 12}, 0b1},
                {row_marker_write{1, 4, 6, lifetime::expired}, 0b11},
                {row_marker_write{1, 5, 5, lifetime::permanent}, 0b10},
            },
            select_query{.select_s = false},
            read_options{.replica_count = 2, .page_size = 2, .page_size_in_bytes = 44, .querier_cache = true},
        },
        // Fixed: a resumed page which drops a partition tombstone
        read_case{
            placed_history{
                {partition_deletion{4, 8}, 0b1},
                {range_deletion{1, bound{3, false}, bound{4, true}, 2}, 0b1},
                {row_deletion{1, 3, 7}, 0b1},
                {static_cell_write{2, std::nullopt, 3, lifetime::permanent}, 0b1},
                {range_deletion{4, bound{1, true}, bound{6, true}, 4}, 0b1},
                {regular_cell_write{3, 2, regular_column::v2, 8, 1, lifetime::permanent}, 0b1},
            },
            select_query{.select_v1 = false, .select_v2 = false},
            read_options{.replica_count = 1, .page_size = 3, .page_size_in_bytes = 3, .tombstone_limit = 1, .querier_cache = true, .schedule_seed = 1533966746},
        },
        // Fixed: an invented DISTINCT row on a page which continues a partition
        read_case{
            placed_history{
                {range_deletion{2, std::nullopt, bound{4, false}, 14}, 0b1},
                {partition_deletion{4, 11}, 0b10},
                {regular_cell_write{4, 1, regular_column::v1, 6, 8, lifetime::expiring}, 0b1},
                {static_cell_write{4, 0, 4, lifetime::permanent}, 0b1},
                {static_cell_write{1, 9, 12, lifetime::expiring}, 0b10},
            },
            select_query{.distinct = true, .select_s = false, .select_v1 = false, .select_v2 = false},
            read_options{.replica_count = 2, .page_size = 3, .page_size_in_bytes = 516},
        },
        // Fixed: an unpaged read whose replicas stopped apart
        read_case{
            placed_history{
                {range_deletion{1, bound{2, false}, bound{4, false}, 21}, 0b1},
                {regular_cell_write{2, 1, regular_column::v1, 6, 13, lifetime::permanent}, 0b10},
                {static_cell_write{1, 1, 23, lifetime::permanent}, 0b10},
                {row_marker_write{4, 3, 7, lifetime::permanent}, 0b1},
            },
            select_query{.select_s = false, .select_v1 = false, .select_v2 = false, .limit = 1},
            read_options{.replica_count = 2, .page_size = 3, .querier_cache = true, .schedule_seed = 3135680056},
        },
        // Settled: a replica which stopped without a cursor, with empty_replica_pages disabled
        read_case{
            placed_history{
                {regular_cell_write{4, 3, regular_column::v1, 6, 4, lifetime::permanent}, 0b1},
                {range_deletion{2, std::nullopt, bound{0, true}, 3}, 0b10},
                {row_marker_write{3, 2, 1, lifetime::permanent}, 0b1},
                {static_cell_write{2, 3, 5, lifetime::permanent}, 0b1},
            },
            select_query{.select_s = false, .select_v1 = false, .select_v2 = false},
            read_options{.replica_count = 2, .page_size = 1, .schedule_seed = 2125198832},
        },
        // Fixed: a replica which stopped with a full page is invisible to the decision
        read_case{
            placed_history{
                {row_marker_write{3, 2, 8, lifetime::expiring}, 0b1},
                {static_cell_write{2, 1, 9, lifetime::permanent}, 0b1},
                {regular_cell_write{1, 5, regular_column::v2, 8, 7, lifetime::permanent}, 0b1},
                {regular_cell_write{2, 5, regular_column::v1, std::nullopt, 2, lifetime::permanent}, 0b10},
                {row_marker_write{4, 3, 4, lifetime::permanent}, 0b1},
            },
            select_query{.select_s = false, .select_v1 = false, .select_v2 = false},
            read_options{.replica_count = 2, .page_size = 1, .querier_cache = true, .schedule_seed = 3230499980},
        },
        // Fixed: a cached querier reused for a page which asks for no clustering row
        read_case{
            placed_history{
                {range_deletion{1, bound{2, true}, std::nullopt, 15}, 0b1},
                {row_marker_write{1, 3, 9, lifetime::expired}, 0b11},
                {row_marker_write{3, 5, 8, lifetime::expiring}, 0b11},
                {regular_cell_write{2, 2, regular_column::v2, 1, 16, lifetime::permanent}, 0b1},
                {regular_cell_write{1, 5, regular_column::v1, 4, 5, lifetime::permanent}, 0b11},
            },
            select_query{.select_s = false, .select_v2 = false},
            read_options{.replica_count = 2, .extra_replicas = 1, .page_size = 3, .page_size_in_bytes = 25, .querier_cache = true, .schedule_seed = 3651344462},
        },
        // Fixed: a re-emitted range tombstone change counted twice
        read_case{
            placed_history{
                {range_deletion{2, bound{0, true}, bound{5, true}, 7}, 0b10},
                {partition_deletion{2, 17}, 0b101},
                {static_cell_write{1, 6, 16, lifetime::permanent}, 0b1},
                {regular_cell_write{2, 1, regular_column::v2, 9, 9, lifetime::permanent}, 0b10},
                {row_marker_write{2, 4, 18, lifetime::permanent}, 0b110},
                {regular_cell_write{3, 4, regular_column::v1, 5, 14, lifetime::expiring}, 0b1},
                {row_marker_write{2, 5, 11, lifetime::permanent}, 0b10},
            },
            select_query{.select_v2 = false, .filter = {predicate{column::v2, comparison::lt, 4}}},
            read_options{.replica_count = 3, .page_size = 2, .tombstone_limit = 1, .querier_cache = true, .schedule_seed = 3864528926},
        },
        // Fixed: the stop can be anywhere in the page which lost an undecidable row
        read_case{
            placed_history{
                {regular_cell_write{3, 2, regular_column::v1, 5, 3, lifetime::permanent}, 0b10},
                {static_cell_write{2, 3, 1, lifetime::permanent}, 0b10},
                {regular_cell_write{4, 1, regular_column::v2, 5, 2, lifetime::expiring}, 0b11},
                {range_deletion{2, bound{1, true}, bound{5, false}, 16}, 0b1},
            },
            select_query{.distinct = true, .select_s = false, .select_v1 = false, .select_v2 = false},
            read_options{.replica_count = 2, .page_size = 2},
        },
        // Fixed: a page which re-emits a static row exceeds its row limit
        read_case{
            placed_history{
                {regular_cell_write{4, 2, regular_column::v1, 1, 4, lifetime::permanent}, 0b1},
                {partition_deletion{3, 5}, 0b1},
                {row_deletion{2, 5, 7}, 0b1},
                {static_cell_write{2, 3, 2, lifetime::permanent}, 0b1},
            },
            select_query{.select_s = false},
            read_options{.replica_count = 4, .extra_replicas = 3, .page_size = 1, .querier_cache = true, .schedule_seed = 2947302135},
        },
        // Fixed: a cursor which leaves no clustering row of its partition
        read_case{
            placed_history{
                {row_marker_write{1, 2, 13, lifetime::permanent}, 0b101},
                {range_deletion{1, bound{2, false}, bound{6, false}, 14}, 0b110},
                {range_deletion{2, bound{1, true}, bound{2, false}, 1}, 0b101},
                {regular_cell_write{1, 1, regular_column::v2, 1, 3, lifetime::expiring}, 0b1},
                {static_cell_write{1, 3, 2, lifetime::permanent}, 0b10},
            },
            select_query{.ck_end = bound{3, false}, .select_v1 = false, .select_v2 = false},
            read_options{.replica_count = 3, .extra_replicas = 1, .page_size = 3, .page_size_in_bytes = 37, .querier_cache = true, .schedule_seed = 2743198667},
        },
        // Fixed: a replica which left the page's last partition at its per-partition limit
        read_case{
            placed_history{
                {range_deletion{2, bound{1, true}, bound{5, false}, 16}, 0b1},
                {regular_cell_write{2, 3, regular_column::v2, 2, 8, lifetime::permanent}, 0b10},
                {regular_cell_write{2, 5, regular_column::v1, 0, 7, lifetime::permanent}, 0b1},
                {range_deletion{2, bound{4, false}, bound{6, false}, 11}, 0b10},
            },
            select_query{.distinct = true, .select_s = false, .select_v1 = false, .select_v2 = false, .limit = 1},
            read_options{.replica_count = 2, .page_size = 5},
        },
        // Fixed: a builder which starts from a slice drops the per-partition limit
        read_case{
            placed_history{
                {regular_cell_write{1, 5, regular_column::v2, 9, 17, lifetime::permanent}, 0b1},
                {regular_cell_write{1, 3, regular_column::v2, 9, 13, lifetime::permanent}, 0b1},
            },
            select_query{.partitions = std::vector<int32_t>{1}, .reversed = true, .per_partition_limit = 1},
            read_options{.replica_count = 1, .page_size = 5, .schedule_seed = 695175667},
        },
        // Fixed: defect 7 which truncates the query, with the feature disabled
        read_case{
            placed_history{
                {static_cell_write{2, 0, 3, lifetime::permanent}, 0b10},
                {row_marker_write{2, 4, 8, lifetime::expired}, 0b1},
                {row_marker_write{4, 1, 1, lifetime::permanent}, 0b10},
            },
            select_query{.select_s = false},
            read_options{.replica_count = 2, .page_size = 1},
        },
        // What a row count in the digest reply would and would not catch
        read_case{
            placed_history{
                {row_marker_write{1, 1, 1, lifetime::expired}, 0b11},
                {row_marker_write{2, 1, 2, lifetime::expired}, 0b11},
                {static_cell_write{1, 7, 3, lifetime::permanent}, 0b01},
                {static_cell_write{2, 8, 4, lifetime::permanent}, 0b10},
            },
            select_query{.select_s = false},
            read_options{.replica_count = 2, .page_size = 2},
        },
        // Fixed: a cached querier reused for a page which starts after its position
        read_case{
            placed_history{
                {row_marker_write{2, 5, 8, lifetime::permanent}, 0b1},
                {regular_cell_write{2, 1, regular_column::v1, 6, 7, lifetime::permanent}, 0b1},
                {range_deletion{1, std::nullopt, bound{4, true}, 4}, 0b1},
                {range_deletion{2, bound{0, false}, bound{5, false}, 6}, 0b1},
            },
            select_query{.select_v2 = false},
            read_options{.replica_count = 3, .extra_replicas = 2, .page_size = 1, .tombstone_limit = 1, .querier_cache = true, .schedule_seed = 4178336223},
        },
        // Fixed: a DISTINCT row which a replica invents from a static cell
        read_case{
            placed_history{
                {static_cell_write{2, 3, 1, lifetime::permanent}, 0b1},
                {static_cell_write{2, std::nullopt, 4, lifetime::permanent}, 0b10},
            },
            select_query{.distinct = true, .select_s = false, .select_v1 = false, .select_v2 = false},
            read_options{.replica_count = 2, .page_size = 3},
        },
        // Fixed: an unpaged read which returns an undecided static-only row
        read_case{
            placed_history{
                {regular_cell_write{1, 3, regular_column::v2, 1, 3, lifetime::permanent}, 0b1},
                {static_cell_write{1, 3, 7, lifetime::expiring}, 0b10},
                {regular_cell_write{1, 4, regular_column::v1, 3, 9, lifetime::permanent}, 0b1},
                {range_deletion{1, bound{2, true}, std::nullopt, 6}, 0b10},
            },
            select_query{.select_s = false, .select_v1 = false, .limit = 1},
            read_options{.replica_count = 2, .page_size = 100},
        },
        // Fixed: a single reply which stopped early skipped the resolution
        read_case{
            placed_history{
                {regular_cell_write{2, 2, regular_column::v1, 2, 4, lifetime::expired}, 0b1},
                {regular_cell_write{2, 4, regular_column::v1, 0, 6, lifetime::expiring}, 0b1},
                {static_cell_write{2, 9, 5, lifetime::permanent}, 0b1},
            },
            select_query{.partitions = std::vector<int32_t>{1, 2}, .select_s = false, .select_v1 = false, .select_v2 = false},
            read_options{.replica_count = 2, .extra_replicas = 1, .page_size = 3, .page_size_in_bytes = 7, .schedule_seed = 408707876},
        },
        // Fixed: a cursor which reports the static row of a consumed partition
        read_case{
            placed_history{
                {static_cell_write{2, 5, 14, lifetime::permanent}, 0b1},
                {range_deletion{2, bound{2, true}, std::nullopt, 10}, 0b1},
                {static_cell_write{2, std::nullopt, 5, lifetime::permanent}, 0b10},
                {regular_cell_write{3, 1, regular_column::v1, 9, 18, lifetime::permanent}, 0b10},
            },
            select_query{.select_s = false},
            read_options{.replica_count = 2, .page_size = 5, .tombstone_limit = 1},
        },
        // Fixed: a consumer fed after it asked to stop
        read_case{
            placed_history{
                {regular_cell_write{4, 2, regular_column::v2, 4, 14, lifetime::permanent}, 0b1},
                {row_deletion{2, 4, 21}, 0b10},
                {regular_cell_write{3, 2, regular_column::v1, 6, 19, lifetime::permanent}, 0b1},
                {range_deletion{4, bound{2, true}, bound{5, false}, 8}, 0b1},
                {regular_cell_write{4, 4, regular_column::v1, 3, 10, lifetime::permanent}, 0b10},
                {regular_cell_write{2, 4, regular_column::v2, 2, 17, lifetime::permanent}, 0b1},
            },
            select_query{.per_partition_limit = 3},
            read_options{.replica_count = 2, .page_size = 1},
        },
        // Fixed: a range tombstone reopened after the static row
        read_case{
            placed_history{
                {range_deletion{4, bound{2, true}, bound{2, true}, 18}, 0b1},
                {range_deletion{4, bound{0, true}, std::nullopt, 12}, 0b1},
                {regular_cell_write{4, 3, regular_column::v1, 6, 10, lifetime::expiring}, 0b10},
                {static_cell_write{4, 3, 11, lifetime::permanent}, 0b1},
            },
            select_query{.partitions = std::vector<int32_t>{4}, .select_s = false, .select_v1 = false},
            read_options{.replica_count = 2, .page_size = 2, .page_size_in_bytes = 279, .querier_cache = true},
        },
        // Fixed: a page which ends past a replica stop, estimated with an inflated row count
        read_case{
            placed_history{
                {row_marker_write{4, 3, 8, lifetime::permanent}, 0b1},
                {regular_cell_write{3, 2, regular_column::v2, 4, 5, lifetime::permanent}, 0b10},
                {static_cell_write{1, 4, 4, lifetime::permanent}, 0b1},
                {row_deletion{2, 4, 7}, 0b10},
                {regular_cell_write{2, 4, regular_column::v2, 6, 2, lifetime::permanent}, 0b1},
            },
            select_query{.ck_end = bound{6, true}, .select_v1 = false, .limit = 1},
            read_options{.replica_count = 2, .page_size = 0},
        },
        // Fixed without a historical fix: a retry which does not enlarge its limit
        read_case{
            placed_history{
                {regular_cell_write{1, 1, regular_column::v1, 9, 7, lifetime::permanent}, 0b100},
                {static_cell_write{4, 0, 5, lifetime::permanent}, 0b1},
                {regular_cell_write{1, 3, regular_column::v2, 4, 11, lifetime::permanent}, 0b10},
                {row_deletion{1, 1, 12}, 0b1},
            },
            select_query{.select_s = false, .select_v1 = false, .select_v2 = false, .limit = 1},
            read_options{.replica_count = 3, .page_size = 0},
        },
        // Fixed without a historical fix: a digest which misses a static-only row
        read_case{
            placed_history{
                {static_cell_write{4, 0, 5, lifetime::permanent}, 0b1},
                {row_marker_write{4, 4, 3, lifetime::expired}, 0b1111},
            },
            select_query{.partitions = std::vector<int32_t>{2, 3, 4}, .select_s = false, .select_v1 = false, .select_v2 = false},
            read_options{.replica_count = 4, .page_size = 3, .schedule_seed = 2345716740},
        },
        // Fixed: an incomplete repeated-state key
        read_case{
            placed_history{
                {static_cell_write{2, std::nullopt, 11, lifetime::permanent}, 0b10},
                {regular_cell_write{3, 5, regular_column::v1, 8, 1, lifetime::expiring}, 0b1},
            },
            select_query{.select_s = false, .select_v1 = false},
            read_options{.replica_count = 2, .page_size = 2, .page_size_in_bytes = 4},
        },
        // Fixed: defect 7 with the static-row digest feature disabled
        read_case{
            placed_history{
                {regular_cell_write{1, 5, regular_column::v2, std::nullopt, 8, lifetime::permanent}, 0b1},
                {regular_cell_write{2, 4, regular_column::v1, 0, 15, lifetime::permanent}, 0b1},
                {static_cell_write{1, 4, 5, lifetime::expiring}, 0b10},
            },
            select_query{.select_s = false, .select_v1 = false},
            read_options{.replica_count = 2, .page_size = 1, .schedule_seed = 3637446295},
        },
        // Fixed: a static-only row lost when the next page starts in a later partition
        read_case{
            placed_history{
                {static_cell_write{1, 4, 2, lifetime::permanent}, 0b1},
                {regular_cell_write{4, 4, regular_column::v1, 1, 4, lifetime::permanent}, 0b1},
                {range_deletion{1, bound{2, false}, bound{4, true}, 6}, 0b1},
            },
            select_query{.select_s = false, .select_v1 = false, .select_v2 = false},
            read_options{.replica_count = 2, .page_size = 1, .querier_cache = true},
        },
        // Fixed: an undecidable row which frees a slot under a limit
        read_case{
            placed_history{
                {range_deletion{1, bound{2, false}, std::nullopt, 3}, 0b10},
                {static_cell_write{1, 9, 10, lifetime::expiring}, 0b1},
                {static_cell_write{4, 0, 2, lifetime::permanent}, 0b10},
                {row_marker_write{3, 5, 8, lifetime::permanent}, 0b1},
            },
            select_query{.select_s = false, .limit = 2},
            read_options{.replica_count = 2, .page_size = 2, .schedule_seed = 3538202229},
        },
        // Fixed: a short page whose cursor is behind its last row
        read_case{
            placed_history{
                {regular_cell_write{2, 4, regular_column::v1, 0, 15, lifetime::permanent}, 0b1},
                {regular_cell_write{1, 3, regular_column::v1, 1, 4, lifetime::expired}, 0b10},
                {static_cell_write{1, 4, 5, lifetime::expiring}, 0b10},
            },
            select_query{.select_s = false, .select_v1 = false},
            read_options{.replica_count = 2, .page_size = 1},
        },
        // Fixed: a static-only row lost after a short page inside its partition
        read_case{
            placed_history{
                {regular_cell_write{4, 4, regular_column::v2, std::nullopt, 6, lifetime::permanent}, 0b1},
                {static_cell_write{4, 3, 4, lifetime::permanent}, 0b1},
            },
            select_query{.partitions = std::vector<int32_t>{4}, .select_v2 = false},
            read_options{.replica_count = 2, .page_size = 5, .page_size_in_bytes = 3, .querier_cache = true},
        },
        // Fixed: a stale cursor of a cached querier repeats the paging state
        read_case{
            placed_history{
                {row_deletion{2, 3, 4}, 0b101},
                {row_marker_write{2, 5, 3, lifetime::expired}, 0b1},
                {regular_cell_write{4, 2, regular_column::v1, 4, 12, lifetime::permanent}, 0b1},
                {regular_cell_write{1, 5, regular_column::v1, 4, 11, lifetime::permanent}, 0b1},
            },
            select_query{},
            read_options{.replica_count = 3, .extra_replicas = 2, .page_size = 2, .tombstone_limit = 1, .querier_cache = true, .schedule_seed = 2389981646},
        },
        // Fixed without a historical fix: a reconciled cursor at the end of a partition repeats
        read_case{
            placed_history{
                {static_cell_write{1, 1, 9, lifetime::expiring}, 0b1},
                {range_deletion{4, bound{5, true}, std::nullopt, 8}, 0b10},
            },
            select_query{.select_s = false},
            read_options{.replica_count = 2, .page_size = 2, .page_size_in_bytes = 2},
        },
        // Resolved: the fix of `87a340bd41` depended on other fixes
        read_case{
            placed_history{
                {row_deletion{2, 1, 11}, 0b1},
                {range_deletion{1, std::nullopt, bound{3, true}, 1}, 0b1},
                {regular_cell_write{2, 2, regular_column::v1, 8, 3, lifetime::permanent}, 0b10},
                {regular_cell_write{1, 2, regular_column::v2, 5, 10, lifetime::expiring}, 0b1},
                {static_cell_write{4, 3, 4, lifetime::permanent}, 0b1},
                {regular_cell_write{1, 1, regular_column::v2, std::nullopt, 17, lifetime::permanent}, 0b1},
            },
            select_query{.distinct = true, .select_v1 = false, .select_v2 = false},
            read_options{.replica_count = 2, .page_size = 2, .page_size_in_bytes = 777},
        },
    };
    with_harness([&] (harness& hs) {
        for (const auto& c : witnesses) {
            run_and_check(hs, c);
        }
    });
}

namespace {

// 1 to 4 replicas, of which all but one may be extra replicas.
read_options random_replicas() {
    read_options opts;
    opts.replica_count = tests::random::get_int<size_t>(1, 4);
    opts.extra_replicas = tests::random::get_int<size_t>(0, opts.replica_count - 1);
    return opts;
}

// Places each write of `h` on a random set of the replicas of `replicas`.
// The set includes a replica which counts toward the consistency level.
placed_history random_placement(const history& h, const read_options& replicas) {
    const size_t block_for = replicas.replica_count - replicas.extra_replicas;
    placed_history placed;
    for (const auto& op : h) {
        auto mask = tests::random::get_int<uint32_t>(1, (uint32_t(1) << replicas.replica_count) - 1);
        if (!(mask & ((uint32_t(1) << block_for) - 1))) {
            mask |= uint32_t(1) << tests::random::get_int<size_t>(0, block_for - 1);
        }
        placed.push_back(placed_operation{op, mask});
    }
    return placed;
}

// `opts` with its replica counts kept, and everything else random: the page
// size, the byte and tombstone limits, the cluster features, the querier
// cache, repairs and the schedule.
read_options random_options(read_options opts) {
    // A page size of 0 makes the query unpaged.
    opts.page_size = std::array{0, 1, 2, 3, 5, 100}[tests::random::get_int(0, 5)];
    // Page sizes in bytes range from 1, which stops a page after its first
    // row, to a few kilobytes, which a small history rarely reaches. Each
    // range [2^k, 2^(k+1)) is equally likely, so small sizes are more likely.
    if (tests::random::get_bool()) {
        const auto magnitude = tests::random::get_int(0, 12);
        opts.page_size_in_bytes = tests::random::get_int<uint64_t>(uint64_t(1) << magnitude, (uint64_t(2) << magnitude) - 1);
    }
    if (tests::random::get_int(0, 3) == 0) {
        opts.tombstone_limit = tests::random::get_int<uint64_t>(1, 8);
    }
    // Each feature requires the older features before it.
    opts.empty_replica_pages = tests::random::get_int(0, 3) != 0;
    opts.empty_replica_mutation_pages = opts.empty_replica_pages && tests::random::get_int(0, 3) != 0;
    opts.native_reverse_queries = opts.empty_replica_mutation_pages && tests::random::get_int(0, 3) != 0;
    opts.read_frontiers = opts.native_reverse_queries && tests::random::get_int(0, 3) != 0;
    opts.querier_cache = tests::random::get_bool();
    opts.apply_repairs = !opts.querier_cache && tests::random::get_bool();
    opts.schedule_seed = tests::random::get_int<uint32_t>();
    return opts;
}

} // anonymous namespace

// The general test. It draws random histories on random replicas. It reads
// each history with ten random queries and random options: page sizes, byte
// and tombstone limits, cluster features, a querier cache, repairs and a
// schedule of the coordinator. It uses every feature of the harness, and it
// treats no case specially.
//
// The baseline code has known defects and fails many cases. So the test runs
// only when the environment variable SCYLLA_PAGED_READ_CAMPAIGN is set. Its
// value is the number of histories. The test reports each failure with a
// shrunk case, and logs the number of failures of each kind. When the
// environment variable SCYLLA_PAGED_READ_STOP_AT_FAILURE is set, the test
// stops after its first failure. When the environment variable
// SCYLLA_PAGED_READ_VIOLATION is set, a run fails only if one of its
// violations contains the variable's value. This keeps a campaign on one
// kind of defect while the baseline still has others.
SEASTAR_THREAD_TEST_CASE(test_general) {
    const char* histories = std::getenv("SCYLLA_PAGED_READ_CAMPAIGN");
    if (!histories) {
        testlog.info("The general test runs only when SCYLLA_PAGED_READ_CAMPAIGN sets the number of histories");
        return;
    }
    const int history_count = std::stoi(histories);
    const bool stop_at_failure = std::getenv("SCYLLA_PAGED_READ_STOP_AT_FAILURE");
    const char* violation_filter = std::getenv("SCYLLA_PAGED_READ_VIOLATION");
    with_harness([history_count, stop_at_failure, violation_filter] (harness& hs) {
        size_t runs = 0;
        std::map<std::string, size_t> failures;
        auto stopped = [&] { return stop_at_failure && !failures.empty(); };
        for (int i = 0; i < history_count && !stopped(); ++i) {
            const auto replicas = random_replicas();
            // Longer histories give denser partitions and longer runs of
            // tombstones.
            const auto h = random_placement(random_history(std::array{6, 12, 24}[tests::random::get_int(0, 2)]), replicas);
            for (int j = 0; j < 10 && !stopped(); ++j) {
                const read_case c{h, random_query(), random_options(replicas)};
                testlog.debug("Running {}", describe(c));
                ++runs;
                const auto violations = hs.violations(c);
                if (violations.empty() || (violation_filter && std::ranges::none_of(violations, [&] (const std::string& v) {
                        return v.contains(violation_filter);
                    }))) {
                    continue;
                }
                ++failures[violation_kind(violations)];
                const auto shrunk = hs.shrink(c);
                const auto expected = evaluate(*hs.schema(), complete_history(shrunk.history), shrunk.query);
                const auto o = hs.run(shrunk);
                BOOST_ERROR(fmt::format("Shrunk from:\n{}\n{}", describe(c), report(shrunk, o, expected, check(o, expected))));
            }
        }
        for (const auto& [kind, count] : failures) {
            testlog.info("{} failures: {}", count, kind);
        }
        testlog.info("{} of {} runs failed", std::ranges::fold_left(failures | std::views::values, size_t(0), std::plus<>()), runs);
    });
}

BOOST_AUTO_TEST_SUITE_END()
