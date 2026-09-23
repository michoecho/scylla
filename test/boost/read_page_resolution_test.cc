/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Characterization tests of the coordinator's page resolution
// (service/read_page_resolution.hh) and of the replica page driver
// (replica::read_data_page() and replica::read_mutation_page()).
//
// The tests record what the code does today. The code was extracted from
// storage_proxy and replica::table, and the tests check that the extraction
// kept its behavior. They do not claim that this behavior is correct. A test
// which records behavior that is known to be wrong says so.
//
// Terms used in this file:
// - Cursor: the page's last position, query::result::last_position(). It is
//   the position of the last fragment which the reader's compaction consumed.
//   This can be a dead row after the page's last live row. The next page
//   starts after the cursor.
// - Short read: a page which stopped before its row or partition limit, for
//   example on its size limit. The page carries a short-read flag.
// - Digest: a hash of a replica's data page. A digest reply carries the
//   digest and the cursor, but no rows.
// - Consistency level (CL): the number of replica replies which the
//   coordinator waits for before it decides the page.
// - Reconciliation: when the digests do not match, the coordinator reads
//   mutation pages from the replicas and merges them. It converts the merged
//   mutations into a data page for the client, and computes a repair
//   difference for each replica.
// - Retry: a further reconciliation round with larger limits. The coordinator
//   retries when the merged replies may be missing rows which the page needs.

#undef SEASTAR_TESTING_MAIN

#include <boost/test/unit_test.hpp>
#include <fmt/ranges.h>
#include <seastar/testing/thread_test_case.hh>

#include "data_dictionary/user_types_metadata.hh"
#include "db/config.hh"
#include "db/extensions.hh"
#include "db/schema_tables.hh"
#include "gms/feature_service.hh"
#include "init.hh"
#include "mutation_query.hh"
#include "partition_slice_builder.hh"
#include "query/query-result-set.hh"
#include "query/query-result-writer.hh"
#include "readers/from_mutations.hh"
#include "readers/mutation_source.hh"
#include "replica/database.hh"
#include "replica/multishard_query.hh"
#include "replica/querier.hh"
#include "schema/schema_builder.hh"
#include "schema/schema_registry.hh"
#include "service/read_page_resolution.hh"
#include "serializer_impl.hh"
#include "idl/keys.dist.hh"
#include "idl/position_in_partition.dist.hh"
#include "idl/full_position.dist.hh"
#include "idl/frozen_mutation.dist.hh"
#include "idl/result.dist.hh"
#include "idl/reconcilable_result.dist.hh"
#include "idl/keys.dist.impl.hh"
#include "idl/position_in_partition.dist.impl.hh"
#include "idl/full_position.dist.impl.hh"
#include "idl/frozen_mutation.dist.impl.hh"
#include "idl/result.dist.impl.hh"
#include "idl/reconcilable_result.dist.impl.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/mutation_assertions.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "test/lib/test_utils.hh"
#include "tombstone_gc_extension.hh"

using namespace service;

BOOST_AUTO_TEST_SUITE(read_page_resolution_test)

namespace {

const gc_clock::time_point query_time{gc_clock::duration(1'000'000)};

const locator::host_id replica_a{utils::UUID(0, 1)};
const locator::host_id replica_b{utils::UUID(0, 2)};

schema_ptr make_schema() {
    return schema_builder(this_smp_shard_count(), "ks", "cf")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("s", int32_type, column_kind::static_column)
            .with_column("v", int32_type, column_kind::regular_column)
            .build();
}

partition_key make_pk(const schema& s, int32_t pk) {
    return partition_key::from_single_value(s, int32_type->decompose(pk));
}

clustering_key make_ck(const schema& s, int32_t ck) {
    return clustering_key::from_single_value(s, int32_type->decompose(ck));
}

// A row without a row marker, whose liveness comes from `v`.
mutation make_row(schema_ptr s, int32_t pk, int32_t ck, int32_t v, api::timestamp_type ts) {
    mutation m(s, make_pk(*s, pk));
    m.set_clustered_cell(make_ck(*s, ck), to_bytes("v"), data_value(v), ts);
    return m;
}

mutation make_row_deletion(schema_ptr s, int32_t pk, int32_t ck, api::timestamp_type ts) {
    mutation m(s, make_pk(*s, pk));
    m.partition().apply_delete(*s, make_ck(*s, ck), tombstone(ts, query_time));
    return m;
}

// Merges `muts` into the contents of one replica: one mutation per partition,
// in ring order.
utils::chunked_vector<mutation> make_contents(std::vector<mutation> muts) {
    utils::chunked_vector<mutation> contents;
    for (auto& m : muts) {
        auto it = std::ranges::find_if(contents, [&] (const mutation& c) {
            return c.decorated_key().equal(*m.schema(), m.decorated_key());
        });
        if (it == contents.end()) {
            contents.push_back(std::move(m));
        } else {
            it->apply(std::move(m));
        }
    }
    std::ranges::sort(contents, [] (const mutation& a, const mutation& b) {
        return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
    });
    return contents;
}

mutation merged(const utils::chunked_vector<mutation>& a, const utils::chunked_vector<mutation>& b) {
    BOOST_REQUIRE_EQUAL(a.size(), 1);
    BOOST_REQUIRE_EQUAL(b.size(), 1);
    mutation m = a.front();
    m.apply(b.front());
    return m;
}

query::read_command make_command(const schema& query_schema, query::partition_slice slice, uint64_t row_limit,
        uint32_t partition_limit = query::max_partitions) {
    return query::read_command(query_schema.id(), query_schema.version(), std::move(slice),
            query::max_result_size(query::result_memory_limiter::unlimited_result_size), query::tombstone_limit::max,
            query::row_limit(row_limit), query::partition_limit(partition_limit), query_time);
}

mutation_source make_source(const utils::chunked_vector<mutation>& contents) {
    return mutation_source([contents] (schema_ptr s, reader_permit permit, const dht::partition_range& range, const query::partition_slice& slice,
            tracing::trace_state_ptr, streamed_mutation::forwarding fwd, mutation_reader::forwarding) {
        return make_mutation_reader_from_mutations(std::move(s), std::move(permit), contents, range, slice, fwd);
    });
}

const query::max_result_size unlimited_size(query::result_memory_limiter::unlimited_result_size);

// The options of a data request when the read has a single target. The reply
// has no digest.
const query::result_options data_only{query::result_request::only_result, query::digest_algorithm::none};
// The options of a data request when the read has several targets. The reply
// also has a digest, which the coordinator compares with the digest replies.
const query::result_options data_and_digest{query::result_request::result_and_digest, query::digest_algorithm::xxHash};

// A digest reply, reduced to the fields which
// storage_proxy::query_result_local_digest() returns. It has no short-read
// flag, so the coordinator cannot tell that a digest reply stopped short.
struct digest_reply {
    query::result_digest digest;
    api::timestamp_type last_modified;
    std::optional<full_position> last_pos;
    std::optional<query::read_frontier> frontier;
};

struct test_env {
    schema_ptr table_schema = make_schema();
    tests::reader_concurrency_semaphore_wrapper semaphore;
    // Replies hold memory units of this limiter, so it must outlive them.
    query::result_memory_limiter limiter{query::result_memory_limiter::maximum_result_size * 100};

    // Reads a mutation page of `range` from `contents` through
    // replica::read_mutation_page(), like replica::table::mutation_query().
    reconcilable_result query_mutations(schema_ptr query_schema, const utils::chunked_vector<mutation>& contents, const query::read_command& cmd,
            const dht::partition_range& range, query::max_result_size max_size, std::optional<replica::querier>* saved_querier) {
        const auto short_read_allowed = query::short_read(cmd.slice.options.contains<query::partition_slice::option::allow_short_read>());
        auto accounter = limiter.new_mutation_read(max_size, short_read_allowed).get();
        return replica::read_mutation_page(make_source(contents), query_schema, semaphore.make_permit(), cmd, range, {}, std::move(accounter),
                tombstone_gc_state::no_gc(), {}, saved_querier).get();
    }

    // Reads a mutation page of the full range with a fresh querier.
    foreign_ptr<lw_shared_ptr<reconcilable_result>> read_mutation_page(schema_ptr query_schema, const utils::chunked_vector<mutation>& contents,
            const query::read_command& cmd, query::max_result_size max_size = unlimited_size) {
        auto result = query_mutations(query_schema, contents, cmd, query::full_partition_range, max_size, nullptr);
        return make_foreign(make_lw_shared<reconcilable_result>(std::move(result)));
    }

    // Reads a data page of `ranges` from `contents` through
    // replica::read_data_page(), like replica::table::query().
    lw_shared_ptr<query::result> query_data(schema_ptr query_schema, const utils::chunked_vector<mutation>& contents, const query::read_command& cmd,
            query::result_options opts, const dht::partition_range_vector& ranges, query::max_result_size max_size,
            std::optional<replica::querier>* saved_querier) {
        const auto short_read_allowed = query::short_read(cmd.slice.options.contains<query::partition_slice::option::allow_short_read>());
        auto accounter = (opts.request == query::result_request::only_digest
                ? limiter.new_digest_read(max_size, short_read_allowed)
                : limiter.new_data_read(max_size, short_read_allowed)).get();
        return replica::read_data_page(make_source(contents), query_schema, semaphore.make_permit(), cmd, opts, ranges, {}, std::move(accounter),
                tombstone_gc_state::no_gc(), {}, saved_querier).get();
    }

    // Reads a data page of the full range with a fresh querier.
    foreign_ptr<lw_shared_ptr<query::result>> read_data_page(schema_ptr query_schema, const utils::chunked_vector<mutation>& contents,
            const query::read_command& cmd, query::result_options opts, query::max_result_size max_size = unlimited_size) {
        return make_foreign(query_data(query_schema, contents, cmd, opts, {query::full_partition_range}, max_size, nullptr));
    }

    digest_reply read_digest_page(schema_ptr query_schema, const utils::chunked_vector<mutation>& contents, const query::read_command& cmd,
            query::max_result_size max_size = unlimited_size) {
        auto result = read_data_page(query_schema, contents, cmd, query::result_options::only_digest(query::digest_algorithm::xxHash), max_size);
        if (auto frontier = result->frontier()) {
            return digest_reply{*result->digest(), result->last_modified(), std::nullopt, std::move(frontier)};
        }
        return digest_reply{*result->digest(), result->last_modified(), result->last_position(), std::nullopt};
    }
};

void add_digest(foreground_reply_collector& replies, bool counts_for_cl, digest_reply reply) {
    replies.add_digest(counts_for_cl, std::move(reply.digest), reply.last_modified, std::move(reply.last_pos), std::move(reply.frontier));
}

full_position row_position(const schema& s, int32_t pk, int32_t ck) {
    return full_position(make_pk(s, pk), position_in_partition::for_key(make_ck(s, ck)));
}

void require_position(const schema& s, const std::optional<full_position>& actual, const full_position& expected) {
    BOOST_REQUIRE(actual);
    BOOST_REQUIRE(full_position::cmp(s, *actual, expected) == 0);
}

// The (ck, v) pairs of a single-partition page, in query order.
std::vector<std::pair<int32_t, int32_t>> rows_of(schema_ptr query_schema, const query::read_command& cmd, const query::result& result) {
    auto rs = query::result_set::from_raw_result(query_schema, cmd.slice, result);
    return rs.rows() | std::views::transform([] (const query::result_set_row& row) {
        return std::pair(row.get_nonnull<int32_t>("ck"), row.get_nonnull<int32_t>("v"));
    }) | std::ranges::to<std::vector>();
}

using rows = std::vector<std::pair<int32_t, int32_t>>;

const accepted_mutation_page& require_accepted(const mutation_page_resolution& resolution) {
    BOOST_REQUIRE(std::holds_alternative<accepted_mutation_page>(resolution));
    return std::get<accepted_mutation_page>(resolution);
}

const query::read_command& require_retry(const mutation_page_resolution& resolution) {
    BOOST_REQUIRE(std::holds_alternative<mutation_page_retry>(resolution));
    return *std::get<mutation_page_retry>(resolution).cmd;
}

// The repair difference which `page` plans for `replica` in partition `pk`.
// It is disengaged when the replica needs no repair in that partition.
const std::optional<mutation>& diff_for(const schema& s, const accepted_mutation_page& page, int32_t pk, locator::host_id replica) {
    auto pit = page.repair_diffs.find(make_pk(s, pk));
    BOOST_REQUIRE(pit != page.repair_diffs.end());
    auto rit = pit->second.find(replica);
    BOOST_REQUIRE(rit != pit->second.end());
    return rit->second;
}

} // anonymous namespace

SEASTAR_THREAD_TEST_CASE(test_single_reply_is_returned_unchanged) {
    test_env env;
    auto s = env.table_schema;
    auto contents = make_contents({make_row(s, 1, 1, 10, 1), make_row(s, 1, 2, 20, 1)});
    auto cmd = make_command(*s, partition_slice_builder(*s).build(), query::max_rows);

    std::vector<mutation_page_reply> replies;
    replies.push_back({replica_a, env.read_mutation_page(s, contents, cmd)});
    auto resolution = resolve_mutation_page(s, cmd, cmd, std::move(replies)).get();

    auto& page = require_accepted(resolution);
    BOOST_REQUIRE_EQUAL(rows_of(s, cmd, page.result), (rows{{1, 10}, {2, 20}}));
    BOOST_REQUIRE(page.result.is_short_read() == query::short_read::no);
    BOOST_REQUIRE(page.repair_diffs.empty());
}

SEASTAR_THREAD_TEST_CASE(test_identical_replies_need_no_repair) {
    test_env env;
    auto s = env.table_schema;
    auto contents = make_contents({make_row(s, 1, 1, 10, 1), make_row(s, 1, 2, 20, 1)});
    auto cmd = make_command(*s, partition_slice_builder(*s).build(), query::max_rows);

    std::vector<mutation_page_reply> replies;
    replies.push_back({replica_a, env.read_mutation_page(s, contents, cmd)});
    replies.push_back({replica_b, env.read_mutation_page(s, contents, cmd)});
    auto resolution = resolve_mutation_page(s, cmd, cmd, std::move(replies)).get();

    auto& page = require_accepted(resolution);
    BOOST_REQUIRE_EQUAL(rows_of(s, cmd, page.result), (rows{{1, 10}, {2, 20}}));
    BOOST_REQUIRE(page.repair_diffs.empty());
}

SEASTAR_THREAD_TEST_CASE(test_divergent_replies_are_merged_and_repaired) {
    test_env env;
    auto s = env.table_schema;
    auto a = make_contents({make_row(s, 1, 1, 10, 1)});
    auto b = make_contents({make_row(s, 1, 1, 11, 2), make_row(s, 1, 2, 20, 1)});
    auto cmd = make_command(*s, partition_slice_builder(*s).build(), query::max_rows);

    std::vector<mutation_page_reply> replies;
    replies.push_back({replica_a, env.read_mutation_page(s, a, cmd)});
    replies.push_back({replica_b, env.read_mutation_page(s, b, cmd)});
    auto resolution = resolve_mutation_page(s, cmd, cmd, std::move(replies)).get();

    auto& page = require_accepted(resolution);
    BOOST_REQUIRE_EQUAL(rows_of(s, cmd, page.result), (rows{{1, 11}, {2, 20}}));

    auto& diff_a = diff_for(*s, page, 1, replica_a);
    BOOST_REQUIRE(diff_a);
    mutation repaired_a = a.front();
    repaired_a.apply(*diff_a);
    assert_that(repaired_a).is_equal_to(merged(a, b));
    BOOST_REQUIRE(!diff_for(*s, page, 1, replica_b));
}

// Replica A fills the row limit, but B deletes one of A's rows. The merged
// page has fewer live rows than the limit. A may have more rows after its
// last one, so the coordinator retries with a larger row limit.
SEASTAR_THREAD_TEST_CASE(test_rows_lost_in_merge_cause_a_retry) {
    test_env env;
    auto s = env.table_schema;
    auto a = make_contents({make_row(s, 1, 1, 10, 1), make_row(s, 1, 2, 20, 1), make_row(s, 1, 3, 30, 1)});
    auto b = make_contents({make_row_deletion(s, 1, 1, 2)});
    auto cmd = make_command(*s, partition_slice_builder(*s).build(), 2);

    std::vector<mutation_page_reply> replies;
    replies.push_back({replica_a, env.read_mutation_page(s, a, cmd)});
    replies.push_back({replica_b, env.read_mutation_page(s, b, cmd)});
    auto resolution = resolve_mutation_page(s, cmd, cmd, std::move(replies)).get();

    auto& retry = require_retry(resolution);
    // The new limit is t*t/l + 1, where t = 2 rows were requested and l = 1
    // live row survived the merge.
    BOOST_REQUIRE_EQUAL(retry.get_row_limit(), 5);
    BOOST_REQUIRE_EQUAL(retry.partition_limit, query::max_partitions);
    BOOST_REQUIRE_EQUAL(retry.slice.partition_row_limit(), query::partition_max_rows);
}

// The same replies as above, but the command allows short reads. The merged
// page is accepted as a short read instead of a retry.
SEASTAR_THREAD_TEST_CASE(test_rows_lost_in_merge_give_a_short_page_when_allowed) {
    test_env env;
    auto s = env.table_schema;
    auto a = make_contents({make_row(s, 1, 1, 10, 1), make_row(s, 1, 2, 20, 1), make_row(s, 1, 3, 30, 1)});
    auto b = make_contents({make_row_deletion(s, 1, 1, 2)});
    auto slice = partition_slice_builder(*s).with_option<query::partition_slice::option::allow_short_read>().build();
    auto cmd = make_command(*s, std::move(slice), 2);

    std::vector<mutation_page_reply> replies;
    replies.push_back({replica_a, env.read_mutation_page(s, a, cmd)});
    replies.push_back({replica_b, env.read_mutation_page(s, b, cmd)});
    auto resolution = resolve_mutation_page(s, cmd, cmd, std::move(replies)).get();

    auto& page = require_accepted(resolution);
    BOOST_REQUIRE_EQUAL(rows_of(s, cmd, page.result), (rows{{2, 20}}));
    BOOST_REQUIRE(page.result.is_short_read() == query::short_read::yes);
    BOOST_REQUIRE(diff_for(*s, page, 1, replica_a));
}

// When no live row survives the merge, the retry disallows short reads. The
// replicas then cannot stop on their size limit before they return a live
// row.
SEASTAR_THREAD_TEST_CASE(test_retry_without_live_rows_disallows_short_reads) {
    test_env env;
    auto s = env.table_schema;
    auto a = make_contents({make_row(s, 1, 1, 10, 1), make_row(s, 1, 2, 20, 1), make_row(s, 1, 3, 30, 1)});
    auto b = make_contents({make_row_deletion(s, 1, 1, 2), make_row_deletion(s, 1, 2, 2)});
    auto slice = partition_slice_builder(*s).with_option<query::partition_slice::option::allow_short_read>().build();
    auto cmd = make_command(*s, std::move(slice), 2);

    std::vector<mutation_page_reply> replies;
    replies.push_back({replica_a, env.read_mutation_page(s, a, cmd)});
    replies.push_back({replica_b, env.read_mutation_page(s, b, cmd)});
    auto resolution = resolve_mutation_page(s, cmd, cmd, std::move(replies)).get();

    auto& retry = require_retry(resolution);
    // With no live row, the new limit is t + 1, where t = 2 rows were
    // requested.
    BOOST_REQUIRE_EQUAL(retry.get_row_limit(), 3);
    BOOST_REQUIRE(!retry.slice.options.contains<query::partition_slice::option::allow_short_read>());
}

// Replica A stops short on its size limit after row 1. B returns rows 1 to 3.
// The page is trimmed to end at A's last row and is marked short. The repair
// differences are computed before the trimming, so they cover all merged
// rows.
SEASTAR_THREAD_TEST_CASE(test_short_replica_trims_the_page) {
    test_env env;
    auto s = env.table_schema;
    auto a = make_contents({make_row(s, 1, 1, 10, 1), make_row(s, 1, 2, 20, 1)});
    auto b = make_contents({make_row(s, 1, 1, 10, 1), make_row(s, 1, 2, 20, 1), make_row(s, 1, 3, 30, 1)});
    auto slice = partition_slice_builder(*s).with_option<query::partition_slice::option::allow_short_read>().build();
    auto cmd = make_command(*s, std::move(slice), query::max_rows);

    auto reply_a = env.read_mutation_page(s, a, cmd, query::max_result_size(1));
    BOOST_REQUIRE(reply_a->is_short_read() == query::short_read::yes);
    BOOST_REQUIRE_EQUAL(reply_a->row_count(), 1);

    std::vector<mutation_page_reply> replies;
    replies.push_back({replica_a, std::move(reply_a)});
    replies.push_back({replica_b, env.read_mutation_page(s, b, cmd)});
    auto resolution = resolve_mutation_page(s, cmd, cmd, std::move(replies)).get();

    auto& page = require_accepted(resolution);
    BOOST_REQUIRE_EQUAL(rows_of(s, cmd, page.result), (rows{{1, 10}}));
    BOOST_REQUIRE(page.result.is_short_read() == query::short_read::yes);
    require_position(*s, page.result.last_position(), row_position(*s, 1, 1));

    auto& diff_a = diff_for(*s, page, 1, replica_a);
    BOOST_REQUIRE(diff_a);
    mutation repaired_a = make_row(s, 1, 1, 10, 1);
    repaired_a.apply(*diff_a);
    assert_that(repaired_a).is_equal_to(merged(a, b));
}

// Replica A fills the per-partition row limit with a row which B deleted. The
// retry raises the per-partition row limit.
SEASTAR_THREAD_TEST_CASE(test_rows_lost_in_merge_raise_the_per_partition_limit) {
    test_env env;
    auto s = env.table_schema;
    auto a = make_contents({make_row(s, 1, 1, 10, 1), make_row(s, 1, 2, 20, 1)});
    auto b = make_contents({make_row_deletion(s, 1, 1, 2)});
    auto cmd = make_command(*s, partition_slice_builder(*s).with_partition_row_limit(1).build(), query::max_rows);

    std::vector<mutation_page_reply> replies;
    replies.push_back({replica_a, env.read_mutation_page(s, a, cmd)});
    replies.push_back({replica_b, env.read_mutation_page(s, b, cmd)});
    auto resolution = resolve_mutation_page(s, cmd, cmd, std::move(replies)).get();

    auto& retry = require_retry(resolution);
    // With no live row, the new limit is t + 1, where t = 1 row was requested
    // per partition.
    BOOST_REQUIRE_EQUAL(retry.slice.partition_row_limit(), 2);
    BOOST_REQUIRE_EQUAL(retry.get_row_limit(), query::max_rows);
}

// The replicas read with the larger limit of a retry command, but the page
// keeps the row limit of the client's original command.
SEASTAR_THREAD_TEST_CASE(test_retry_round_keeps_the_original_limits) {
    test_env env;
    auto s = env.table_schema;
    auto a = make_contents({make_row(s, 1, 1, 10, 1), make_row(s, 1, 2, 20, 1), make_row(s, 1, 3, 30, 1), make_row(s, 1, 4, 40, 1)});
    auto b = make_contents({make_row_deletion(s, 1, 1, 2)});
    auto original_cmd = make_command(*s, partition_slice_builder(*s).build(), 2);
    auto retry_cmd = original_cmd;
    retry_cmd.set_row_limit(5);

    std::vector<mutation_page_reply> replies;
    replies.push_back({replica_a, env.read_mutation_page(s, a, retry_cmd)});
    replies.push_back({replica_b, env.read_mutation_page(s, b, retry_cmd)});
    auto resolution = resolve_mutation_page(s, original_cmd, retry_cmd, std::move(replies)).get();

    auto& page = require_accepted(resolution);
    BOOST_REQUIRE_EQUAL(rows_of(s, original_cmd, page.result), (rows{{2, 20}, {3, 30}}));
    // The conversion to a data page stops at the original row limit. So the
    // cursor is at the page's last row, row 3, and not at the replies' last
    // row, row 4.
    require_position(*s, page.result.last_position(), row_position(*s, 1, 3));
}

// In a reversed query, the replies and the page use the reversed schema. The
// repair differences use the table schema.
SEASTAR_THREAD_TEST_CASE(test_reversed_query_repairs_in_table_order) {
    test_env env;
    auto s = env.table_schema;
    auto query_schema = s->make_reversed();
    auto a = make_contents({make_row(s, 1, 1, 10, 1)});
    auto b = make_contents({make_row(s, 1, 1, 11, 2), make_row(s, 1, 2, 20, 1)});
    auto cmd = make_command(*query_schema, partition_slice_builder(*query_schema).reversed().build(), query::max_rows);

    std::vector<mutation_page_reply> replies;
    replies.push_back({replica_a, env.read_mutation_page(query_schema, a, cmd)});
    replies.push_back({replica_b, env.read_mutation_page(query_schema, b, cmd)});
    auto resolution = resolve_mutation_page(query_schema, cmd, cmd, std::move(replies)).get();

    auto& page = require_accepted(resolution);
    BOOST_REQUIRE_EQUAL(rows_of(query_schema, cmd, page.result), (rows{{2, 20}, {1, 11}}));
    require_position(*query_schema, page.result.last_position(), row_position(*query_schema, 1, 1));

    auto& diff_a = diff_for(*s, page, 1, replica_a);
    BOOST_REQUIRE(diff_a);
    BOOST_REQUIRE_EQUAL(diff_a->schema()->version(), s->version());
    mutation repaired_a = a.front();
    repaired_a.apply(*diff_a);
    assert_that(repaired_a).is_equal_to(merged(a, b));
}

// Replica B has a deletion of row 3, which A lacks. The page ends with row 2.
// Its cursor is at the dead row 3, because row 3 is the last fragment which
// the conversion to a data page consumed.
SEASTAR_THREAD_TEST_CASE(test_accepted_page_cursor_at_a_trailing_dead_row) {
    test_env env;
    auto s = env.table_schema;
    auto a = make_contents({make_row(s, 1, 1, 10, 1), make_row(s, 1, 2, 20, 1)});
    auto b = make_contents({make_row(s, 1, 1, 10, 1), make_row(s, 1, 2, 20, 1), make_row_deletion(s, 1, 3, 2)});
    auto cmd = make_command(*s, partition_slice_builder(*s).build(), query::max_rows);

    std::vector<mutation_page_reply> replies;
    replies.push_back({replica_a, env.read_mutation_page(s, a, cmd)});
    replies.push_back({replica_b, env.read_mutation_page(s, b, cmd)});
    auto resolution = resolve_mutation_page(s, cmd, cmd, std::move(replies)).get();

    auto& page = require_accepted(resolution);
    BOOST_REQUIRE_EQUAL(rows_of(s, cmd, page.result), (rows{{1, 10}, {2, 20}}));
    BOOST_REQUIRE(page.result.is_short_read() == query::short_read::no);
    require_position(*s, page.result.last_position(), row_position(*s, 1, 3));
}

// Replica B deletes A's only row. The page has no rows and is not short. It
// has a cursor at the dead row.
SEASTAR_THREAD_TEST_CASE(test_accepted_page_without_rows_has_a_cursor) {
    test_env env;
    auto s = env.table_schema;
    auto a = make_contents({make_row(s, 1, 1, 10, 1)});
    auto b = make_contents({make_row_deletion(s, 1, 1, 2)});
    auto cmd = make_command(*s, partition_slice_builder(*s).build(), query::max_rows);

    std::vector<mutation_page_reply> replies;
    replies.push_back({replica_a, env.read_mutation_page(s, a, cmd)});
    replies.push_back({replica_b, env.read_mutation_page(s, b, cmd)});
    auto resolution = resolve_mutation_page(s, cmd, cmd, std::move(replies)).get();

    auto& page = require_accepted(resolution);
    BOOST_REQUIRE(rows_of(s, cmd, page.result).empty());
    BOOST_REQUIRE(page.result.is_short_read() == query::short_read::no);
    require_position(*s, page.result.last_position(), row_position(*s, 1, 1));
}

// The reversed counterpart of test_short_replica_trims_the_page. In query
// order, replica A has rows 2 and 1 and stops short after row 2. B has rows 3,
// 2 and 1.
//
// This records the current behavior, which is wrong. The page drops row 3,
// which A has passed, and keeps row 1, which A has not reached. The forward
// counterpart keeps the rows up to A's last row and drops the rest, so here
// the page would keep rows 3 and 2.
//
// The cause: the replies use the reversed schema, whose clustering order is
// already the query order. The resolver's progress checks and trimming
// reverse that order once more, as they would for replies in table order.
SEASTAR_THREAD_TEST_CASE(test_short_replica_trims_a_reversed_page) {
    test_env env;
    auto s = env.table_schema;
    auto query_schema = s->make_reversed();
    auto a = make_contents({make_row(s, 1, 1, 10, 1), make_row(s, 1, 2, 20, 1)});
    auto b = make_contents({make_row(s, 1, 1, 10, 1), make_row(s, 1, 2, 20, 1), make_row(s, 1, 3, 30, 1)});
    auto slice = partition_slice_builder(*query_schema).reversed().with_option<query::partition_slice::option::allow_short_read>().build();
    auto cmd = make_command(*query_schema, std::move(slice), query::max_rows);

    auto reply_a = env.read_mutation_page(query_schema, a, cmd, query::max_result_size(1));
    BOOST_REQUIRE(reply_a->is_short_read() == query::short_read::yes);
    BOOST_REQUIRE_EQUAL(reply_a->row_count(), 1);

    std::vector<mutation_page_reply> replies;
    replies.push_back({replica_a, std::move(reply_a)});
    replies.push_back({replica_b, env.read_mutation_page(query_schema, b, cmd)});
    auto resolution = resolve_mutation_page(query_schema, cmd, cmd, std::move(replies)).get();

    auto& page = require_accepted(resolution);
    BOOST_REQUIRE_EQUAL(rows_of(query_schema, cmd, page.result), (rows{{2, 20}, {1, 10}}));
    BOOST_REQUIRE(page.result.is_short_read() == query::short_read::yes);
    require_position(*query_schema, page.result.last_position(), row_position(*query_schema, 1, 1));
}

namespace {

// Initializes the schema registry. The conversions between the legacy and
// the native reversed format look up schemas in it.
struct schema_registry_init {
    std::unique_ptr<db::config> config;
    gms::feature_service features;

    schema_registry_init()
        : config(std::make_unique<db::config>())
        , features({get_disabled_features_from_db_config(*config)}) {
        local_schema_registry().init(db::schema_ctxt(config->extensions(), config->murmur3_partitioner_ignore_msb_bits(),
                                                     std::make_shared<data_dictionary::dummy_user_types_storage>(), features),
                                     std::chrono::seconds(config->schema_registry_grace_period()));
    }
};

} // anonymous namespace

// A replica without native reversed queries receives a reversed command in
// legacy format, and returns its reply in table order. On the coordinator,
// make_mutation_data_request() converts the command and the reply with
// reversed(). On the replica, handle_read() converts them back. The test
// checks that the converted reply equals the native reply and resolves to
// the same page.
SEASTAR_THREAD_TEST_CASE(test_legacy_reversed_reply_resolves_like_a_native_one) {
    schema_registry_init registry;
    test_env env;
    auto s = local_schema_registry().learn(env.table_schema);
    auto query_schema = s->get_reversed();
    auto a = make_contents({make_row(s, 1, 1, 10, 1)});
    auto b = make_contents({make_row(s, 1, 1, 11, 2), make_row(s, 1, 2, 20, 1)});
    auto cmd = make_command(*query_schema, partition_slice_builder(*query_schema).reversed().build(), query::max_rows);

    // Reads a mutation page from a replica without native reversed queries.
    auto read_legacy = [&] (const utils::chunked_vector<mutation>& contents) {
        // make_mutation_data_request() sends the command in legacy format.
        auto legacy_cmd = reversed(make_lw_shared<query::read_command>(cmd));
        BOOST_REQUIRE(legacy_cmd->schema_version == s->version());
        // handle_read() converts the command to native format, and the reply
        // to legacy format.
        auto native_cmd = reversed(make_lw_shared<query::read_command>(*legacy_cmd));
        BOOST_REQUIRE(native_cmd->schema_version == query_schema->version());
        auto legacy_reply = reversed(env.read_mutation_page(query_schema, contents, *native_cmd)).get();
        for (const partition& p : legacy_reply->partitions()) {
            BOOST_REQUIRE(p.mut().schema_version() == s->version());
        }
        // make_mutation_data_request() converts the reply to native format.
        return reversed(std::move(legacy_reply)).get();
    };

    auto native_b = env.read_mutation_page(query_schema, b, cmd);
    auto legacy_b = read_legacy(b);
    BOOST_REQUIRE_EQUAL(legacy_b->row_count(), native_b->row_count());
    BOOST_REQUIRE(legacy_b->is_short_read() == native_b->is_short_read());
    BOOST_REQUIRE_EQUAL(legacy_b->partitions().size(), native_b->partitions().size());
    for (auto&& [legacy, native] : std::views::zip(legacy_b->partitions(), native_b->partitions())) {
        BOOST_REQUIRE(legacy.mut().schema_version() == query_schema->version());
        BOOST_REQUIRE_EQUAL(legacy.row_count(), native.row_count());
        assert_that(legacy.mut().unfreeze(query_schema)).is_equal_to(native.mut().unfreeze(query_schema));
    }

    auto resolve = [&] (foreign_ptr<lw_shared_ptr<reconcilable_result>> reply_b) {
        std::vector<mutation_page_reply> replies;
        replies.push_back({replica_a, env.read_mutation_page(query_schema, a, cmd)});
        replies.push_back({replica_b, std::move(reply_b)});
        return resolve_mutation_page(query_schema, cmd, cmd, std::move(replies)).get();
    };
    auto native_resolution = resolve(std::move(native_b));
    auto legacy_resolution = resolve(std::move(legacy_b));
    auto& native_page = require_accepted(native_resolution);
    auto& legacy_page = require_accepted(legacy_resolution);
    BOOST_REQUIRE_EQUAL(rows_of(query_schema, cmd, native_page.result), (rows{{2, 20}, {1, 11}}));
    BOOST_REQUIRE_EQUAL(rows_of(query_schema, cmd, legacy_page.result), (rows{{2, 20}, {1, 11}}));
    require_position(*query_schema, native_page.result.last_position(), row_position(*query_schema, 1, 1));
    require_position(*query_schema, legacy_page.result.last_position(), row_position(*query_schema, 1, 1));
    auto& native_diff = diff_for(*s, native_page, 1, replica_a);
    auto& legacy_diff = diff_for(*s, legacy_page, 1, replica_a);
    BOOST_REQUIRE(native_diff && legacy_diff);
    assert_that(*legacy_diff).is_equal_to(*native_diff);
}

// prepare_mutation_read() sets the allow_mutation_read_page_without_live_row
// option when the empty_replica_mutation_pages feature is enabled. The option
// lets a mutation page stop on its size limit after a dead row. Without it,
// the page continues to the next live row.
SEASTAR_THREAD_TEST_CASE(test_mutation_page_can_stop_without_a_live_row) {
    test_env env;
    auto s = env.table_schema;
    auto contents = make_contents({make_row(s, 1, 1, 10, 1), make_row_deletion(s, 1, 1, 2), make_row(s, 1, 2, 20, 1)});
    auto slice = partition_slice_builder(*s).with_option<query::partition_slice::option::allow_short_read>().build();
    auto cmd = make_command(*s, std::move(slice), query::max_rows);

    prepare_mutation_read(cmd, false);
    BOOST_REQUIRE(!cmd.slice.options.contains<query::partition_slice::option::allow_mutation_read_page_without_live_row>());
    auto reply = env.read_mutation_page(s, contents, cmd, query::max_result_size(1));
    BOOST_REQUIRE(reply->is_short_read() == query::short_read::yes);
    BOOST_REQUIRE_EQUAL(reply->row_count(), 1);

    prepare_mutation_read(cmd, true);
    BOOST_REQUIRE(cmd.slice.options.contains<query::partition_slice::option::allow_mutation_read_page_without_live_row>());
    reply = env.read_mutation_page(s, contents, cmd, query::max_result_size(1));
    BOOST_REQUIRE(reply->is_short_read() == query::short_read::yes);
    BOOST_REQUIRE_EQUAL(reply->row_count(), 0);
    BOOST_REQUIRE_EQUAL(reply->partitions().size(), 1);
    assert_that(reply->partitions().front().mut().unfreeze(s)).is_equal_to(make_row_deletion(s, 1, 1, 2));
}

// With allow_mutation_read_page_without_live_row, two replicas stop on their
// size limit after the dead row 1. The reconciled page is empty and short.
// Its cursor is at the dead row.
SEASTAR_THREAD_TEST_CASE(test_replies_without_live_rows_give_an_empty_short_page) {
    test_env env;
    auto s = env.table_schema;
    auto contents = make_contents({make_row(s, 1, 1, 10, 1), make_row_deletion(s, 1, 1, 2), make_row(s, 1, 2, 20, 1)});
    auto slice = partition_slice_builder(*s).with_option<query::partition_slice::option::allow_short_read>().build();
    auto cmd = make_command(*s, std::move(slice), query::max_rows);
    prepare_mutation_read(cmd, true);

    std::vector<mutation_page_reply> replies;
    replies.push_back({replica_a, env.read_mutation_page(s, contents, cmd, query::max_result_size(1))});
    replies.push_back({replica_b, env.read_mutation_page(s, contents, cmd, query::max_result_size(1))});
    auto resolution = resolve_mutation_page(s, cmd, cmd, std::move(replies)).get();

    auto& page = require_accepted(resolution);
    BOOST_REQUIRE(rows_of(s, cmd, page.result).empty());
    BOOST_REQUIRE(page.result.is_short_read() == query::short_read::yes);
    require_position(*s, page.result.last_position(), row_position(*s, 1, 1));
    BOOST_REQUIRE(page.repair_diffs.empty());
}

namespace {

const accepted_digest_page& require_accepted(const digest_page_decision& decision) {
    BOOST_REQUIRE(std::holds_alternative<accepted_digest_page>(decision));
    return std::get<accepted_digest_page>(decision);
}

// Replica A has rows 1 and 2 of partition 1. Replica B has the same rows and
// a deletion of row 3, which A lacks. Their digests match, because a digest
// covers only live data. A's cursor is at row 2, and B's cursor is at the
// dead row 3.
struct matching_replicas {
    utils::chunked_vector<mutation> a;
    utils::chunked_vector<mutation> b;

    explicit matching_replicas(schema_ptr s)
        : a(make_contents({make_row(s, 1, 1, 10, 1), make_row(s, 1, 2, 20, 1)}))
        , b(make_contents({make_row(s, 1, 1, 10, 1), make_row(s, 1, 2, 20, 1), make_row_deletion(s, 1, 3, 2)}))
    {}
};

// Decides the page of a read with two targets and CL=2. The digest reply
// arrives first. The data reply arrives second and reaches the consistency
// level.
digest_page_decision decide_data_and_digest(test_env& env, const query::read_command& cmd, const utils::chunked_vector<mutation>& data_contents,
        const utils::chunked_vector<mutation>& digest_contents, bool empty_replica_pages) {
    auto s = env.table_schema;
    foreground_reply_collector replies(s, 2);
    replies.add_wait_targets(2);
    auto cl = replies.has_cl();
    add_digest(replies, true, env.read_digest_page(s, digest_contents, cmd));
    replies.add_data(true, env.read_data_page(s, data_contents, cmd, data_and_digest));
    return decide_digest_page(*s, cmd, cl.get().value(), replies, empty_replica_pages, false);
}

// Attaches the decision to has_cl() the way abstract_read_executor::execute()
// does: in a continuation. The continuation runs later than the add_data() or
// add_digest() call which reached the consistency level. Replies added in
// between are visible to the decision.
future<digest_page_decision> decide_at_cl(const schema& s, const query::read_command& cmd, foreground_reply_collector& replies, bool empty_replica_pages) {
    return replies.has_cl().then([&s, &cmd, &replies, empty_replica_pages] (exceptions::coordinator_result<digest_read_result> cl_result) {
        return decide_digest_page(s, cmd, std::move(cl_result).value(), replies, empty_replica_pages, false);
    });
}

exceptions::coordinator_exception_container make_timeout(const schema& s) {
    return exceptions::read_timeout_exception(s.ks_name(), s.cf_name(), db::consistency_level::QUORUM, 1, 2, true);
}

} // anonymous namespace

// With a single target, the data request asks for no digest, and the data
// reply alone reaches the consistency level.
SEASTAR_THREAD_TEST_CASE(test_single_data_reply_reaches_cl) {
    test_env env;
    auto s = env.table_schema;
    auto contents = make_contents({make_row(s, 1, 1, 10, 1), make_row(s, 1, 2, 20, 1)});
    auto cmd = make_command(*s, partition_slice_builder(*s).build(), query::max_rows);

    foreground_reply_collector replies(s, 1);
    replies.add_wait_targets(1);
    auto cl = replies.has_cl();
    BOOST_REQUIRE(!cl.available());
    replies.add_data(true, env.read_data_page(s, contents, cmd, data_only));
    BOOST_REQUIRE(cl.available());
    BOOST_REQUIRE(replies.is_completed());

    auto decision = decide_digest_page(*s, cmd, cl.get().value(), replies, true, false);
    auto& page = require_accepted(decision);
    BOOST_REQUIRE_EQUAL(rows_of(s, cmd, *page.result), (rows{{1, 10}, {2, 20}}));
    require_position(*s, page.result->last_position(), row_position(*s, 1, 2));
}

// Digest replies alone do not reach the consistency level, even when there
// are enough of them.
SEASTAR_THREAD_TEST_CASE(test_cl_waits_for_a_data_reply) {
    test_env env;
    auto s = env.table_schema;
    auto contents = make_contents({make_row(s, 1, 1, 10, 1)});
    auto cmd = make_command(*s, partition_slice_builder(*s).build(), query::max_rows);

    // CL=QUORUM with three targets.
    foreground_reply_collector replies(s, 2);
    replies.add_wait_targets(3);
    auto cl = replies.has_cl();
    add_digest(replies, true, env.read_digest_page(s, contents, cmd));
    add_digest(replies, true, env.read_digest_page(s, contents, cmd));
    BOOST_REQUIRE_EQUAL(replies.cl_responses(), 2);
    BOOST_REQUIRE(!cl.available());

    replies.add_data(true, env.read_data_page(s, contents, cmd, data_and_digest));
    BOOST_REQUIRE(cl.available());
    BOOST_REQUIRE(replies.is_completed());
    digest_read_result cl_result = cl.get().value();
    BOOST_REQUIRE(cl_result.digests_match);
}

// Replies from replicas which do not count toward the consistency level are
// collected, but do not reach it.
SEASTAR_THREAD_TEST_CASE(test_replies_outside_cl_do_not_count) {
    test_env env;
    auto s = env.table_schema;
    auto contents = make_contents({make_row(s, 1, 1, 10, 1)});
    auto cmd = make_command(*s, partition_slice_builder(*s).build(), query::max_rows);

    // CL=LOCAL_QUORUM with two local targets and a remote one.
    foreground_reply_collector replies(s, 2);
    replies.add_wait_targets(3);
    auto cl = replies.has_cl();
    replies.add_data(true, env.read_data_page(s, contents, cmd, data_and_digest));
    add_digest(replies, false, env.read_digest_page(s, contents, cmd));
    BOOST_REQUIRE_EQUAL(replies.cl_responses(), 1);
    BOOST_REQUIRE_EQUAL(replies.response_count(), 2);
    BOOST_REQUIRE(!cl.available());

    add_digest(replies, true, env.read_digest_page(s, contents, cmd));
    BOOST_REQUIRE_EQUAL(replies.cl_responses(), 2);
    BOOST_REQUIRE(cl.available());
}

// When the digests of the replies differ, the decision is to reconcile.
SEASTAR_THREAD_TEST_CASE(test_digest_mismatch_requests_reconciliation) {
    test_env env;
    auto s = env.table_schema;
    auto a = make_contents({make_row(s, 1, 1, 10, 1)});
    auto b = make_contents({make_row(s, 1, 1, 11, 2)});
    auto cmd = make_command(*s, partition_slice_builder(*s).build(), query::max_rows);

    auto decision = decide_data_and_digest(env, cmd, a, b, true);
    BOOST_REQUIRE(std::holds_alternative<digest_page_mismatch>(decision));
}

// With the empty_replica_pages feature, the page takes the earliest cursor of
// all collected replies. Without the feature, the page keeps the cursor of
// the data reply.
SEASTAR_THREAD_TEST_CASE(test_matching_digests_take_the_earliest_cursor) {
    test_env env;
    auto s = env.table_schema;
    matching_replicas replicas(s);
    auto cmd = make_command(*s, partition_slice_builder(*s).build(), query::max_rows);

    // B sends the data reply, and A sends the digest reply. Without the
    // feature, the page keeps the cursor of B's data reply.
    {
        auto decision = decide_data_and_digest(env, cmd, replicas.b, replicas.a, false);
        auto& page = require_accepted(decision);
        BOOST_REQUIRE_EQUAL(rows_of(s, cmd, *page.result), (rows{{1, 10}, {2, 20}}));
        require_position(*s, page.result->last_position(), row_position(*s, 1, 3));
    }
    // With the feature, the page takes the earlier cursor of A's digest
    // reply.
    {
        auto decision = decide_data_and_digest(env, cmd, replicas.b, replicas.a, true);
        auto& page = require_accepted(decision);
        BOOST_REQUIRE_EQUAL(rows_of(s, cmd, *page.result), (rows{{1, 10}, {2, 20}}));
        require_position(*s, page.result->last_position(), row_position(*s, 1, 2));
    }
    // With the roles swapped, the digest reply has the later cursor. It does
    // not move the page's cursor.
    {
        auto decision = decide_data_and_digest(env, cmd, replicas.a, replicas.b, true);
        auto& page = require_accepted(decision);
        require_position(*s, page.result->last_position(), row_position(*s, 1, 2));
    }
}

// A reply which arrives after the consistency level was reached, but before
// the continuation runs, still moves the page's cursor. This records the
// current behavior: the page's cursor depends on when the continuation runs.
SEASTAR_THREAD_TEST_CASE(test_reply_after_cl_moves_the_cursor) {
    test_env env;
    auto s = env.table_schema;
    matching_replicas replicas(s);
    auto cmd = make_command(*s, partition_slice_builder(*s).build(), query::max_rows);

    // Both replies are read before either is added. A read can yield, which
    // would let the continuation run before the late reply is added.
    auto data = env.read_data_page(s, replicas.b, cmd, data_and_digest);
    auto digest = env.read_digest_page(s, replicas.a, cmd);

    // CL=ONE with a read repair to a second target.
    foreground_reply_collector replies(s, 1);
    replies.add_wait_targets(2);
    auto decision = decide_at_cl(*s, cmd, replies, true);
    replies.add_data(true, std::move(data));
    BOOST_REQUIRE(!decision.available());
    add_digest(replies, true, std::move(digest));

    auto late = decision.get();
    require_position(*s, require_accepted(late).result->last_position(), row_position(*s, 1, 2));

    // The same read without the late reply keeps the cursor of the data reply.
    foreground_reply_collector replay(s, 1);
    replay.add_wait_targets(2);
    auto replay_decision = decide_at_cl(*s, cmd, replay, true);
    replay.add_data(true, env.read_data_page(s, replicas.b, cmd, data_and_digest));
    auto on_time = replay_decision.get();
    require_position(*s, require_accepted(on_time).result->last_position(), row_position(*s, 1, 3));
}

// A digest reply which conflicts with the data reply and stops short after
// row 1 arrives after the consistency level was reached, but before the
// continuation runs.
//
// This records the current behavior, which is wrong. The decision uses the
// digest comparison from the moment the consistency level was reached, so it
// accepts the page. The page keeps both rows of the data reply, but takes the
// cursor of the late reply, which is at row 1. The cursor is thus before the
// page's last row, so the next page starts before row 2 and returns it again.
// The page is not marked short.
SEASTAR_THREAD_TEST_CASE(test_conflicting_short_reply_after_cl) {
    test_env env;
    auto s = env.table_schema;
    auto a = make_contents({make_row(s, 1, 1, 10, 1), make_row(s, 1, 2, 20, 1)});
    auto c = make_contents({make_row(s, 1, 1, 11, 2), make_row(s, 1, 2, 20, 1)});
    auto slice = partition_slice_builder(*s).with_option<query::partition_slice::option::allow_short_read>().build();
    auto cmd = make_command(*s, std::move(slice), query::max_rows);

    // Both replies are read before either is added. A read can yield, which
    // would let the continuation run before the late reply is added.
    auto data = env.read_data_page(s, a, cmd, data_and_digest);
    auto late = env.read_digest_page(s, c, cmd, query::max_result_size(1));
    require_position(*s, late.last_pos, row_position(*s, 1, 1));

    // CL=ONE with a read repair to a second target.
    foreground_reply_collector replies(s, 1);
    replies.add_wait_targets(2);
    auto decision = decide_at_cl(*s, cmd, replies, true);
    replies.add_data(true, std::move(data));
    BOOST_REQUIRE(!decision.available());
    add_digest(replies, true, std::move(late));

    auto result = decision.get();
    auto& page = require_accepted(result);
    BOOST_REQUIRE_EQUAL(rows_of(s, cmd, *page.result), (rows{{1, 10}, {2, 20}}));
    BOOST_REQUIRE(page.result->is_short_read() == query::short_read::no);
    require_position(*s, page.result->last_position(), row_position(*s, 1, 1));
    // The conflict leads only to a background reconciliation.
    // abstract_read_executor::execute() starts one after all targets replied,
    // if not all digests match.
    BOOST_REQUIRE(!replies.digests_match());
}

// A failure after the consistency level was reached, but before the
// continuation runs, drops the collected replies. The page then keeps the
// cursor of B's data reply, although A's digest reply had an earlier cursor.
// This records the current behavior.
SEASTAR_THREAD_TEST_CASE(test_failure_after_cl_drops_the_replies) {
    test_env env;
    auto s = env.table_schema;
    matching_replicas replicas(s);
    auto cmd = make_command(*s, partition_slice_builder(*s).build(), query::max_rows);

    foreground_reply_collector replies(s, 2);
    replies.add_wait_targets(3);
    auto decision = decide_at_cl(*s, cmd, replies, true);
    add_digest(replies, true, env.read_digest_page(s, replicas.a, cmd));
    replies.add_data(true, env.read_data_page(s, replicas.b, cmd, data_and_digest));
    BOOST_REQUIRE(!decision.available());
    replies.fail(make_timeout(*s));
    BOOST_REQUIRE_EQUAL(replies.response_count(), 0);

    auto result = decision.get();
    require_position(*s, require_accepted(result).result->last_position(), row_position(*s, 1, 3));
}

// A failure before the consistency level is reached resolves has_cl() with
// the error, and drops the collected replies.
SEASTAR_THREAD_TEST_CASE(test_failure_before_cl_fails_has_cl) {
    test_env env;
    auto s = env.table_schema;
    auto contents = make_contents({make_row(s, 1, 1, 10, 1)});
    auto cmd = make_command(*s, partition_slice_builder(*s).build(), query::max_rows);

    foreground_reply_collector replies(s, 2);
    replies.add_wait_targets(2);
    auto cl = replies.has_cl();
    replies.add_data(true, env.read_data_page(s, contents, cmd, data_and_digest));
    BOOST_REQUIRE(replies.has_data());
    replies.fail(make_timeout(*s));

    BOOST_REQUIRE(!cl.get());
    BOOST_REQUIRE(!replies.has_data());
    BOOST_REQUIRE_EQUAL(replies.response_count(), 0);
}

namespace {

// `pks` in ring order.
std::vector<int32_t> in_ring_order(const schema& s, std::vector<int32_t> pks) {
    std::ranges::sort(pks, [&s] (int32_t a, int32_t b) {
        return dht::decorate_key(s, make_pk(s, a)).less_compare(s, dht::decorate_key(s, make_pk(s, b)));
    });
    return pks;
}

dht::partition_range singular_range(const schema& s, int32_t pk) {
    return dht::partition_range::make_singular(dht::decorate_key(s, make_pk(s, pk)));
}

// Closes the saved querier which a test leaves behind.
auto close_saved(std::optional<replica::querier>& saved) {
    return defer([&saved] () noexcept {
        if (saved) {
            saved->close().get();
        }
    });
}

} // anonymous namespace

// A data page reads its ranges in order, each with a new querier. A querier
// is the reader state which a page can save for the next page. The cursor is
// the current position of the querier which read last.
SEASTAR_THREAD_TEST_CASE(test_data_page_over_several_ranges) {
    test_env env;
    auto s = env.table_schema;
    auto pks = in_ring_order(*s, {1, 2});
    auto contents = make_contents({make_row(s, pks[0], 1, 10 * pks[0] + 1, 1), make_row(s, pks[0], 2, 10 * pks[0] + 2, 1),
            make_row(s, pks[1], 1, 10 * pks[1] + 1, 1), make_row(s, pks[1], 2, 10 * pks[1] + 2, 1)});
    auto cmd = make_command(*s, partition_slice_builder(*s).build(), 3);
    const dht::partition_range_vector ranges{singular_range(*s, pks[0]), singular_range(*s, pks[1])};

    auto result = env.query_data(s, contents, cmd, data_only, ranges, unlimited_size, nullptr);
    BOOST_REQUIRE_EQUAL(rows_of(s, cmd, *result), (rows{{1, 10 * pks[0] + 1}, {2, 10 * pks[0] + 2}, {1, 10 * pks[1] + 1}}));
    BOOST_REQUIRE(result->is_short_read() == query::short_read::no);
    require_position(*s, result->last_position(), row_position(*s, pks[1], 1));
}

// A data page which ends in a range without data has no cursor, although an
// earlier range returned rows. The querier of the last range consumed
// nothing, so it has no position. This records the current behavior.
SEASTAR_THREAD_TEST_CASE(test_data_page_ending_in_an_empty_range_has_no_cursor) {
    test_env env;
    auto s = env.table_schema;
    auto pks = in_ring_order(*s, {1, 2});
    auto contents = make_contents({make_row(s, pks[0], 1, 10, 1)});
    auto cmd = make_command(*s, partition_slice_builder(*s).build(), query::max_rows);
    const dht::partition_range_vector ranges{singular_range(*s, pks[0]), singular_range(*s, pks[1])};

    auto result = env.query_data(s, contents, cmd, data_only, ranges, unlimited_size, nullptr);
    BOOST_REQUIRE_EQUAL(rows_of(s, cmd, *result), (rows{{1, 10}}));
    BOOST_REQUIRE(result->is_short_read() == query::short_read::no);
    BOOST_REQUIRE(!result->last_position());
}

// A data page which reaches its row limit saves its querier, and the next
// page continues with it. A page which exhausts the source closes the
// querier, but still has a cursor at its last row.
SEASTAR_THREAD_TEST_CASE(test_saved_querier_continues_the_next_data_page) {
    test_env env;
    auto s = env.table_schema;
    auto contents = make_contents({make_row(s, 1, 1, 10, 1), make_row(s, 1, 2, 20, 1), make_row(s, 1, 3, 30, 1)});
    auto first_cmd = make_command(*s, partition_slice_builder(*s).build(), 1);
    auto last_cmd = make_command(*s, partition_slice_builder(*s).build(), query::max_rows);
    const dht::partition_range_vector ranges{query::full_partition_range};
    std::optional<replica::querier> saved;
    auto close_saved_querier = close_saved(saved);

    auto first = env.query_data(s, contents, first_cmd, data_only, ranges, unlimited_size, &saved);
    BOOST_REQUIRE_EQUAL(rows_of(s, first_cmd, *first), (rows{{1, 10}}));
    require_position(*s, first->last_position(), row_position(*s, 1, 1));
    BOOST_REQUIRE(saved);

    auto last = env.query_data(s, contents, last_cmd, data_only, ranges, unlimited_size, &saved);
    BOOST_REQUIRE_EQUAL(rows_of(s, last_cmd, *last), (rows{{2, 20}, {3, 30}}));
    BOOST_REQUIRE(last->is_short_read() == query::short_read::no);
    require_position(*s, last->last_position(), row_position(*s, 1, 3));
    BOOST_REQUIRE(!saved);
}

// A mutation page which stops short saves its querier, and the next page
// continues with it. A page which exhausts the source closes the querier.
SEASTAR_THREAD_TEST_CASE(test_saved_querier_continues_a_short_mutation_page) {
    test_env env;
    auto s = env.table_schema;
    auto contents = make_contents({make_row(s, 1, 1, 10, 1), make_row(s, 1, 2, 20, 1), make_row(s, 1, 3, 30, 1)});
    auto slice = partition_slice_builder(*s).with_option<query::partition_slice::option::allow_short_read>().build();
    auto cmd = make_command(*s, std::move(slice), query::max_rows);
    std::optional<replica::querier> saved;
    auto close_saved_querier = close_saved(saved);

    auto first = env.query_mutations(s, contents, cmd, query::full_partition_range, query::max_result_size(1), &saved);
    BOOST_REQUIRE(first.is_short_read() == query::short_read::yes);
    BOOST_REQUIRE_EQUAL(first.row_count(), 1);
    BOOST_REQUIRE(saved);

    auto last = env.query_mutations(s, contents, cmd, query::full_partition_range, unlimited_size, &saved);
    BOOST_REQUIRE(last.is_short_read() == query::short_read::no);
    BOOST_REQUIRE_EQUAL(last.row_count(), 2);
    BOOST_REQUIRE_EQUAL(last.partitions().size(), 1);
    assert_that(last.partitions().front().mut().unfreeze(s))
            .is_equal_to(make_contents({make_row(s, 1, 2, 20, 1), make_row(s, 1, 3, 30, 1)}).front());
    BOOST_REQUIRE(!saved);
}

namespace {

// Registers the tombstone_gc extension, so that a table can disable
// tombstone garbage collection. Reads of such a table then compact like the
// reads in test_env, which use tombstone_gc_state::no_gc().
cql_test_config config_with_tombstone_gc_extension() {
    auto ext = std::make_shared<db::extensions>();
    ext->add_schema_extension<tombstone_gc_extension>(tombstone_gc_extension::NAME);
    return cql_test_config(seastar::make_shared<db::config>(ext));
}

using keyed_rows = std::vector<std::tuple<int32_t, int32_t, int32_t>>;

// The (pk, ck, v) rows of a page, in query order.
keyed_rows keyed_rows_of(schema_ptr s, const query::read_command& cmd, const query::result& result) {
    auto rs = query::result_set::from_raw_result(s, cmd.slice, result);
    return rs.rows() | std::views::transform([] (const query::result_set_row& row) {
        return std::tuple(row.get_nonnull<int32_t>("pk"), row.get_nonnull<int32_t>("ck"), row.get_nonnull<int32_t>("v"));
    }) | std::ranges::to<std::vector>();
}

// The position right after row `ck` of partition `pk`.
full_position after_row(const schema& s, int32_t pk, int32_t ck) {
    return full_position(make_pk(s, pk), position_in_partition::after_key(s, make_ck(s, ck)));
}

full_position end_of_partition(const schema& s, int32_t pk) {
    return full_position(make_pk(s, pk), position_in_partition::for_partition_end());
}

void require_frontier(const schema& s, const std::optional<query::read_frontier>& actual, const query::read_frontier& expected) {
    BOOST_REQUIRE(actual);
    BOOST_REQUIRE_MESSAGE(actual->equal(s, expected), fmt::format("frontier {}, expected {}",
            query::read_frontier::printer{s, *actual}, query::read_frontier::printer{s, expected}));
}

void require_same_position(const schema& s, const std::optional<full_position>& actual, const std::optional<full_position>& expected) {
    BOOST_REQUIRE_EQUAL(bool(actual), bool(expected));
    if (expected) {
        require_position(s, actual, *expected);
    }
}

// The skips of `r`, with the keys of their partitions. See
// reconcilable_result::skips().
std::vector<full_position> skips_of(const schema& s, const reconcilable_result& r) {
    return r.skips() | std::views::transform([&] (const partition_skip& skip) {
        return full_position(r.partitions()[skip.partition].mut().key(), skip.position);
    }) | std::ranges::to<std::vector>();
}

void require_skips(const schema& s, const reconcilable_result& r, const std::vector<full_position>& expected) {
    const auto actual = skips_of(s, r);
    BOOST_REQUIRE_EQUAL(actual.size(), expected.size());
    for (auto&& [a, e] : std::views::zip(actual, expected)) {
        require_position(s, a, e);
    }
}

// `cmd`, asking for a frontier. See query::partition_slice::option::send_read_frontier.
query::read_command with_read_frontier(query::read_command cmd) {
    cmd.slice.options.set<query::partition_slice::option::send_read_frontier>();
    return cmd;
}

// A frontier, and the skips of a mutation page. See
// reconcilable_result::skips().
struct frontier_and_skips {
    query::read_frontier frontier;
    std::vector<full_position> skips;
};

// One case of test_page_driver_and_multishard_producer, with the expected
// data page of each producer.
struct producer_case {
    std::string name;
    query::read_command cmd;
    dht::partition_range_vector ranges;
    keyed_rows rows;
    query::short_read short_read;
    // The cursor of the multishard producer's data page.
    std::optional<full_position> multishard_cursor;
    // The cursor of read_data_page().
    std::optional<full_position> driver_cursor;
    // The frontier of the data and mutation pages of both producers, and the
    // skips of their mutation pages, when the command asks for a frontier.
    frontier_and_skips frontier;
};

} // anonymous namespace

namespace {

// The body of test_page_driver_and_multishard_producer. With `tablets`, the
// table uses tablets, and the multishard producer's cursors are not checked.
void compare_producers(cql_test_env& env, bool tablets) {
    const auto ks = tablets ? "ks_tablets" : "ks_vnodes";
    env.execute_cql(fmt::format("CREATE KEYSPACE {} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}"
            " AND tablets = {{'enabled': {}}}", ks, tablets ? "true, 'initial': 4" : "false")).get();
    env.execute_cql(fmt::format("CREATE TABLE {}.cf (pk int, ck int, s int static, v int, PRIMARY KEY (pk, ck))"
            " WITH tombstone_gc = {{'mode': 'disabled'}}", ks)).get();
    auto s = env.local_db().find_schema(ks, "cf");

    // Partition p has live rows 1 and 2, and a deletion of row 3.
    // Partition q has live rows 1 and 2.
    auto pks = in_ring_order(*s, {1, 2});
    const int32_t p = pks[0];
    const int32_t q = pks[1];
    std::vector<mutation> muts{make_row(s, p, 1, 10 * p + 1, 1), make_row(s, p, 2, 10 * p + 2, 1), make_row_deletion(s, p, 3, 2),
            make_row(s, q, 1, 10 * q + 1, 1), make_row(s, q, 2, 10 * q + 2, 1)};
    for (const auto& m : muts) {
        const auto shard = env.local_db().find_column_family(s).shard_for_reads(m.decorated_key().token());
        smp::submit_to(shard, [&env, gs = global_schema_ptr(s), fm = freeze(m)] () mutable {
            return env.local_db().apply(gs.get(), std::move(fm), {}, db::commitlog_force_sync::no, db::no_timeout);
        }).get();
    }
    auto contents = make_contents(muts);

    const keyed_rows all_rows{{p, 1, 10 * p + 1}, {p, 2, 10 * p + 2}, {q, 1, 10 * q + 1}, {q, 2, 10 * q + 2}};
    const keyed_rows rows_of_p{{p, 1, 10 * p + 1}, {p, 2, 10 * p + 2}};
    const dht::partition_range_vector full_range{query::full_partition_range};
    // Two ranges. The second one has no data.
    const auto q_pos = dht::ring_position(dht::decorate_key(*s, make_pk(*s, q)));
    const dht::partition_range_vector split_ranges{
            dht::partition_range::make_ending_with({q_pos, true}),
            dht::partition_range::make_starting_with({q_pos, false})};

    auto full_slice = partition_slice_builder(*s).build();
    auto short_cmd = make_command(*s, partition_slice_builder(*s).with_option<query::partition_slice::option::allow_short_read>().build(),
            query::max_rows);
    short_cmd.max_result_size = query::max_result_size(1);

    const auto end = frontier_and_skips{query::read_frontier::end(), {}};
    const auto stop_at = [] (full_position pos) {
        return frontier_and_skips{query::read_frontier{std::move(pos)}, {}};
    };
    // Each partition stops after its first row.
    const auto first_rows = frontier_and_skips{query::read_frontier::end(), {after_row(*s, p, 1), after_row(*s, q, 1)}};

    std::vector<producer_case> cases;
    cases.push_back({"exhausted", make_command(*s, full_slice, query::max_rows), full_range, all_rows, query::short_read::no,
            std::nullopt, row_position(*s, q, 2), end});
    cases.push_back({"row limit", make_command(*s, full_slice, 2), full_range, rows_of_p, query::short_read::no,
            row_position(*s, p, 2), row_position(*s, p, 2), stop_at(after_row(*s, p, 2))});
    // The page consumes the dead row 3 before it reaches the end of p, so
    // both cursors are at row 3. The page stops at the end of p.
    cases.push_back({"partition limit", make_command(*s, full_slice, query::max_rows, 1), full_range, rows_of_p, query::short_read::no,
            row_position(*s, p, 3), row_position(*s, p, 3), stop_at(end_of_partition(*s, p))});
    cases.push_back({"short read", short_cmd, full_range, {{p, 1, 10 * p + 1}}, query::short_read::yes,
            row_position(*s, p, 1), row_position(*s, p, 1), stop_at(after_row(*s, p, 1))});
    // The per-partition limit stops each partition early, but the page is
    // exhausted.
    cases.push_back({"per-partition limit", make_command(*s, partition_slice_builder(*s).with_partition_row_limit(1).build(), query::max_rows),
            full_range, {{p, 1, 10 * p + 1}, {q, 1, 10 * q + 1}}, query::short_read::no, std::nullopt, row_position(*s, q, 1), first_rows});
    // DISTINCT limits each partition to one row, like a per-partition
    // limit of one.
    cases.push_back({"distinct", make_command(*s, partition_slice_builder(*s).with_option<query::partition_slice::option::distinct>().build(),
            query::max_rows), full_range, {{p, 1, 10 * p + 1}, {q, 1, 10 * q + 1}}, query::short_read::no, std::nullopt,
            row_position(*s, q, 1), first_rows});
    // A per-partition limit in the slice of a DISTINCT query applies instead
    // of the limit of one.
    cases.push_back({"distinct with a per-partition limit", make_command(*s, partition_slice_builder(*s)
            .with_option<query::partition_slice::option::distinct>().with_partition_row_limit(2).build(), query::max_rows), full_range,
            all_rows, query::short_read::no, std::nullopt, row_position(*s, q, 2),
            frontier_and_skips{query::read_frontier::end(), {after_row(*s, p, 2), after_row(*s, q, 2)}}});
    // The page driver reads the second range with a new querier. That
    // querier consumes nothing, so it has no position.
    cases.push_back({"empty last range", make_command(*s, full_slice, query::max_rows), split_ranges, all_rows, query::short_read::no,
            std::nullopt, std::nullopt, end});

    test_env local;
    for (const auto& c : cases) {
      for (const bool read_frontier : {false, true}) {
        BOOST_TEST_CONTEXT(c.name << (read_frontier ? ", with a frontier" : "")) {
            const auto cmd = read_frontier ? with_read_frontier(c.cmd) : c.cmd;
            auto multishard = std::get<0>(replica::query_data_on_all_shards(env.db(), s, cmd, c.ranges, query::result_options::only_result(),
                    nullptr, db::no_timeout).get());
            auto driver = local.query_data(s, contents, cmd, data_only, c.ranges, *cmd.max_result_size, nullptr);
            BOOST_REQUIRE_EQUAL(keyed_rows_of(s, cmd, *multishard), c.rows);
            BOOST_REQUIRE_EQUAL(keyed_rows_of(s, cmd, *driver), c.rows);
            BOOST_REQUIRE(multishard->is_short_read() == c.short_read);
            BOOST_REQUIRE(driver->is_short_read() == c.short_read);
            if (read_frontier) {
                require_frontier(*s, multishard->frontier(), c.frontier.frontier);
                require_frontier(*s, driver->frontier(), c.frontier.frontier);
            } else {
                BOOST_REQUIRE(!multishard->frontier());
                BOOST_REQUIRE(!driver->frontier());
                if (!tablets) {
                    require_same_position(*s, multishard->last_position(), c.multishard_cursor);
                }
                require_same_position(*s, driver->last_position(), c.driver_cursor);
            }

            // The page driver reads mutation pages of a single range.
            if (c.ranges.size() == 1) {
                auto multishard_mutations = std::get<0>(replica::query_mutations_on_all_shards(env.db(), s, cmd, c.ranges, nullptr,
                        db::no_timeout).get());
                auto driver_mutations = local.query_mutations(s, contents, cmd, c.ranges.front(), *cmd.max_result_size, nullptr);
                BOOST_REQUIRE_EQUAL(multishard_mutations->row_count(), driver_mutations.row_count());
                BOOST_REQUIRE(multishard_mutations->is_short_read() == driver_mutations.is_short_read());
                BOOST_REQUIRE_EQUAL(multishard_mutations->partitions().size(), driver_mutations.partitions().size());
                if (read_frontier) {
                    require_frontier(*s, multishard_mutations->frontier(), c.frontier.frontier);
                    require_frontier(*s, driver_mutations.frontier(), c.frontier.frontier);
                    require_skips(*s, *multishard_mutations, c.frontier.skips);
                    require_skips(*s, driver_mutations, c.frontier.skips);
                } else {
                    BOOST_REQUIRE(!multishard_mutations->frontier());
                    BOOST_REQUIRE(!driver_mutations.frontier());
                    require_skips(*s, *multishard_mutations, {});
                    require_skips(*s, driver_mutations, {});
                }
                for (auto&& [m, d] : std::views::zip(multishard_mutations->partitions(), driver_mutations.partitions())) {
                    BOOST_REQUIRE_EQUAL(m.row_count(), d.row_count());
                    assert_that(m.mut().unfreeze(s)).is_equal_to(d.mut().unfreeze(s));
                }
            }
        }
      }
    }

}

} // anonymous namespace

// The wire carries a frontier's stop in the place of the last position, so
// a received reply holds a last position until the receiver, which asked for
// a frontier, reinterprets it. The skips of a mutation reply travel with
// it.
SEASTAR_THREAD_TEST_CASE(test_frontier_on_the_wire) {
    test_env env;
    auto s = env.table_schema;
    auto pks = in_ring_order(*s, {1, 2});
    auto contents = make_contents({make_row(s, pks[0], 1, 1, 1), make_row(s, pks[0], 2, 2, 1),
            make_row(s, pks[1], 1, 3, 1), make_row(s, pks[1], 2, 4, 1)});
    // The per-partition limit leaves the first partition after its first
    // row, and the row limit stops the page after the first row of the
    // second partition.
    const auto cmd = with_read_frontier(make_command(*s, partition_slice_builder(*s).with_partition_row_limit(1).build(), 2));
    const auto stop = query::read_frontier{after_row(*s, pks[1], 1)};
    const std::vector<full_position> skips{after_row(*s, pks[0], 1)};

    auto data = env.query_data(s, contents, cmd, data_only, {query::full_partition_range}, unlimited_size, nullptr);
    require_frontier(*s, data->frontier(), stop);
    auto received_data = ser::deserialize_from_buffer(ser::serialize_to_buffer<bytes>(*data), std::type_identity<query::result>());
    BOOST_REQUIRE(!received_data.frontier());
    require_position(*s, received_data.last_position(), *stop.stop);
    received_data.reinterpret_position_as_frontier();
    require_frontier(*s, received_data.frontier(), stop);
    BOOST_REQUIRE(received_data.buf() == data->buf());

    auto mutations = env.query_mutations(s, contents, cmd, query::full_partition_range, unlimited_size, nullptr);
    require_frontier(*s, mutations.frontier(), stop);
    require_skips(*s, mutations, skips);
    auto received_mutations = ser::deserialize_from_buffer(ser::serialize_to_buffer<bytes>(mutations), std::type_identity<reconcilable_result>());
    BOOST_REQUIRE(!received_mutations.frontier());
    require_skips(*s, received_mutations, skips);
    received_mutations.reinterpret_position_as_frontier();
    require_frontier(*s, received_mutations.frontier(), stop);

    // A frontier at the end of the range travels as no position.
    const auto exhausting_cmd = with_read_frontier(make_command(*s, partition_slice_builder(*s).build(), query::max_rows));
    auto exhausted = env.query_mutations(s, contents, exhausting_cmd, query::full_partition_range, unlimited_size, nullptr);
    require_frontier(*s, exhausted.frontier(), query::read_frontier::end());
    auto received_exhausted = ser::deserialize_from_buffer(ser::serialize_to_buffer<bytes>(exhausted), std::type_identity<reconcilable_result>());
    BOOST_REQUIRE(!received_exhausted.frontier());
    received_exhausted.reinterpret_position_as_frontier();
    require_frontier(*s, received_exhausted.frontier(), query::read_frontier::end());
}

// Compares the pages of the replica page driver with those of the multishard
// producer on a few cases.
//
// storage_proxy reads a range scan on the local node with
// query_data_on_all_shards() when the command has the range_scan_data_variant
// option, and with query_mutations_on_all_shards() otherwise. For tables with
// vnodes, both functions read all shards with one multishard reader and set
// the cursor themselves. This is the multishard producer. For tables with
// tablets, they read each tablet through replica::database, which uses the
// page driver.
//
// The data pages of both producers have the same rows and short-read flag.
// Their cursors differ when the page is exhausted. The multishard producer
// sets a cursor only when the page reaches a limit or stops short. The page
// driver sets one whenever the querier which read last has a position. The
// mutation pages of both producers are the same. When the command asks for a
// frontier, all pages of both producers hold the same frontier instead of a
// cursor, and the mutation pages have the same skips.
SEASTAR_THREAD_TEST_CASE(test_page_driver_and_multishard_producer) {
    do_with_cql_env_thread([] (cql_test_env& env) {
        compare_producers(env, false);
    }, config_with_tombstone_gc_extension()).get();
}

// Like test_page_driver_and_multishard_producer, for a table with tablets.
// storage_proxy's local read then reads each tablet through the page driver
// and merges the pages. The merged pages have the rows, the short-read flag
// and the frontier of the page driver's page of the whole range.
SEASTAR_THREAD_TEST_CASE(test_page_driver_and_tablet_producer) {
    do_with_cql_env_thread([] (cql_test_env& env) {
        compare_producers(env, true);
    }, config_with_tombstone_gc_extension()).get();
}

BOOST_AUTO_TEST_SUITE_END()

