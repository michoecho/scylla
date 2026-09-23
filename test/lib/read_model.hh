/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include <fmt/core.h>

#include "mutation/mutation.hh"
#include "utils/chunked_vector.hh"

// A small model of the writes to a table, and of the complete answer of a
// query over them.
//
// Tests of paging and of reconciliation compare the pages of the production
// read path with this answer. Reconciliation is how the coordinator merges the
// replies of replicas which disagree. The model computes the answer independently: it does
// not use mutations, the compactor, the result builders, or the coordinator's
// page resolution. It uses the schema only to order partitions by token.
//
// The model has one table:
//
//     CREATE TABLE cf (pk int, ck int, s int static, v1 int, v2 int, PRIMARY KEY (pk, ck))
//
// Its rules:
// - All writes have distinct timestamps. The newest write of a cell or of a
//   row marker wins. A tombstone covers the writes whose timestamp is not
//   greater than its own.
// - A value is live when it was written, its TTL did not expire, and no
//   tombstone covers it. Otherwise it is dead.
// - Tombstones are never purged. Tables which the model describes must
//   disable tombstone GC.
// - A clustering row exists when its row marker or one of its regular cells
//   is live. The selected columns do not matter.
// - The static row exists when its static cell is live.
// - Without a clustering restriction, a partition which has a live static row
//   but no live clustering row gives one static-only row. Its clustering key
//   and regular columns are null. Filtering does not create such a row: a
//   partition whose live rows the filter rejects gives no row.
// - A DISTINCT query gives one row for each partition which has a live static
//   row or a live clustering row.
// - A query of listed partitions (pk IN ...) returns them in the order of
//   their keys, as CQL sorts them. A query of the whole ring returns
//   partitions in ring order, which is the order of their tokens. The rows
//   of a partition come in clustering order, reversed for a reversed query.
// - The filter, the per-partition limit and the limit apply in this order.
//   Both limits count the rows which the filter accepts.
// - The partition limit counts the partitions which give at least one row.

namespace tests::read_model {

// The CQL statement which creates the model's table `ks.cf`, without
// tombstone GC. The tombstone_gc extension must be registered.
std::string create_table_statement(std::string_view ks, std::string_view cf);

// The schema of the model's table, for tests without a database.
schema_ptr make_schema(std::string_view ks = "ks", std::string_view cf = "cf");

// Returns `pks` sorted in ring order.
std::vector<int32_t> ring_order(const schema& s, std::vector<int32_t> pks);

// The lifetime of a written value, relative to the query time.
enum class lifetime {
    // Written without a TTL.
    permanent,
    // Written with a TTL which expired before the query time.
    expired,
    // Written with a TTL which expires after the query time.
    expiring,
};

enum class regular_column { v1, v2 };

// A write of the static cell `s`. A value of nullopt writes a cell tombstone.
struct static_cell_write {
    int32_t pk;
    std::optional<int32_t> value;
    api::timestamp_type ts;
    lifetime life = lifetime::permanent;
};

// A write of a regular cell of row `ck`. A value of nullopt writes a cell
// tombstone.
struct regular_cell_write {
    int32_t pk;
    int32_t ck;
    regular_column column;
    std::optional<int32_t> value;
    api::timestamp_type ts;
    lifetime life = lifetime::permanent;
};

// A write of the row marker of row `ck`, like an INSERT.
struct row_marker_write {
    int32_t pk;
    int32_t ck;
    api::timestamp_type ts;
    lifetime life = lifetime::permanent;
};

struct row_deletion {
    int32_t pk;
    int32_t ck;
    api::timestamp_type ts;
};

// A bound of a clustering range.
struct bound {
    int32_t ck;
    bool inclusive;
};

// A deletion of the clustering rows between `start` and `end`. A bound of
// nullopt is unbounded.
struct range_deletion {
    int32_t pk;
    std::optional<bound> start;
    std::optional<bound> end;
    api::timestamp_type ts;
};

struct partition_deletion {
    int32_t pk;
    api::timestamp_type ts;
};

using operation = std::variant<static_cell_write, regular_cell_write, row_marker_write, row_deletion, range_deletion, partition_deletion>;

// The writes to the table. Their order does not matter.
using history = std::vector<operation>;

// Throws std::invalid_argument if `h` is not a valid history. Every write
// needs a timestamp, the timestamps must be distinct, cell tombstones cannot
// have a TTL, and range deletions cannot be empty.
void validate(const history& h);

// The mutations of `h`, one for each partition, in ring order. Values with a
// TTL expire relative to `query_time`. Throws if validate(h) throws.
utils::chunked_vector<mutation> to_mutations(schema_ptr s, const history& h, gc_clock::time_point query_time);

// `op` and `h` as C++ code, which a test can use to replay them.
std::string describe(const operation& op);
std::string describe(const history& h);

// A random history of 1 to `max_writes` writes to partitions 1 to 4 and rows
// 1 to 5, from tests::random. The bounds of range deletions are from 0 to 6.
// The kinds, keys and values of the writes are independent.
history random_history(size_t max_writes = 12);

// A non-key column which a query can filter on.
enum class column { s, v1, v2 };

enum class comparison { eq, lt, gt };

// The restriction `col op value` of a query with filtering. A null value
// never satisfies it.
struct predicate {
    column col;
    comparison op;
    int32_t value;
};

struct select_query {
    // The partitions to read: pk IN (...). nullopt reads the whole ring.
    std::optional<std::vector<int32_t>> partitions;
    // The restriction of the clustering key. Without either bound, the query
    // has no clustering restriction.
    std::optional<bound> ck_start;
    std::optional<bound> ck_end;
    // Reverses the clustering order. The order of partitions does not change.
    bool reversed = false;
    // SELECT DISTINCT: at most one row for each partition.
    bool distinct = false;
    // The selected non-key columns. The partition key and, except for
    // DISTINCT, the clustering key are always selected.
    bool select_s = true;
    bool select_v1 = true;
    bool select_v2 = true;
    // A conjunction of restrictions, which requires filtering.
    std::vector<predicate> filter;
    std::optional<uint64_t> limit;
    std::optional<uint64_t> per_partition_limit;
    // The partition limit of a read command. CQL has no equivalent.
    std::optional<uint64_t> partition_limit;
};

// Throws std::invalid_argument if the model does not support `q`.
//
// A DISTINCT query can only select the static column and have a limit. A
// query with a partition limit cannot filter, because a read command counts
// partitions before filtering. A list of partitions cannot be empty, and its
// keys must be distinct. The clustering range cannot be empty, and the limits
// must be positive.
void validate(const select_query& q);

// The CQL statement of `q` on table `ks.cf`, without paging options.
//
// Throws std::invalid_argument if validate(q) throws, or if CQL cannot
// express `q`. CQL cannot express a partition limit. It also cannot express a
// reversed query of more than one partition, because CQL orders the rows of
// several partitions by their clustering key.
std::string to_cql(const select_query& q, std::string_view ks, std::string_view cf);

// A random query which CQL can express, on partitions 1 to 5, from
// tests::random.
select_query random_query();

// A row of a query's answer.
struct answer_row {
    int32_t pk;
    // nullopt for a static-only row and for a row of a DISTINCT query.
    std::optional<int32_t> ck;
    // The values of the selected columns. nullopt for a null value and for a
    // column which the query does not select.
    std::optional<int32_t> s;
    std::optional<int32_t> v1;
    std::optional<int32_t> v2;

    bool operator==(const answer_row&) const = default;
};

// The complete answer of `q` over `h`, in the order which the rules above
// describe. `s` must be a schema of the model's table. Throws if validate(h)
// or validate(q) throws.
std::vector<answer_row> evaluate(const schema& s, const history& h, const select_query& q);

} // namespace tests::read_model

template <>
struct fmt::formatter<tests::read_model::answer_row> : fmt::formatter<string_view> {
    auto format(const tests::read_model::answer_row& r, fmt::format_context& ctx) const -> decltype(ctx.out());
};

// Formats a query as C++ code, which a test can use to replay it.
template <>
struct fmt::formatter<tests::read_model::select_query> : fmt::formatter<string_view> {
    auto format(const tests::read_model::select_query& q, fmt::format_context& ctx) const -> decltype(ctx.out());
};
