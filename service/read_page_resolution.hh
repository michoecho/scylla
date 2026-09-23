/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <optional>
#include <unordered_map>
#include <variant>
#include <vector>

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sharded.hh>

#include "exceptions/coordinator_result.hh"
#include "keys/full_position.hh"
#include "locator/host_id.hh"
#include "mutation_query.hh"
#include "query/query-result.hh"
#include "utils/small_vector.hh"

// Coordinator decisions which turn replica replies into a page for the client.
//
// A read has up to two kinds of rounds:
// - The first round sends a data request to one replica, or sometimes two,
//   and digest requests to the others. A digest is a hash of the page which
//   the replica would return. If the digests match, the data reply becomes
//   the page.
// - If the digests differ, reconciliation rounds follow. Each replica returns
//   a mutation page: its data together with its tombstones. The coordinator
//   merges the mutation pages and computes the page itself. A round can also
//   decide that the replies are not enough, and ask for another round.
//
// This code does not send requests, choose replicas, or handle errors and
// timeouts. storage_proxy does that and passes the successful replies here.
// Tests can therefore drive these decisions with chosen replies.

namespace service {

// The state of the first round of a read when enough replies arrived to
// satisfy the consistency level.
struct digest_read_result {
    // The first data reply.
    foreign_ptr<lw_shared_ptr<query::result>> result;
    // Whether the digests of the replies received until then match.
    bool digests_match;
};

// Collects the successful data and digest replies of the first round of a
// read, and reports when they reach the consistency level.
//
// storage_proxy decides which replicas count toward the consistency level. It
// also handles errors and timeouts, and calls fail() when the request fails.
class foreground_reply_collector {
    struct digest_and_last_pos {
        query::result_digest digest;
        std::optional<full_position> last_pos;
        std::optional<query::read_frontier> frontier;

        digest_and_last_pos(query::result_digest digest, std::optional<full_position> last_pos, std::optional<query::read_frontier> frontier)
            : digest(std::move(digest)), last_pos(std::move(last_pos)), frontier(std::move(frontier))
        { }
    };

    schema_ptr _schema;
    size_t _block_for;
    size_t _targets_count = 0;
    size_t _cl_responses = 0;
    promise<exceptions::coordinator_result<digest_read_result>> _cl_promise;
    bool _cl_reported = false;
    // The number of replies when the consistency level was reached.
    size_t _replies_at_cl = 0;
    foreign_ptr<lw_shared_ptr<query::result>> _data_result;
    utils::small_vector<digest_and_last_pos, 3> _digest_results;
    api::timestamp_type _last_modified = api::missing_timestamp;

    void got_response(bool counts_for_cl);
public:
    // The consistency level requires `block_for` replies which count toward
    // it, including at least one data reply.
    foreground_reply_collector(schema_ptr schema, size_t block_for);

    // Adds `count` targets to the number of replies to wait for.
    void add_wait_targets(size_t count);

    // Adds a successful reply. `counts_for_cl` tells whether the replica
    // counts toward the consistency level.
    void add_data(bool counts_for_cl, foreign_ptr<lw_shared_ptr<query::result>> result);
    // `frontier` is the frontier of the digest reply, if the replica sent
    // one. See query::read_frontier.
    void add_digest(bool counts_for_cl, query::result_digest digest, api::timestamp_type last_modified, std::optional<full_position> last_pos,
            std::optional<query::read_frontier> frontier);

    // Drops the collected replies. If the consistency level was not reached,
    // has_cl() resolves with `ex`.
    void fail(exceptions::coordinator_exception_container ex);

    // Resolves when the replies reach the consistency level, or when the
    // request fails before that. The result holds the first data reply.
    //
    // The continuation runs after the reply which reached the consistency
    // level was added. Replies added in between are visible through this
    // object, but not through the result.
    future<exceptions::coordinator_result<digest_read_result>> has_cl();

    // The number of replies, including those after the consistency level.
    size_t response_count() const {
        return _digest_results.size();
    }
    // The number of replies which count toward the consistency level.
    size_t cl_responses() const {
        return _cl_responses;
    }
    size_t block_for() const {
        return _block_for;
    }
    // Whether the collector holds a data reply. The collector hands the first
    // data reply over to has_cl() when the consistency level is reached. It
    // keeps a data reply which arrives after that.
    bool has_data() const {
        return bool(_data_result);
    }
    // Whether all targets replied.
    bool is_completed() const {
        return response_count() == _targets_count;
    }
    api::timestamp_type last_modified() const {
        return _last_modified;
    }

    // Whether the digests of all replies received so far match.
    bool digests_match() const;
    // The earliest last position of all replies received so far. A reply's
    // last position is where its reader was when the page ended; see
    // replica::read_data_page(). A reply without one sorts first.
    const std::optional<full_position>& min_position() const;
    // Whether every reply which has_cl() saw has a frontier. Call only after
    // has_cl() resolved successfully.
    bool all_have_frontiers() const;
    // The earliest frontier stop of the replies which has_cl() saw, or
    // nullopt if each of them reached the end of its range. Call only if
    // all_have_frontiers().
    std::optional<full_position> min_frontier_stop() const;
};

// The data reply may be returned to the client.
struct accepted_digest_page {
    foreign_ptr<lw_shared_ptr<query::result>> result;
};

// The digests do not match; the coordinator must reconcile the replicas'
// mutations.
struct digest_page_mismatch {};

using digest_page_decision = std::variant<accepted_digest_page, digest_page_mismatch>;

// Decides what to do when the first round of a read reaches the consistency
// level.
//
// `cmd` is the command of the read. `cl_result` is the value of
// `replies.has_cl()`. The decision compares the digests which `cl_result`
// saw. `empty_replica_pages` and `read_frontiers` tell whether the cluster
// features of these names are enabled.
//
// When the digests match, the page ends where the earliest reply ended:
// - With `read_frontiers`, if every reply which `cl_result` saw has a
//   frontier, let E be the earliest frontier stop of these replies. Their
//   matching digests mean that the data reply has no row at or after E.
//   Replies which arrived later do not count, because their digests may
//   differ. If no reply stopped, the page is the
//   data reply, without a cursor. If E lies inside a partition, before the end of its
//   clustering rows, and the data reply ends with a static-only row of that
//   partition, the decision is a mismatch: a clustering row after E may
//   cancel the static-only row. Otherwise, if the data reply reached its
//   row or partition limit, the page's cursor is E. Otherwise, if the
//   command allows short reads, the page is short and its cursor is E.
//   Otherwise the page may not end there, and the decision is a mismatch,
//   so that the coordinator reconciles.
//   A cursor before the static row becomes a cursor before the clustering
//   rows.
// - Otherwise, if `empty_replica_pages` is true, the decision lowers the
//   page's last position to replies.min_position(). That covers replies
//   which arrived after the consistency level was reached. If a reply has no
//   last position, the page loses its own.
digest_page_decision decide_digest_page(const schema& s, const query::read_command& cmd, digest_read_result cl_result,
        const foreground_reply_collector& replies, bool empty_replica_pages, bool read_frontiers);

using mutations_per_partition_key_map =
        std::unordered_map<partition_key, std::unordered_map<locator::host_id, std::optional<mutation>>, partition_key::hashing, partition_key::equality>;

// A mutation page returned by one replica in a reconciliation round.
struct mutation_page_reply {
    locator::host_id from;
    foreign_ptr<lw_shared_ptr<reconcilable_result>> result;
};

// A reconciled page which the coordinator may return to the client.
struct accepted_mutation_page {
    // The client-visible page. The original command limits it.
    query::result result;
    // For each partition and replica, the mutation which the replica lacks
    // compared to the merged replies, if any. The mutations use the table
    // schema, also for reversed queries.
    mutations_per_partition_key_map repair_diffs;
};

// The replies are not enough to build the page, and the coordinator must run
// another round.
struct mutation_page_retry {
    // The command for the next round. Its limits may be larger than the
    // limits of the original command.
    lw_shared_ptr<query::read_command> cmd;
};

using mutation_page_resolution = std::variant<accepted_mutation_page, mutation_page_retry>;

// Sets the options of the command which a reconciliation round sends to the
// replicas. `empty_replica_mutation_pages` tells whether the
// empty_replica_mutation_pages cluster feature is enabled. Its option lets a
// replica end a mutation page without a live row.
void prepare_mutation_read(query::read_command& cmd, bool empty_replica_mutation_pages);

// Reconciles the replies of all targets of one reconciliation round.
//
// `cmd` is the command sent to the replicas in this round. `original_cmd` is
// the client's command, which limits the returned page. In the first round,
// both are the same command.
//
// For reversed queries, `schema` is the reversed schema, and the replies are
// in native reversed format.
future<mutation_page_resolution> resolve_mutation_page(schema_ptr schema, const query::read_command& original_cmd,
        const query::read_command& cmd, std::vector<mutation_page_reply> replies);

// Reconciles a page in rounds which follow the frontiers of the replies. Use
// it only if the read_frontiers cluster feature is enabled, so that every
// mutation reply has a frontier. See query::read_frontier.
//
// Each round reads a mutation page from every target. The reconciliation
// merges the replies and finds their common frontier E: the earliest stop of
// the replies, and in each partition the earliest point where a reply left
// the partition at the per-partition row limit. It keeps only the merged
// data before E, which every reply covers, and computes the repair
// mutations from that data. A partition which a reply left at the
// per-partition limit, but which holds fewer live rows than the limit after
// the merge, is incomplete, and E moves back to it.
//
// The reconciliation then converts the data of all rounds to the client's
// page, within the client's limits. A static-only row depends on the whole
// partition, so the conversion leaves out a partition which E cuts before
// the end of its clustering rows, if the partition has no live clustering
// row before E. The pager then continues the partition, see
// service::pager::paging_state::get_partition_row_pending(). The page ends:
// - where the conversion stopped, if the limits stopped it;
// - at the end of the range, if E is at the end;
// - short at E, if the command allows short reads.
// Otherwise another round reads from E, with the client's limits.
//
// Every round moves E forward, because every replica reads at least one
// fragment after the start of its page.
class frontier_reconciliation {
    schema_ptr _schema;
    lw_shared_ptr<const query::read_command> _cmd;
    dht::partition_range _range;
    lw_shared_ptr<query::read_command> _round_cmd;
    dht::partition_range _round_range;
    // The common frontier of the previous round, where this round starts.
    std::optional<full_position> _round_start;
    // The reconciled data of all rounds, in ring order. It uses the query
    // schema.
    utils::chunked_vector<mutation> _reconciled;
    mutations_per_partition_key_map _diffs;
public:
    // Reconciles the page of `range` which the client's command `cmd` asks
    // for. `cmd` must ask for frontiers, see
    // query::partition_slice::option::send_read_frontier. For reversed
    // queries, `schema` is the reversed schema, and the replies are in native
    // reversed format.
    frontier_reconciliation(schema_ptr schema, lw_shared_ptr<const query::read_command> cmd, dht::partition_range range);

    // The command and the range which the next round sends to the replicas.
    // The command's options are set with prepare_mutation_read().
    const lw_shared_ptr<query::read_command>& round_command() const {
        return _round_cmd;
    }
    const dht::partition_range& round_range() const {
        return _round_range;
    }

    // Adds the replies of all targets of a round. Returns the page if the
    // replies of all rounds so far decide it. Otherwise returns nullopt, and
    // round_command() and round_range() describe the next round.
    future<std::optional<accepted_mutation_page>> add_round(std::vector<mutation_page_reply> replies);
};

} // namespace service
