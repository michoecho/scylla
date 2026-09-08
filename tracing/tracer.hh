/*
 * Scylla's own tracepoints, over the binary tracepoints in modules/tracer of
 * the cpp_template playground.
 *
 * The machinery is Seastar's: task ids, the per-thread rings, the snapshot and
 * the switch all live in <seastar/core/tracer.hh>, which this header includes
 * and every user of this one therefore gets. What is here is only what Seastar
 * cannot see -- a CQL frame, the reader concurrency semaphore, the prepared
 * statement cache -- and Seastar does not include this header, or anything else
 * of Scylla's.
 *
 * The call sites are out of line in tracing/tracer.cc, for the same reason
 * Seastar keeps its own out of line: this header is included widely enough that
 * a change to it is an expensive rebuild, while a change confined to the .cc is
 * a few steps and a relink.
 *
 * They are compiled into Scylla rather than into libseastar.so. Each loaded
 * object registers a tracepoint table of its own -- see the header comment on
 * <seastar/core/tracer.hh> -- so the events below sit in the executable's table
 * beside Seastar's in the library's, and one set_tracepoints_enabled() switches
 * both.
 */

#pragma once

#include <seastar/core/tracer.hh>

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

namespace seastar {

/// A CQL frame arrived and opened a new task chain: `task` is the id the shard
/// runs under from here on, and every continuation, work item and I/O
/// descriptor created while it is current inherits it. That is what lets a
/// viewer recover one request out of the trace.
void trace_cql_request(uint32_t task) noexcept;

/// The reader concurrency semaphore's loop ran a queued read, as the task that
/// asked for it.
void trace_semaphore_execute(uint32_t task) noexcept;

// Prepared-statement records deliberately carry no task id. The viewer derives
// the running task from the shard timeline, while the statement id and its
// metadata remain independent of task propagation.
void trace_prepared_statement_added(std::string_view keyspace, std::string_view statement,
        std::span<const std::byte> id) noexcept;
void trace_prepared_statement_removed(std::string_view keyspace, std::string_view statement,
        std::span<const std::byte> id) noexcept;
void trace_prepared_query_run(std::span<const std::byte> id) noexcept;
void trace_prepared_statements_snapshot_begin() noexcept;
void trace_prepared_statement_snapshot_entry(std::string_view keyspace, std::string_view statement,
        std::span<const std::byte> id) noexcept;
void trace_prepared_statements_snapshot_end() noexcept;

}
