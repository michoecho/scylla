/*
 * The translation unit holding Scylla's tracepoint call sites.
 *
 * See tracing/tracer.hh for why they are here rather than inlined at the sites
 * that call them, and why they are compiled into Scylla rather than into
 * libseastar.so, where Seastar's own live (src/core/tracer.cc there).
 *
 * The events:
 *
 *   cql_request{task}            a CQL frame arrived and opened a new chain
 *   semaphore_execute{task}      the reader concurrency semaphore's loop ran
 *                                a queued read, as the task that asked for it
 *   prepared_statement_added     a prepared statement entered the shard cache
 *   prepared_statement_removed   a prepared statement left the shard cache
 *   prepared_query_run           a prepared statement was executed
 *   prepared_statements_snapshot_* a full cache snapshot, emitted at dump time
 *
 * The first two are *switches*: `task` is the id the shard is running from here
 * on. A trace viewer recovers one request by taking every record whose task is
 * the id a cql_request opened, which works because a task id is inherited by
 * every continuation, work item and I/O descriptor created while it is current
 * -- see task_id in <seastar/core/tracer.hh>.
 */

#include <tracing/tracer.hh>

#include <seastar/core/tracer_control.hh>

#include "tracer/tracer.h"

namespace seastar {

void trace_cql_request(uint32_t task) noexcept {
    ensure_thread_tracer();
    TRACEPOINT(tracer::event_level::info, "cql_request", "task", task);
}

void trace_semaphore_execute(uint32_t task) noexcept {
    ensure_thread_tracer();
    TRACEPOINT(tracer::event_level::debug, "semaphore_execute", "task", task);
}

void trace_prepared_statement_added(std::string_view keyspace, std::string_view statement,
        std::span<const std::byte> id) noexcept {
    ensure_thread_tracer();
    TRACEPOINT(tracer::event_level::info, "prepared_statement_added",
            "keyspace", keyspace, "statement", statement, "id", id);
}

void trace_prepared_statement_removed(std::string_view keyspace, std::string_view statement,
        std::span<const std::byte> id) noexcept {
    ensure_thread_tracer();
    TRACEPOINT(tracer::event_level::info, "prepared_statement_removed",
            "keyspace", keyspace, "statement", statement, "id", id);
}

void trace_prepared_query_run(std::span<const std::byte> id) noexcept {
    ensure_thread_tracer();
    TRACEPOINT(tracer::event_level::info, "prepared_query_run", "id", id);
}

void trace_prepared_statements_snapshot_begin() noexcept {
    ensure_thread_tracer();
    TRACEPOINT(tracer::event_level::info, "prepared_statements_snapshot_begin");
}

void trace_prepared_statement_snapshot_entry(std::string_view keyspace, std::string_view statement,
        std::span<const std::byte> id) noexcept {
    ensure_thread_tracer();
    TRACEPOINT(tracer::event_level::info, "prepared_statement_snapshot_entry",
            "keyspace", keyspace, "statement", statement, "id", id);
}

void trace_prepared_statements_snapshot_end() noexcept {
    ensure_thread_tracer();
    TRACEPOINT(tracer::event_level::info, "prepared_statements_snapshot_end");
}

}
