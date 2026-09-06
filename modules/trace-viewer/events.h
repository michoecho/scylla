// The events this viewer consumes, as the viewer wants them.
//
// This file is the *consumer* half of the trace contract, and it is the half
// that belongs to this program. The producer half is not a file at all: it is
// the tracepoint table in each object a snapshot came from, read out of the ELF
// by tracepoint_table.h. There is one such table per object of per build of the
// traced program -- a cluster mid-upgrade has several at once, and two of them
// may lay the same tracepoint out differently, or spell a field with a
// different width, or not have it at all.
//
// So nothing here is read off the wire. `decoder_plugin.cc` matches this file
// against those tables, by name and by field name, and generates the conversion
// between them; a field this file wants and a build has not got is reported and
// left at its default, rather than quietly becoming whatever byte followed it.
// That is the whole point of writing the events down twice.
//
// Rules for what may appear here, because a generated reader has to be able to
// fill it in from what a record holds:
//
//   * an event is a struct nested in `events`, named exactly as the tracepoint
//     is named in the table -- and named again in VIEWER_EVENT_LIST at the
//     bottom, which is the list the generator and the viewer's exported
//     callbacks are both built from. A struct that is not in that list is one
//     no trace will ever fill in;
//   * a field is named exactly as the tracepoint's parameter is, and is an
//     integer, a `std::string_view`, a `std::span<const std::byte>`, or a
//     struct whose members are filled in the same way by name (`source_location`
//     below is the one of those);
//   * a field is filled from a parameter of the same type, or from a narrower
//     integer of the same signedness. Nothing else converts, and anything that
//     does not is reported by name at startup.
//
// Everything a view or a span points at lives in the trace buffer, or in the
// decoder's own event, and is valid only for the duration of the `on_decode_*`
// call it arrives in. Copy what you keep.

#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

namespace viewer {

// Where a record was taken, once the decoder has read it back out of the object
// the address pointed into. Unresolved -- no object in `dsos/`, or a record
// from before it was mapped -- leaves `file` and `function` empty and keeps
// `address`, which is all the trace said.
struct source_location {
    std::string_view file;
    std::string_view function;
    std::uint32_t line = 0;
    std::uint64_t address = 0;
    bool resolved = false;
};

// Which tracepoint a record came from and when it was taken: what its entry in
// the object's table said, under the names this side uses for them. Everything
// but `timestamp` is a fact about the call site, fixed when the object was
// compiled.
//
// `has_timestamp` is false for a record written by a tracepoint that carries no
// time of its own; `timestamp` is then the moment of the record before it in
// the same buffer. See interpolate_untimed() in viewer.cc.
struct event_meta {
    std::string_view name;
    std::string_view file;
    std::int32_t line = 0;
    std::string_view function;
    bool has_timestamp = true;
    std::uint64_t timestamp = 0;
};

// The tracepoints, one struct each. A build whose tables have none of these is
// not one this viewer can read; a build whose tables have more of them is
// perfectly readable, and the ones not named here are dropped.
struct events {
    // --- the five ways a reactor picks up a task ---------------------------
    struct run_task {
        std::uint64_t prev = 0;
        std::uint64_t task = 0;
        source_location at;
    };
    struct cql_request {
        std::uint64_t prev = 0;
        std::uint64_t task = 0;
    };
    struct semaphore_execute {
        std::uint64_t prev = 0;
        std::uint64_t task = 0;
    };
    struct execution_stage {
        std::uint64_t prev = 0;
        std::uint64_t task = 0;
    };
    // Also the far end of a message, which is what joins two nodes; the viewer
    // makes two rows of it. See decode_sink.
    struct rpc_request_handled {
        std::uint64_t connection = 0;
        std::uint64_t sequence = 0;
        std::uint64_t prev = 0;
        std::uint64_t task = 0;
    };

    // --- the reactor's turn on the cpu -------------------------------------
    struct task_queue_run_begin {
        std::uint32_t scheduling_group = 0;
    };
    struct task_queue_run_end {};

    // --- i/o ---------------------------------------------------------------
    struct io_begin {
        std::uint64_t task = 0;
        std::uint64_t io = 0;
    };
    struct io_end {
        std::uint64_t task = 0;
        std::uint64_t io = 0;
    };

    // --- the prepared statement cache --------------------------------------
    struct prepared_query_run {
        std::span<const std::byte> id;
    };
    struct prepared_statement_added {
        std::string_view keyspace;
        std::string_view statement;
        std::span<const std::byte> id;
    };
    struct prepared_statement_removed {
        std::string_view keyspace;
        std::string_view statement;
        std::span<const std::byte> id;
    };
    struct prepared_statement_snapshot_entry {
        std::string_view keyspace;
        std::string_view statement;
        std::span<const std::byte> id;
    };

    // --- connections -------------------------------------------------------
    struct rpc_connection_open {
        std::uint64_t connection = 0;
        std::string_view local;
        std::string_view remote;
        std::uint64_t peer_boot_msb = 0;
        std::uint64_t peer_boot_lsb = 0;
        std::uint32_t peer_shard = 0;
    };
    struct rpc_connection_close {
        std::uint64_t connection = 0;
        std::uint64_t peer_boot_msb = 0;
        std::uint64_t peer_boot_lsb = 0;
        std::uint32_t peer_shard = 0;
    };
    struct rpc_connection_snapshot_entry {
        std::uint64_t connection = 0;
        std::string_view local;
        std::string_view remote;
        std::uint64_t peer_boot_msb = 0;
        std::uint64_t peer_boot_lsb = 0;
        std::uint32_t peer_shard = 0;
    };

    // --- messages ----------------------------------------------------------
    struct rpc_message_sent {
        std::uint64_t connection = 0;
        std::uint64_t sequence = 0;
        std::uint64_t task = 0;
    };
    struct rpc_message_received {
        std::uint64_t connection = 0;
        std::uint64_t sequence = 0;
    };
    struct rpc_reply_sent {
        std::uint64_t connection = 0;
        std::uint64_t sequence = 0;
        std::int64_t msg_id = 0;
        std::uint64_t task = 0;
    };
    struct rpc_reply_received {
        std::uint64_t connection = 0;
        std::uint64_t sequence = 0;
        std::int64_t msg_id = 0;
    };

    // --- not an event of the program's own ---------------------------------
    // How pass_retime dates everything else.
    // The tick count this sync was taken at is the record's own timestamp, and
    // is read from the metadata rather than from the event -- so a decoder that
    // also carries it as a field is not asked for it here.
    struct clock_sync {
        std::uint64_t realtime_ns = 0;
        std::uint64_t ticks_per_second = 0;
    };
};

}  // namespace viewer

// Every event above, once, as a list something else can be written from.
//
// Two things are: the `on_decode_*` symbols the viewer exports, in viewer.cc,
// and the set of tracepoint names decoder_plugin.cc will bridge. Neither can be
// derived from the structs above -- C++ has no way to ask a namespace what it
// contains -- so this is the one place the list is written down, and both read
// it from here.
#define VIEWER_EVENT_LIST(X)             \
    X(run_task)                          \
    X(cql_request)                       \
    X(semaphore_execute)                 \
    X(execution_stage)                   \
    X(rpc_request_handled)               \
    X(task_queue_run_begin)              \
    X(task_queue_run_end)                \
    X(io_begin)                          \
    X(io_end)                            \
    X(prepared_query_run)                \
    X(prepared_statement_added)          \
    X(prepared_statement_removed)        \
    X(prepared_statement_snapshot_entry) \
    X(rpc_connection_open)               \
    X(rpc_connection_close)              \
    X(rpc_connection_snapshot_entry)     \
    X(rpc_message_sent)                  \
    X(rpc_message_received)              \
    X(rpc_reply_sent)                    \
    X(rpc_reply_received)                \
    X(clock_sync)
