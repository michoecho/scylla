#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <span>
#include <string>
#include <vector>

namespace scylla_trace {

enum class event_kind {
  task_switch,
  query_start,
  semaphore_execute,
  execution_stage,
  io_begin,
  io_end,
  stack_sample,
};

// A decoded Scylla event. timestamp_ns is relative to the beginning of the
// exported trace, which is the clock domain used by the native Perfetto file.
struct event {
  event_kind kind;
  std::uint64_t timestamp_ns = 0;
  std::uint32_t shard = 0;
  std::uint64_t task_id = 0;
  std::uint64_t argument = 0;
  std::uint64_t previous_task = 0;
  std::string location;
  std::uint32_t line = 0;
  std::string function;
  std::vector<std::uint64_t> frames;
};

// Serialize already-decoded events as a native Perfetto Trace protobuf. This
// is also the small seam used by the unit tests; the snapshot reader below is
// responsible only for Scylla's binary input format and clock conversion.
[[nodiscard]] std::vector<std::byte> serialize(std::span<const event> events);

// Read shard-N.trace files from snapshot_dir and write a native Perfetto trace.
// The output is a complete Trace message, suitable for perfetto.dev or the
// trace_processor_shell. Returns the number of decoded Scylla events.
std::size_t convert_snapshot(const std::filesystem::path &snapshot_dir,
                             const std::filesystem::path &output);

} // namespace scylla_trace
