#include "converter.h"

#include <algorithm>
#include <cstddef>
#include <span>
#include <vector>

#include <doctest/doctest.h>
#include <protos/perfetto/trace/trace.pb.h>

TEST_CASE("native Perfetto trace has one track per shard") {
  const std::vector<scylla_trace::event> events = {
      {scylla_trace::event_kind::task_switch,
       100,
       0,
       0x1234,
       0x1234,
       0,
       "scylla_tracer.cc",
       86,
       "seastar::trace_run_task",
       {}},
      {scylla_trace::event_kind::io_begin, 200, 0, 0x1234, 7, 0, "", 0, "", {}},
      {scylla_trace::event_kind::io_end, 400, 0, 0x1234, 7, 0, "", 0, "", {}},
      {scylla_trace::event_kind::task_switch,
       500,
       0,
       0,
       0,
       0x1234,
       "",
       0,
       "",
       {}},
  };

  const std::vector<std::byte> bytes = scylla_trace::serialize(events);
  perfetto::protos::Trace trace;
  REQUIRE(trace.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())));

  bool found_shard = false;
  bool has_only_shard_descriptor = true;
  bool all_events_are_on_shard = true;
  bool has_no_flow_or_correlation = true;
  bool found_task_base = false;
  bool found_run_task_child = false;
  bool run_task_is_a_slice = true;
  bool run_task_has_function = true;
  bool all_events_have_sequence_id = true;
  for (const auto &packet : trace.packet()) {
    if (packet.has_track_descriptor()) {
      found_shard |= packet.track_descriptor().name() == "Shard 0";
      has_only_shard_descriptor &=
          packet.track_descriptor().name().starts_with("Shard ");
    }
    if (packet.has_track_event()) {
      all_events_are_on_shard &= packet.track_event().track_uuid() == 0x1000;
      has_no_flow_or_correlation &= packet.track_event().flow_ids_size() == 0;
      has_no_flow_or_correlation &= packet.track_event().correlation_id() == 0;
      if (packet.track_event().name() == "task 0x1234") {
        found_task_base = true;
      }
      if (packet.track_event().name() == "scylla_tracer.cc:86") {
        found_run_task_child = true;
        run_task_is_a_slice &= packet.track_event().type() ==
                               perfetto::protos::TrackEvent::TYPE_SLICE_BEGIN;
        run_task_has_function &= packet.track_event().has_source_location();
        run_task_has_function &=
            packet.track_event().source_location().function_name() ==
            "seastar::trace_run_task";
      }
      all_events_have_sequence_id &= packet.has_trusted_packet_sequence_id();
      all_events_have_sequence_id &= packet.trusted_packet_sequence_id() == 1;
    }
  }
  CHECK(found_shard);
  CHECK(has_only_shard_descriptor);
  CHECK(all_events_are_on_shard);
  CHECK(has_no_flow_or_correlation);
  CHECK(found_task_base);
  CHECK(found_run_task_child);
  CHECK(run_task_is_a_slice);
  CHECK(all_events_have_sequence_id);
}

TEST_CASE("empty input is not emitted as a malformed Perfetto trace") {
  const std::vector<scylla_trace::event> empty;
  CHECK(scylla_trace::serialize(empty).empty());
}
