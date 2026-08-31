#include "converter.h"

#include "decoder.h"
#include <protos/perfetto/trace/trace.pb.h>

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <exception>
#include <format>
#include <fstream>
#include <iterator>
#include <limits>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace scylla_trace {
namespace {

using perfetto::protos::Trace;
using perfetto::protos::TracePacket;
using perfetto::protos::TrackDescriptor;
using perfetto::protos::TrackEvent;

struct clock_sync_point {
  std::int64_t ticks;
  std::uint64_t realtime_ns;
  std::uint64_t ticks_per_second;
};

class wall_clock {
public:
  void build(std::vector<clock_sync_point> points) {
    std::sort(points.begin(), points.end(),
              [](const auto &a, const auto &b) { return a.ticks < b.ticks; });
    points.erase(std::unique(points.begin(), points.end(),
                             [](const auto &a, const auto &b) {
                               return a.ticks == b.ticks;
                             }),
                 points.end());
    points_ = std::move(points);
  }

  [[nodiscard]] std::optional<std::uint64_t>
  realtime_ns(std::int64_t ticks) const {
    if (points_.empty()) {
      return std::nullopt;
    }
    const auto after =
        std::lower_bound(points_.begin(), points_.end(), ticks,
                         [](const auto &point, std::int64_t value) {
                           return point.ticks < value;
                         });
    if (after == points_.begin()) {
      return extrapolate(points_.front(), ticks);
    }
    const auto &before = *(after - 1);
    if (after == points_.end()) {
      return extrapolate(before, ticks);
    }
    const __int128 tick_span = __int128(after->ticks) - before.ticks;
    const __int128 ns_span = __int128(after->realtime_ns) - before.realtime_ns;
    const __int128 offset =
        (__int128(ticks - before.ticks) * ns_span) / tick_span;
    const __int128 result = __int128(before.realtime_ns) + offset;
    return result < 0 ? std::nullopt
                      : std::optional<std::uint64_t>(std::uint64_t(result));
  }

  [[nodiscard]] std::optional<double> ns_per_tick() const {
    if (points_.size() >= 2) {
      return double(points_.back().realtime_ns - points_.front().realtime_ns) /
             double(points_.back().ticks - points_.front().ticks);
    }
    if (points_.size() == 1 && points_.front().ticks_per_second != 0) {
      return 1e9 / double(points_.front().ticks_per_second);
    }
    return std::nullopt;
  }

private:
  static std::optional<std::uint64_t> extrapolate(const clock_sync_point &point,
                                                  std::int64_t ticks) {
    if (point.ticks_per_second == 0) {
      return std::nullopt;
    }
    const __int128 offset = (__int128(ticks - point.ticks) * 1'000'000'000) /
                            __int128(point.ticks_per_second);
    const __int128 result = __int128(point.realtime_ns) + offset;
    return result < 0 ? std::nullopt
                      : std::optional<std::uint64_t>(std::uint64_t(result));
  }

  std::vector<clock_sync_point> points_;
};

struct raw_event {
  event_kind kind;
  std::int64_t ticks = 0;
  std::uint64_t realtime_ns = 0;
  std::uint32_t shard = 0;
  std::uint64_t task_id = 0;
  std::uint64_t argument = 0;
  std::uint64_t previous_task = 0;
  std::string location;
  std::uint32_t line = 0;
  std::string function;
  std::vector<std::uint64_t> frames;
};

struct decoder_sink {
  std::vector<raw_event> &events;
  std::vector<clock_sync_point> &syncs;
  std::uint32_t shard;

  void operator()(const trace::run_task &e,
                  const trace::tracepoint_metadata &m) const {
    events.push_back({event_kind::task_switch,
                      std::int64_t(m.timestamp),
                      0,
                      shard,
                      e.task,
                      0,
                      e.prev,
                      e.at.file,
                      e.at.line,
                      e.at.function,
                      {}});
  }
  void operator()(const trace::cql_request &e,
                  const trace::tracepoint_metadata &m) const {
    events.push_back({event_kind::query_start,
                      std::int64_t(m.timestamp),
                      0,
                      shard,
                      e.task,
                      0,
                      e.prev,
                      {},
                      0,
                      {},
                      {}});
  }
  void operator()(const trace::semaphore_execute &e,
                  const trace::tracepoint_metadata &m) const {
    events.push_back({event_kind::semaphore_execute,
                      std::int64_t(m.timestamp),
                      0,
                      shard,
                      e.task,
                      0,
                      e.prev,
                      {},
                      0,
                      {},
                      {}});
  }
  void operator()(const trace::execution_stage &e,
                  const trace::tracepoint_metadata &m) const {
    events.push_back({event_kind::execution_stage,
                      std::int64_t(m.timestamp),
                      0,
                      shard,
                      e.task,
                      0,
                      e.prev,
                      {},
                      0,
                      {},
                      {}});
  }
  void operator()(const trace::io_begin &e,
                  const trace::tracepoint_metadata &m) const {
    events.push_back({event_kind::io_begin,
                      std::int64_t(m.timestamp),
                      0,
                      shard,
                      e.task,
                      e.io,
                      0,
                      {},
                      0,
                      {},
                      {}});
  }
  void operator()(const trace::io_end &e,
                  const trace::tracepoint_metadata &m) const {
    events.push_back({event_kind::io_end,
                      std::int64_t(m.timestamp),
                      0,
                      shard,
                      e.task,
                      e.io,
                      0,
                      {},
                      0,
                      {},
                      {}});
  }
  void operator()(const trace::stacktrace_sample &e,
                  const trace::tracepoint_metadata &m) const {
    const auto *words =
        reinterpret_cast<const std::uint64_t *>(e.frames.data());
    std::vector<std::uint64_t> frames(words, words + e.frames.size() /
                                                         sizeof(std::uint64_t));
    events.push_back({event_kind::stack_sample,
                      std::int64_t(m.timestamp),
                      e.time_ns,
                      shard,
                      0,
                      0,
                      0,
                      {},
                      0,
                      {},
                      std::move(frames)});
  }
  void operator()(const trace::clock_sync &e,
                  const trace::tracepoint_metadata &m) const {
    syncs.push_back(
        {std::int64_t(m.timestamp), e.realtime_ns, e.ticks_per_second});
  }
  template <typename ignored_event>
  void operator()(const ignored_event &,
                  const trace::tracepoint_metadata &) const {}
};

std::optional<std::uint32_t> shard_of(const std::filesystem::path &path) {
  const std::string stem = path.stem().string();
  const std::size_t dash = stem.rfind('-');
  if (dash == std::string::npos) {
    return std::nullopt;
  }
  try {
    return std::uint32_t(std::stoul(stem.substr(dash + 1)));
  } catch (const std::exception &) {
    return std::nullopt;
  }
}

std::vector<char> read_file(const std::filesystem::path &path) {
  std::ifstream input(path, std::ios::binary);
  if (!input) {
    throw std::system_error(errno, std::generic_category(), path.string());
  }
  return {std::istreambuf_iterator<char>(input),
          std::istreambuf_iterator<char>()};
}

struct track_ids {
  std::map<std::uint32_t, std::uint64_t> shard;
};

constexpr std::uint64_t shard_track_base = 0x1000;
constexpr std::uint32_t packet_sequence_id = 1;

constexpr bool is_switch(event_kind kind) {
  return kind == event_kind::task_switch || kind == event_kind::query_start ||
         kind == event_kind::semaphore_execute ||
         kind == event_kind::execution_stage;
}

std::string event_name(event_kind kind) {
  switch (kind) {
  case event_kind::task_switch:
    return "run_task";
  case event_kind::query_start:
    return "cql_request";
  case event_kind::semaphore_execute:
    return "semaphore_execute";
  case event_kind::execution_stage:
    return "execution_stage";
  case event_kind::io_begin:
    return "io_begin";
  case event_kind::io_end:
    return "io_end";
  case event_kind::stack_sample:
    return "stacktrace_sample";
  }
  return "unknown";
}

track_ids make_track_ids(const std::vector<event> &events) {
  track_ids ids;
  for (const event &e : events) {
    ids.shard.try_emplace(e.shard, shard_track_base + e.shard);
  }
  return ids;
}

struct event_spec {
  std::uint64_t timestamp = 0;
  int order = 0;
  std::uint64_t track = 0;
  TrackEvent::Type type = TrackEvent::TYPE_INSTANT;
  std::string name;
  std::uint64_t task_id = 0;
  std::uint64_t argument = 0;
  std::uint64_t previous_task = 0;
  std::uint32_t shard = 0;
  std::string location;
  std::uint32_t line = 0;
  std::string function;
  std::vector<std::uint64_t> frames;
};

void add_slice(std::vector<event_spec> &specs, std::uint64_t begin,
               std::uint64_t end, std::uint64_t track, std::string name,
               std::uint64_t task_id, std::uint32_t shard,
               const event *begin_event = nullptr, int begin_order = 2,
               int end_order = 1) {
  event_spec begin_spec{begin,
                        begin_order,
                        track,
                        TrackEvent::TYPE_SLICE_BEGIN,
                        std::move(name),
                        task_id,
                        0,
                        0,
                        shard,
                        {},
                        0,
                        {},
                        {}};
  if (begin_event != nullptr) {
    begin_spec.argument = begin_event->argument;
    begin_spec.previous_task = begin_event->previous_task;
    begin_spec.location = begin_event->location;
    begin_spec.line = begin_event->line;
    begin_spec.function = begin_event->function;
    begin_spec.frames = begin_event->frames;
  }
  specs.push_back(std::move(begin_spec));
  specs.push_back({end,
                   end_order,
                   track,
                   TrackEvent::TYPE_SLICE_END,
                   {},
                   task_id,
                   0,
                   0,
                   shard,
                   {},
                   0,
                   {},
                   {}});
}

void add_instant(std::vector<event_spec> &specs, const event &e,
                 std::uint64_t track, std::string name) {
  specs.push_back({e.timestamp_ns, 4, track, TrackEvent::TYPE_INSTANT,
                   std::move(name), e.task_id, e.argument, e.previous_task,
                   e.shard, e.location, e.line, e.function, e.frames});
}

std::string run_task_name(const event &e) {
  if (e.location.empty() || e.line == 0) {
    return "unknown:0";
  }
  return std::format("{}:{}", e.location, e.line);
}

void set_annotation(TrackEvent *output, std::string_view name,
                    std::uint64_t value) {
  auto *annotation = output->add_debug_annotations();
  annotation->set_name(std::string(name));
  annotation->set_uint_value(value);
}

void populate_track_event(const event_spec &spec, TracePacket *packet) {
  // TrackEvent decoding is incremental-state based. Even though this
  // converter does not currently emit interned data, Perfetto requires every
  // TrackEvent packet to identify its trusted packet sequence.
  packet->set_trusted_packet_sequence_id(packet_sequence_id);
  TrackEvent *output = packet->mutable_track_event();
  output->set_type(spec.type);
  output->set_track_uuid(spec.track);
  if (!spec.name.empty()) {
    output->set_name(spec.name);
    output->add_categories("scylla");
  }
  if (spec.task_id != 0) {
    set_annotation(output, "task_id", spec.task_id);
  }
  set_annotation(output, "shard", spec.shard);
  if (spec.argument != 0) {
    set_annotation(output, "io_id", spec.argument);
  }
  if (spec.previous_task != 0) {
    set_annotation(output, "previous_task_id", spec.previous_task);
  }
  if (!spec.location.empty()) {
    auto *location = output->mutable_source_location();
    location->set_file_name(spec.location);
    location->set_line_number(spec.line);
    if (!spec.function.empty()) {
      location->set_function_name(spec.function);
    }
  }
  if (!spec.frames.empty()) {
    std::string frames;
    for (std::size_t i = 0; i < spec.frames.size(); ++i) {
      if (i != 0) {
        frames += ',';
      }
      frames += std::format("{:#x}", spec.frames[i]);
    }
    auto *annotation = output->add_debug_annotations();
    annotation->set_name("frames");
    annotation->set_string_value(frames);
  }
}

void add_descriptor(Trace *trace, std::uint64_t uuid, std::string name,
                    std::string description) {
  TrackDescriptor *descriptor = trace->add_packet()->mutable_track_descriptor();
  descriptor->set_uuid(uuid);
  descriptor->set_name(std::move(name));
  descriptor->set_sibling_merge_behavior(
      TrackDescriptor::SIBLING_MERGE_BEHAVIOR_NONE);
  descriptor->set_child_ordering(TrackDescriptor::CHRONOLOGICAL);
  if (!description.empty()) {
    descriptor->set_description(std::move(description));
  }
}

Trace build_trace(std::vector<event> events) {
  std::sort(events.begin(), events.end(), [](const event &a, const event &b) {
    return a.timestamp_ns < b.timestamp_ns;
  });
  if (events.empty()) {
    return {};
  }

  const track_ids ids = make_track_ids(events);
  Trace trace;
  for (const auto &[shard, uuid] : ids.shard) {
    add_descriptor(&trace, uuid, std::format("Shard {}", shard),
                   "Scylla timeline for one shard");
  }

  std::vector<event_spec> specs;
  struct running {
    std::uint64_t task = 0;
    std::uint64_t start = 0;
    std::optional<event> start_event;
    bool initialized = false;
  };
  std::map<std::uint32_t, running> current;
  for (const event &e : events) {
    running &state = current[e.shard];
    if (!state.initialized) {
      state.start = e.timestamp_ns;
      state.initialized = true;
    }
    if (is_switch(e.kind)) {
      if (state.start < e.timestamp_ns) {
        add_slice(specs, state.start, e.timestamp_ns, ids.shard.at(e.shard),
                  state.task == 0 ? "idle"
                                  : std::format("task {:#x}", state.task),
                  state.task, e.shard);
        if (state.start_event &&
            state.start_event->kind == event_kind::task_switch) {
          add_slice(specs, state.start, e.timestamp_ns, ids.shard.at(e.shard),
                    run_task_name(*state.start_event), state.task, e.shard,
                    &*state.start_event, 3, 0);
        }
      }
      state.task = e.task_id;
      state.start = e.timestamp_ns;
      state.start_event = e;
    } else {
      add_instant(specs, e, ids.shard.at(e.shard), event_name(e.kind));
    }
  }
  const std::uint64_t end = events.back().timestamp_ns;
  for (const auto &[shard, state] : current) {
    if (state.start < end) {
      add_slice(specs, state.start, end, ids.shard.at(shard),
                state.task == 0 ? "idle"
                                : std::format("task {:#x}", state.task),
                state.task, shard);
      if (state.start_event &&
          state.start_event->kind == event_kind::task_switch) {
        add_slice(specs, state.start, end, ids.shard.at(shard),
                  run_task_name(*state.start_event), state.task, shard,
                  &*state.start_event, 3, 0);
      }
    }
  }

  std::sort(specs.begin(), specs.end(),
            [](const event_spec &a, const event_spec &b) {
              if (a.timestamp != b.timestamp) {
                return a.timestamp < b.timestamp;
              }
              return a.order < b.order;
            });
  for (const event_spec &spec : specs) {
    TracePacket *packet = trace.add_packet();
    packet->set_timestamp(spec.timestamp);
    populate_track_event(spec, packet);
  }
  return trace;
}

} // namespace

std::vector<std::byte> serialize(std::span<const event> events) {
  const Trace trace =
      build_trace(std::vector<event>(events.begin(), events.end()));
  if (trace.ByteSizeLong() == 0) {
    return {};
  }
  std::vector<std::byte> result(trace.ByteSizeLong());
  if (!trace.SerializeToArray(result.data(), static_cast<int>(result.size()))) {
    throw std::runtime_error("failed to serialize Perfetto trace");
  }
  return result;
}

std::size_t convert_snapshot(const std::filesystem::path &snapshot_dir,
                             const std::filesystem::path &output) {
  std::vector<std::filesystem::path> files;
  for (const auto &entry : std::filesystem::directory_iterator(snapshot_dir)) {
    if (entry.path().extension() == ".trace") {
      files.push_back(entry.path());
    }
  }
  std::sort(files.begin(), files.end());
  if (files.empty()) {
    throw std::runtime_error(
        std::format("no *.trace files in {}", snapshot_dir.string()));
  }

  std::vector<raw_event> raw_events;
  std::vector<clock_sync_point> syncs;
  for (std::size_t i = 0; i < files.size(); ++i) {
    const std::uint32_t shard = shard_of(files[i]).value_or(std::uint32_t(i));
    const std::vector<char> raw = read_file(files[i]);
    trace::decode({reinterpret_cast<const std::byte *>(raw.data()), raw.size()},
                  decoder_sink{raw_events, syncs, shard});
  }
  if (raw_events.empty()) {
    throw std::runtime_error(
        std::format("no records in {}", snapshot_dir.string()));
  }

  wall_clock clock;
  clock.build(std::move(syncs));
  double ns_per_tick = 0.2941171840072451;
  if (const auto calibrated = clock.ns_per_tick()) {
    ns_per_tick = *calibrated;
  }
  std::int64_t first_tick = std::numeric_limits<std::int64_t>::max();
  for (const raw_event &e : raw_events) {
    first_tick = std::min(first_tick, e.ticks);
  }

  std::vector<event> events;
  events.reserve(raw_events.size());
  for (const raw_event &raw : raw_events) {
    std::uint64_t timestamp = 0;
    if (raw.kind == event_kind::stack_sample && raw.realtime_ns != 0) {
      timestamp = raw.realtime_ns;
    } else if (const auto realtime = clock.realtime_ns(raw.ticks)) {
      timestamp = *realtime;
    } else {
      const __int128 relative = __int128(raw.ticks - first_tick) * ns_per_tick;
      timestamp = relative < 0 ? 0 : std::uint64_t(relative);
    }
    events.push_back({raw.kind, timestamp, raw.shard, raw.task_id, raw.argument,
                      raw.previous_task, raw.location, raw.line, raw.function,
                      raw.frames});
  }

  std::sort(events.begin(), events.end(), [](const event &a, const event &b) {
    return a.timestamp_ns < b.timestamp_ns;
  });
  std::map<std::uint32_t, std::uint64_t> running;
  for (event &e : events) {
    if (is_switch(e.kind)) {
      running[e.shard] = e.task_id;
    } else if (e.kind == event_kind::stack_sample) {
      const auto found = running.find(e.shard);
      e.task_id = found == running.end() ? 0 : found->second;
    }
  }

  const std::uint64_t start = events.front().timestamp_ns;
  for (event &e : events) {
    e.timestamp_ns -= start;
  }
  const std::vector<std::byte> encoded = serialize(events);
  std::ofstream out(output, std::ios::binary | std::ios::trunc);
  if (!out) {
    throw std::system_error(errno, std::generic_category(), output.string());
  }
  out.write(reinterpret_cast<const char *>(encoded.data()),
            static_cast<std::streamsize>(encoded.size()));
  if (!out) {
    throw std::system_error(errno, std::generic_category(), output.string());
  }
  return events.size();
}

} // namespace scylla_trace
