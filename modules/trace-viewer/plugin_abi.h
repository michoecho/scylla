// The boundary between the viewer and a decoder plugin.
//
// A plugin is a `.so` the viewer generates and compiles at startup, from the
// tracepoint tables in the objects a snapshot came from: trace_wire.h, this
// viewer's events.h, and a generated switch that reads one build's records
// straight into the other's structs. See decoder_plugin.h.
//
// Three symbols cross this boundary, and all of them are C:
//
//   viewer -> plugin   `trace_plugin_decode`, which reads a whole trace file,
//                      and `trace_plugin_notes`, which says what the tables and
//                      events.h disagreed about.
//   plugin -> viewer   `on_decode_<event>`, one per event in events.h, which
//                      the viewer exports (hence -rdynamic) and the plugin
//                      leaves undefined until it is dlopen()ed.
//
// The `on_decode_*` declarations are not here, because which ones exist is a
// fact about events.h: the generator writes into the plugin source the ones it
// bridged, and viewer.cc defines them all beside decode_sink.
//
// **No exception crosses this boundary.** The viewer links its C++ runtime
// statically, so a `std::runtime_error` thrown in a plugin and caught in the
// viewer would be compared against a different `std::type_info` and go
// uncaught. `trace_plugin_decode` therefore catches everything and hands back a
// message in a buffer, and nothing else here throws.

#pragma once

#include <cstddef>

extern "C" {

// Decode one whole trace file.
//
// `data`/`size` are the file's bytes, which must outlive the call: everything a
// decoded event points at points into them. `sink` is passed through to every
// `on_decode_*` callback and is opaque here -- it is the viewer's decode_sink.
// `dso_root` is the directory of objects, by build ID, that a source location is
// resolved against; the plugin keeps one directory per root and reuses it, so
// each object is read once however many files are decoded through it.
//
// Returns 0 on success. On failure returns non-zero and writes a
// NUL-terminated message into `error`, which is what the trace could not be
// read past -- a record is not self-delimiting, so whatever was decoded before
// that point has already been delivered and is the whole of what this file
// yielded.
using trace_plugin_decode_fn = int (*)(const void* data, std::size_t size, void* sink,
                                       const char* dso_root, char* error,
                                       std::size_t error_size);

// What this plugin knows it will not tell the viewer: a field the tables have
// and events.h has not got, or one whose wire type will not convert to the
// member events.h declares. One call to `emit` per note, with `ctx` passed
// back untouched.
//
// Answered by the plugin rather than by the generator because the questions are
// about events.h's *types*, which only the compiler that built the plugin has
// looked at: every note here is a `requires` the generated source resolved. It
// is therefore cheap and unchanging, and the viewer asks every run rather than
// caching the answer beside the object.
using trace_plugin_note_fn = void (*)(void* ctx, const char* note);
using trace_plugin_notes_fn = void (*)(trace_plugin_note_fn emit, void* ctx);

}  // extern "C"

// The symbols the two are looked up under.
inline constexpr char trace_plugin_decode_symbol[] = "trace_plugin_decode";
inline constexpr char trace_plugin_notes_symbol[] = "trace_plugin_notes";
