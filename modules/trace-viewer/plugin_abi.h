// The boundary between the viewer and a decoder plugin.
//
// A plugin is a `.so` the viewer generates and compiles at startup, one per
// build of the traced program: that build's `decoder_<build-id>.h`, this
// viewer's `events.h`, and a generated bridge between them. See
// decoder_plugin.cc.
//
// Two directions cross this boundary, and both of them are C:
//
//   viewer -> plugin   `trace_plugin_decode`, the one exported entry point.
//   plugin -> viewer   `on_decode_<event>`, one per event in events.h, which
//                      the viewer exports (hence -rdynamic) and the plugin
//                      leaves undefined until it is dlopen()ed.
//
// The `on_decode_*` declarations are not here, because which ones exist is a
// fact about events.h: the generator writes them into the plugin source, and
// viewer.cc defines them beside decode_sink. They are named by the event, take
// the opaque `sink` this call was given, and are the only way anything the
// plugin decoded reaches the viewer.
//
// **No exception crosses this boundary.** The viewer links its C++ runtime
// statically, so a `std::runtime_error` thrown in a plugin and caught in the
// viewer would be compared against a different `std::type_info` and go
// uncaught. `trace_plugin_decode` therefore catches everything and hands back a
// message in a buffer, and the `on_decode_*` callbacks do not throw.

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

}  // extern "C"

// The symbol `trace_plugin_decode_fn` is looked up under.
inline constexpr char trace_plugin_decode_symbol[] = "trace_plugin_decode";
