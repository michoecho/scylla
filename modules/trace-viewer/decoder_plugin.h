// One compiled decoder per build of the traced program.
//
// A trace is only readable through the decoder header generated from the binary
// that wrote it, and a cluster mid-upgrade writes traces from several binaries
// at once. There is therefore no such thing as "the" decoder: there is one per
// build ID, they disagree about tracepoint layouts, and the viewer has to hold
// all of them at the same time.
//
// A C++ program cannot include two headers that both define `trace::run_task`,
// so each build's decoder is compiled into a shared object of its own instead:
//
//   1. find that build's `decoder_<build-id>.h` (see `find_header` in the .cc);
//   2. read it and `events.h` with libclang, and match the two struct by struct
//      and field by field;
//   3. write a plugin source -- the decoder header, `events.h`, and a generated
//      bridge that converts one to the other and calls the viewer's exported
//      `on_decode_<event>` -- and compile it to a `.so`;
//   4. dlopen it, and decode that build's files through its
//      `trace_plugin_decode`.
//
// Steps 2 and 3 cost seconds, so the `.so` is cached under
// `$TRACE_PLUGIN_CACHE` (or `~/.cache/trace-viewer`) against a key covering
// every input: a second run of the same viewer on the same snapshot does step 1
// and step 4 and nothing else.
//
// Builds are done one at a time. Two of them in one snapshot cost about two
// seconds each on the first run and nothing on every run after it, and a
// thread per build would buy that back at the price of making the registry's
// map and the toolchain probe thread-safe. Revisit it if a cluster ever arrives
// with a dozen versions in it.
//
// Nothing here throws. A build whose decoder cannot be found, parsed, or
// compiled comes back with a null `decode` and an `error` saying which, and the
// caller reports it and skips that build's files -- one node of a cluster being
// unreadable is not a reason to refuse the other two.

#pragma once

#include <filesystem>
#include <map>
#include <string>
#include <vector>

#include "plugin_abi.h"

namespace plugin {

// What became of one build's decoder.
struct decoder {
    std::string build_id;
    std::filesystem::path header;  // the decoder header this was built from
    std::filesystem::path source;  // the generated plugin source, kept to be read
    std::filesystem::path object;  // the compiled .so

    // Null if this build could not be given a decoder; `error` then says why.
    trace_plugin_decode_fn decode = nullptr;
    std::string error;

    // What events.h and this build's decoder disagreed about: an event only one
    // of them has, a field the decoder has not got, a field whose type will not
    // convert. Every one of them is a thing the viewer will not know about this
    // build's traces, so all of them are printed.
    std::vector<std::string> notes;

    std::size_t events_bridged = 0;
    bool from_cache = false;  // the .so was already built, and none of it was redone
};

// The decoders, one per build, built on first ask.
class registry {
public:
    // `dso_root` is the directory of objects a source location is resolved
    // against; it is handed to every plugin, which keeps a directory of its own
    // over it.
    explicit registry(std::string dso_root);
    ~registry();

    registry(const registry&) = delete;
    registry& operator=(const registry&) = delete;

    // The decoder for `build_id`, whose header is looked for beside `beside` --
    // the directory the trace files came from -- as well as in
    // `$TRACE_DECODER_DIR`. Built the first time it is asked for and kept; the
    // same build asked for again is the same object, whichever directory the
    // second ask came from.
    //
    // An empty `build_id` is a snapshot too old to carry one in its metadata:
    // it is keyed on the directory instead, so two such snapshots do not share
    // a decoder they have no reason to share.
    const decoder& for_build(const std::string& build_id,
                             const std::filesystem::path& beside);

    [[nodiscard]] const std::string& dso_root() const { return dso_root_; }

private:
    std::string dso_root_;
    std::map<std::string, decoder> decoders_;  // by build id, or by "dir:<path>"
    std::vector<void*> handles_;               // dlopen()ed, closed by ~registry
};

}  // namespace plugin
