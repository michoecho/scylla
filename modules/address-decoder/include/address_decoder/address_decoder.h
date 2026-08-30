// Turning return addresses into function names and source lines, without ever
// making the caller wait for one.
//
// The naive way to symbolise a backtrace is to spawn addr2line and read its
// output, and for a program the size of Scylla that is unusable interactively:
// the *spawn* alone is a second or two, because the process opens a 500 MB
// object and indexes its debug info before it can answer anything, and the
// answer is wanted while a frame is being drawn.
//
// So the process is spawned once per object and kept, and it is kept on a
// thread of its own so that the frame drawing never touches it:
//
//   - one worker thread per object file, owning a persistent llvm-symbolizer
//     bound to that object with --obj. It reads addresses off a queue, writes
//     them into the symbolizer's stdin, and reads the JSON line that comes
//     back. Each object is indexed once, by the first address that lands in it.
//   - a cache per worker, so a repeated address never reaches the symbolizer.
//   - one results queue back to the caller, drained by reap() whenever it
//     suits -- typically once a frame.
//
// The caller's half of the contract is that decoding is *asynchronous*: an
// address that has just been requested has no answer, and the UI has to be able
// to draw the frame in which that is true. See the state machine the sample
// window keeps per displayed address.
#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace addrdec {

// One source position an address resolved to.
//
// An address resolves to *several* of these when the code there was inlined:
// the innermost inlined function first, then its caller, and so on out to the
// function that actually owns the machine code. Empty fields are what the
// symbolizer had -- a stripped object gives a function name from the symbol
// table with no file and no line.
struct source_frame {
    std::string function;
    std::string file;
    std::uint32_t line = 0;
    std::uint32_t column = 0;
};

// The answer for one address.
//
// `frames` empty means the address could not be resolved at all -- no object
// for it, no symbolizer, or the symbolizer had nothing. That is a final answer
// and is cached like any other, so a hopeless address is not retried forever.
struct decoded_address {
    std::uint64_t address = 0;
    std::vector<source_frame> frames;

    // The usual one-line rendering: the innermost frame, with the inlined
    // frames that share its machine code indented under it. Empty for an
    // address that did not resolve.
    std::string to_string() const;
};

// A request: which address, and where in which file to look it up.
//
// `address` is the caller's key and is echoed back untouched -- it is whatever
// the caller wants to index its own state by, typically the process address
// with the return-address adjustment already applied. `file_offset` is where
// that address falls in the object's own address space, which is what the
// symbolizer wants. Keeping the two apart is what lets the caller do the
// mapping arithmetic once and never think about it again.
struct decode_request {
    std::uint64_t address = 0;
    std::string object_path;
    std::uint64_t file_offset = 0;
};

// Parse one line of llvm-symbolizer --output-style=JSON output into the frames
// it describes, innermost inlined frame first.
//
// Exposed because it is the half of this module worth testing on its own: the
// subprocess plumbing needs a real symbolizer and a real object to say anything,
// while the reply format is exactly what changes under you when the toolchain
// moves. Returns empty for a malformed line, and for a reply whose records are
// all blank -- which is what an address that hit nothing comes back as.
std::vector<source_frame> parse_symbolizer_reply(std::string_view line);

class address_decoder {
public:
    // `symbolizer` is the binary to run; empty takes $TRACE_SYMBOLIZER, then
    // "llvm-symbolizer" off the PATH. Nothing is spawned by the constructor --
    // a viewer opened on a trace nobody symbolises costs nothing.
    explicit address_decoder(std::string symbolizer = {});
    ~address_decoder();

    address_decoder(const address_decoder&) = delete;
    address_decoder& operator=(const address_decoder&) = delete;

    // Queue an address, and return immediately.
    //
    // Idempotent per address: a request for one already answered, or already in
    // flight, is dropped. So a caller may re-request every frame without
    // tracking anything, though the point of the state machine on the other
    // side is that it does not have to.
    void request(const decode_request& what);

    // Everything answered since the last call, in no particular order. Never
    // blocks and never spawns anything.
    std::vector<decoded_address> reap();

    // The answer for an address if one is already known, else nullptr. Valid
    // until the next request()/reap(); this is a lookup into the decoder's own
    // record of what it has handed out, not the worker caches.
    const decoded_address* lookup(std::uint64_t address) const;

    // How many addresses are queued or in flight. For the UI to say so.
    std::size_t outstanding() const;

    // Whether any worker failed to start its symbolizer. Sticky, and the reason
    // every address in that object comes back unresolved.
    bool any_worker_failed() const;

private:
    struct impl;
    std::unique_ptr<impl> impl_;
};

}  // namespace addrdec
