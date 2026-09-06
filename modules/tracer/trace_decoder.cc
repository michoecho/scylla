// The generated decoder, as a program.
//
// Everything specific to a tracepoint table is in the generated header; this is
// only a callback that prints. It exists as much to keep the generated code
// honest -- a header that does not compile on its own is a header nobody can
// use -- as to be run.

#include <cstddef>
#include <exception>
#include <format>
#include <fstream>
#include <iostream>
#include <iterator>
#include <span>
#include <string>
#include <vector>

#include "tracer_generated/decoder.h"

namespace {

// One operator(), taking anything: printing a whole event is the one thing
// every tracepoint struct can do alike. A consumer that wants a particular
// tracepoint's fields writes an overload for it; see tracer_test.cc.
struct printer {
    template <typename Event>
    void operator()(const Event& event, const trace::tracepoint_metadata& meta) const {
        // A record of a tracepoint that carries no timestamp is handed the one
        // of the record before it, which is a real answer to "when" but not one
        // this column should claim. A dash says which it is; a consumer that
        // needs a number for such a record has to make one up, and making one
        // up is not this program's job. See tracepoint_metadata.
        std::cout << std::format("{:>18} | {:<{}} | {}\n",
                                 meta.has_timestamp ? std::format("{}", meta.timestamp) : "-",
                                 std::format("{}:{}", meta.file, meta.line),
                                 trace::fileline_width, event.to_string());
    }
};

}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "usage: " << argv[0] << " <trace-file>\n";
        return 2;
    }

    std::ifstream in(argv[1], std::ios::binary);
    if (!in) {
        std::cerr << "cannot open " << argv[1] << "\n";
        return 1;
    }
    const std::vector<char> raw{std::istreambuf_iterator<char>(in),
                                std::istreambuf_iterator<char>()};

    try {
        trace::decode({reinterpret_cast<const std::byte*>(raw.data()), raw.size()}, printer{});
    } catch (const std::exception& e) {
        std::cerr << e.what() << "\n";
        return 1;
    }
    return 0;
}
