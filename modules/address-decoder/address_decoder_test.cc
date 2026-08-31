#include <doctest/doctest.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include "address_decoder/address_decoder.h"
#include "address_decoder/json.h"

namespace {

using namespace addrdec;

// Wait for the decoder to answer everything asked of it.
//
// A poll rather than a wait: reap() is deliberately non-blocking, because the
// caller it is written for is a render loop that must return whether or not an
// answer arrived. So the test does what that loop does, only faster and with a
// deadline -- spawning a symbolizer on a large object is slow, which is the
// whole reason this module exists.
std::vector<decoded_address> drain(address_decoder& decoder,
                                   std::chrono::seconds timeout = std::chrono::seconds(30)) {
    std::vector<decoded_address> out;
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (decoder.outstanding() != 0 && std::chrono::steady_clock::now() < deadline) {
        for (decoded_address& d : decoder.reap()) {
            out.push_back(std::move(d));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    for (decoded_address& d : decoder.reap()) {
        out.push_back(std::move(d));
    }
    return out;
}

}  // namespace

TEST_CASE("json parses the shape llvm-symbolizer emits") {
    const auto doc = json::parse(
        R"J({"Address":"0x1160","ModuleName":"./t","Symbol":[{"Column":36,"FileName":"/tmp/t.cc",)J"
        R"J("FunctionName":"outer(int)","Line":3}]})J");
    REQUIRE(doc.has_value());
    REQUIRE(doc->size() == 1);
    CHECK((*doc)[0].function == "outer(int)");
    CHECK((*doc)[0].file == "/tmp/t.cc");
    CHECK((*doc)[0].line == 3);
    CHECK((*doc)[0].column == 36);
}

TEST_CASE("json handles escapes and omitted fields") {
    const auto doc = json::parse(
        R"J({"Symbol":[{"FunctionName":"x\tyA\"z","Discriminator":2}]})J");
    REQUIRE(doc.has_value());
    REQUIRE(doc->size() == 1);
    CHECK((*doc)[0].function == "x\tyA\"z");
    CHECK((*doc)[0].file.empty());
    CHECK((*doc)[0].line == 0);
    CHECK((*doc)[0].column == 0);
}

TEST_CASE("json rejects malformed input rather than guessing") {
    CHECK(!json::parse("{").has_value());
    CHECK(!json::parse("{\"a\":}").has_value());
    CHECK(!json::parse("[1,]").has_value());
    CHECK(!json::parse("\"unterminated").has_value());
    CHECK(!json::parse("tru").has_value());
    CHECK(!json::parse("01x").has_value());
    // Trailing garbage matters here specifically: the reply reader hands one
    // line at a time, and a line that is a valid document plus junk is a line
    // that did not come from the symbolizer.
    CHECK(!json::parse("{} {}").has_value());
    CHECK(json::parse("  {\"Symbol\":[]}  ").has_value());
}

TEST_CASE("a symbolizer reply becomes frames, innermost first") {
    const auto frames = parse_symbolizer_reply(
        R"J({"Address":"0x1160","ModuleName":"./t","Symbol":[)J"
        R"J({"Column":7,"FileName":"/tmp/t.cc","FunctionName":"inner","Line":2},)J"
        R"J({"Column":36,"FileName":"/tmp/t.cc","FunctionName":"outer","Line":3}]})J");
    REQUIRE(frames.size() == 2);
    CHECK(frames[0].function == "inner");
    CHECK(frames[0].line == 2);
    CHECK(frames[0].column == 7);
    CHECK(frames[1].function == "outer");
    CHECK(frames[1].file == "/tmp/t.cc");
}

TEST_CASE("a reply with nothing in it resolves to no frames") {
    // What an address outside anything the object describes comes back as: a
    // record is still emitted, with every field blank. Reporting that as a
    // frame would print "?? at :0" under every unresolvable address.
    CHECK(parse_symbolizer_reply(
              R"J({"ModuleName":"","Symbol":[{"Column":0,"FileName":"","FunctionName":"","Line":0}]})J")
              .empty());
    CHECK(parse_symbolizer_reply(R"J({"Loc":[],"ModuleName":"./t","SymName":"BAD"})J").empty());
    CHECK(parse_symbolizer_reply("not json at all").empty());
}

TEST_CASE("an address with no object is answered, not dropped") {
    // The caller's state machine moves new -> sent -> decoded and has no way
    // back, so an address that can never be looked up still has to produce a
    // result -- otherwise it sits in "sent" forever.
    address_decoder decoder;
    decoder.request({0x1234, "", 0});
    CHECK(decoder.outstanding() == 1);
    const std::vector<decoded_address> got = decoder.reap();
    REQUIRE(got.size() == 1);
    CHECK(got[0].address == 0x1234);
    CHECK(got[0].frames.empty());
    CHECK(decoder.outstanding() == 0);
    REQUIRE(decoder.lookup(0x1234) != nullptr);
    CHECK(decoder.lookup(0x1234)->frames.empty());
}

TEST_CASE("a repeated address is not asked twice") {
    address_decoder decoder;
    decoder.request({0x1234, "", 0});
    decoder.request({0x1234, "", 0});
    CHECK(decoder.reap().size() == 1);
    // And once answered it is not asked again either.
    decoder.request({0x1234, "", 0});
    CHECK(decoder.reap().empty());
}

TEST_CASE("a missing symbolizer answers everything unresolved") {
    // The failure that matters is a binary that is not there: it must come back
    // as unresolved frames rather than hanging the caller forever waiting for a
    // process that never started.
    address_decoder decoder("no-such-symbolizer-binary-anywhere");
    decoder.request({0x40, "/proc/self/exe", 0x40});
    const std::vector<decoded_address> got = drain(decoder, std::chrono::seconds(10));
    REQUIRE(got.size() == 1);
    CHECK(got[0].frames.empty());
    CHECK(decoder.any_worker_failed());
}
