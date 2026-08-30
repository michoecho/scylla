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
    CHECK((*doc)["ModuleName"].string_or("ModuleName") == "");
    CHECK(doc->string_or("Address") == "0x1160");
    const json::array* const symbols = (*doc)["Symbol"].as_array();
    REQUIRE(symbols != nullptr);
    REQUIRE(symbols->size() == 1);
    CHECK((*symbols)[0].string_or("FunctionName") == "outer(int)");
    CHECK((*symbols)[0].int_or("Line") == 3);
    CHECK((*symbols)[0].int_or("Discriminator", -1) == -1);
}

TEST_CASE("json handles escapes, nesting and the literals") {
    const auto doc = json::parse(
        R"J({"a":"x\tyA\"z","b":[1,-2.5,1e3,true,false,null],"c":{"d":{"e":[]}}})J");
    REQUIRE(doc.has_value());
    CHECK(doc->string_or("a") == "x\tyA\"z");
    const json::array* const b = (*doc)["b"].as_array();
    REQUIRE(b != nullptr);
    REQUIRE(b->size() == 6);
    CHECK((*b)[0].as_int().value_or(0) == 1);
    CHECK((*b)[1].as_number().value_or(0) == doctest::Approx(-2.5));
    CHECK((*b)[2].as_int().value_or(0) == 1000);
    CHECK((*b)[3].as_bool().value_or(false) == true);
    CHECK((*b)[4].as_bool().value_or(true) == false);
    CHECK((*b)[5].is_null());
    CHECK((*doc)["c"]["d"]["e"].as_array() != nullptr);
    // A missing key chains without blowing up, which is what lets the reply
    // parser read a field out of a record that does not have it.
    CHECK((*doc)["nope"]["also nope"].is_null());
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
    CHECK(json::parse("  {\"a\":1}  ").has_value());
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

TEST_CASE("a real symbolizer resolves an address in a real object") {
    // Skipped rather than failed where there is no llvm-symbolizer: this is the
    // one case that needs the toolchain, and a checkout without it should still
    // get every test above.
    if (std::system("llvm-symbolizer --version >/dev/null 2>&1") != 0) {
        MESSAGE("no llvm-symbolizer on PATH; skipping");
        return;
    }

    const std::filesystem::path dir =
        std::filesystem::temp_directory_path() / "addrdec_test";
    std::filesystem::create_directories(dir);
    const std::filesystem::path source = dir / "subject.cc";
    const std::filesystem::path object = dir / "subject";
    {
        std::ofstream out(source);
        out << "int addrdec_subject(int x) { return x * 3; }\n"
               "int main() { return addrdec_subject(2) == 6 ? 0 : 1; }\n";
    }
    const std::string compile =
        "c++ -g -O0 -fno-inline -o '" + object.string() + "' '" + source.string() + "' 2>/dev/null";
    if (std::system(compile.c_str()) != 0) {
        MESSAGE("no working c++ driver; skipping");
        return;
    }

    // The address to ask about, found the way the viewer finds one: out of the
    // object's own symbol table, in the object's own address space. There is no
    // load bias to undo because nothing is loaded.
    std::uint64_t file_offset = 0;
    {
        const std::string nm = "nm '" + object.string() + "' 2>/dev/null";
        std::FILE* const pipe = ::popen(nm.c_str(), "r");
        REQUIRE(pipe != nullptr);
        char line[1024];
        while (std::fgets(line, sizeof(line), pipe) != nullptr) {
            const std::string text(line);
            if (text.find("addrdec_subject") != std::string::npos) {
                file_offset = std::strtoull(text.c_str(), nullptr, 16);
            }
        }
        ::pclose(pipe);
    }
    REQUIRE(file_offset != 0);

    address_decoder decoder;
    // The caller's key is deliberately not the file offset, to pin down that
    // the two are kept apart: the viewer asks about process addresses and gets
    // process addresses back.
    const std::uint64_t key = file_offset + 0x7f0000000000ull;
    decoder.request({key, object.string(), file_offset});
    const std::vector<decoded_address> got = drain(decoder);
    REQUIRE(got.size() == 1);
    CHECK(got[0].address == key);
    REQUIRE(!got[0].frames.empty());
    CHECK(got[0].frames.back().function.find("addrdec_subject") != std::string::npos);
    CHECK(got[0].frames.back().file.find("subject.cc") != std::string::npos);
    CHECK(!got[0].to_string().empty());

    // The second ask for the same address never reaches the symbolizer: it is
    // already known, so nothing is queued at all.
    decoder.request({key, object.string(), file_offset});
    CHECK(decoder.outstanding() == 0);
    CHECK(decoder.reap().empty());
    CHECK(!decoder.any_worker_failed());

    std::filesystem::remove_all(dir);
}

TEST_CASE("destruction interrupts a lookup in flight") {
    // The failure this guards against is an exit that hangs. A worker blocked
    // reading its symbolizer's answer is blocked for however long that answer
    // takes, and the first answer out of a half-gigabyte of debug info is
    // minutes -- so a destructor that set a flag and joined would wedge the
    // program on quit for exactly the object that made this module worth
    // writing. Here the stand-in symbolizer simply never answers.
    const std::filesystem::path dir = std::filesystem::temp_directory_path() / "addrdec_test_hang";
    std::filesystem::create_directories(dir);
    const std::filesystem::path fake = dir / "never-answers";
    {
        std::ofstream out(fake);
        out << "#!/bin/sh\nread line\nsleep 600\n";
    }
    std::filesystem::permissions(fake, std::filesystem::perms::owner_all);

    const auto begin = std::chrono::steady_clock::now();
    {
        address_decoder decoder(fake.string());
        decoder.request({0x1234, "/proc/self/exe", 0x1234});
        // Let the worker actually reach the read; destroying before the spawn
        // would exercise the easy path rather than the one that hangs.
        for (int i = 0; i < 200 && decoder.outstanding() != 0; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        CHECK(decoder.outstanding() == 1);
    }
    const auto took = std::chrono::steady_clock::now() - begin;
    CHECK(took < std::chrono::seconds(20));

    std::filesystem::remove_all(dir);
}
