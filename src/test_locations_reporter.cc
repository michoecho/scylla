// A doctest reporter that lists each test case's source location, for CMake
// test discovery.
//
// The VS Code test explorer resolves "go to test" from the CTest
// DEF_SOURCE_LINE property, so discovery has to learn where each case is
// defined. doctest's built-in xml reporter carries that (filename=, line=),
// but consuming it means parsing XML from CMake -- which in practice meant a
// regex sensitive to attribute order, to entity escaping, and to the fact that
// testsuite= is omitted entirely for cases belonging to no suite.
//
// The location is not actually derived from the XML: it is a plain field on
// TestCaseData, which report_query hands us directly. Emitting it in a form
// CMake can split with string(REPLACE) skips the serialize-then-reparse round
// trip and every escaping question that comes with it.
//
// Output is two lines per case -- name, then file:line -- rather than a single
// delimited line. Test names are arbitrary user strings that routinely contain
// spaces and colons, so a newline (the one character doctest's own
// --list-test-cases already treats as a record separator) is the only
// delimiter that needs no escaping on either side.
//
// This TU is linked into every runner that CMake discovers tests from; it
// registers the reporter as a side effect and is otherwise inert. It must not
// define DOCTEST_CONFIG_IMPLEMENT -- the runner's own main() TU does that.

#include <cstdio>

#include "doctest/doctest.h"

namespace {

struct TestLocationsReporter : public doctest::IReporter {
    explicit TestLocationsReporter(const doctest::ContextOptions&) {}

    // The only callback that does anything. doctest routes --list-test-cases
    // here, having already applied the name/suite/source-file filters, so the
    // listing matches the set of cases that would actually run.
    void report_query(const doctest::QueryData& in) override {
        for (unsigned i = 0; i < in.num_data; ++i) {
            const doctest::TestCaseData* tc = in.data[i];
            std::printf("%s\n%s:%u\n", tc->m_name, tc->m_file.c_str(), tc->m_line);
        }
    }

    // Discovery never runs tests, so the execution callbacks are unused. They
    // are pure virtual on IReporter and so must still be defined.
    void test_run_start() override {}
    void test_run_end(const doctest::TestRunStats&) override {}
    void test_case_start(const doctest::TestCaseData&) override {}
    void test_case_reenter(const doctest::TestCaseData&) override {}
    void test_case_end(const doctest::CurrentTestCaseStats&) override {}
    void test_case_exception(const doctest::TestCaseException&) override {}
    void subcase_start(const doctest::SubcaseSignature&) override {}
    void subcase_end() override {}
    void log_assert(const doctest::AssertData&) override {}
    void log_message(const doctest::MessageData&) override {}
    void test_case_skipped(const doctest::TestCaseData&) override {}
};

// Registered under a name the discovery script passes to --reporters. Priority
// 0 matches the built-in reporters; it only orders reporters when several are
// active, and discovery selects this one explicitly.
DOCTEST_REGISTER_REPORTER("test-locations", 0, TestLocationsReporter);

} // namespace
