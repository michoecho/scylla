// A doctest reporter that lists each test case's source location for test
// discovery integrations.
//
// The VS Code test explorer needs to learn where each case is defined. doctest's
// built-in XML reporter carries that (filename=, line=), but consuming it
// means parsing XML and dealing with escaping and optional attributes.
//
// The location is a plain field on TestCaseData, which report_query hands us
// directly. Emitting it as two newline-delimited records keeps discovery
// independent of XML serialization and escaping.

#include <cstdio>

#include "doctest/doctest.h"

namespace {

struct TestLocationsReporter final : doctest::IReporter {
    explicit TestLocationsReporter(const doctest::ContextOptions&) {}

    void report_query(const doctest::QueryData& query) override {
        for (unsigned i = 0; i < query.num_data; ++i) {
            const doctest::TestCaseData* test = query.data[i];
            std::printf("%s\n%s:%u\n",
                        test->m_name,
                        test->m_file.c_str(),
                        test->m_line);
        }
    }

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

DOCTEST_REGISTER_REPORTER("test-locations", 0, TestLocationsReporter);

}  // namespace
