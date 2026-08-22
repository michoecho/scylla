// A compact doctest reporter for the Buck2 VS Code executor.
//
// The executor may run several selected cases in one process when the target
// carries the `startup_shared` label. Doctest's exit code only describes the
// aggregate run, so this reporter emits one unambiguous record per case.

#include <cmath>
#include <cstdio>

#include "doctest/doctest.h"

namespace {

void print_hex(const char* value) {
    for (const unsigned char* cursor =
             reinterpret_cast<const unsigned char*>(value);
         *cursor != '\0'; ++cursor) {
        std::printf("%02x", static_cast<unsigned>(*cursor));
    }
}

struct VscodeResultsReporter final : doctest::IReporter {
    const doctest::TestCaseData* current = nullptr;

    explicit VscodeResultsReporter(const doctest::ContextOptions&) {}

    void report_query(const doctest::QueryData&) override {}
    void test_run_start() override {}
    void test_run_end(const doctest::TestRunStats&) override {}

    void test_case_start(const doctest::TestCaseData& test) override {
        current = &test;
        std::printf("RUN %s:%u: %s\n",
                    current->m_file.c_str(),
                    current->m_line,
                    current->m_name);
        std::fflush(stdout);
    }

    void test_case_reenter(const doctest::TestCaseData& test) override {
        current = &test;
    }

    void test_case_end(const doctest::CurrentTestCaseStats& stats) override {
        if (current == nullptr) {
            return;
        }
        std::printf("VSCODE_TEST_RESULT\t");
        print_hex(current->m_name);
        std::printf("\t%s\t%llu\n",
                    stats.testCaseSuccess ? "passed" : "failed",
                    static_cast<unsigned long long>(
                        std::llround(stats.seconds * 1000.0)));
        std::fflush(stdout);
    }

    void test_case_exception(const doctest::TestCaseException&) override {}
    void test_case_skipped(const doctest::TestCaseData&) override {}
    void subcase_start(const doctest::SubcaseSignature&) override {}
    void subcase_end() override {}
    void log_assert(const doctest::AssertData&) override {}
    void log_message(const doctest::MessageData&) override {}
};

DOCTEST_REGISTER_REPORTER("vscode-results", 0, VscodeResultsReporter);

}  // namespace
