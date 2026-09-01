// A compact doctest reporter for the Buck2 VS Code executor.
//
// The executor may run several selected cases in one process when the target
// carries the `startup_shared` label. Doctest's exit code only describes the
// aggregate run, so this reporter emits one unambiguous record per case. It
// also brackets each case on both output streams. The executor uses those
// brackets to keep output written by one case away from its neighbours.

#include <cmath>
#include <cstdio>
#include <cstring>
#include <string>

#include "vscode_results_reporter.h"

namespace {

void print_hex(std::FILE* stream, const char* value) {
    static constexpr char kHexDigits[] = "0123456789abcdef";
    std::string out;
    out.reserve(std::strlen(value) * 2);
    for (const unsigned char* cursor =
             reinterpret_cast<const unsigned char*>(value);
         *cursor != '\0'; ++cursor) {
        out.push_back(kHexDigits[*cursor >> 4]);
        out.push_back(kHexDigits[*cursor & 0x0F]);
    }
    std::fwrite(out.data(), 1, out.size(), stream);
}

void print_output_marker(const char* marker, const char* case_name) {
    // Test code is allowed to leave either stream without a trailing newline.
    // Start every marker on its own line so the executor can frame output
    // without confusing a marker with a suffix of test output.
    std::fputc('\n', stdout);
    std::printf("%s\t", marker);
    print_hex(stdout, case_name);
    std::printf("\n");
    std::fputc('\n', stderr);
    std::fprintf(stderr, "%s\t", marker);
    print_hex(stderr, case_name);
    std::fprintf(stderr, "\n");
    std::fflush(stdout);
    std::fflush(stderr);
}

void print_failure_reason(int failure_flags) {
    const struct {
        int flag;
        const char* name;
    } reasons[] = {
        {doctest::TestCaseFailureReason::AssertFailure, "AssertFailure"},
        {doctest::TestCaseFailureReason::Exception, "Exception"},
        {doctest::TestCaseFailureReason::Crash, "Crash"},
        {doctest::TestCaseFailureReason::TooManyFailedAsserts, "TooManyFailedAsserts"},
        {doctest::TestCaseFailureReason::Timeout, "Timeout"},
        {doctest::TestCaseFailureReason::ShouldHaveFailedButDidnt,
         "ShouldHaveFailedButDidnt"},
        {doctest::TestCaseFailureReason::ShouldHaveFailedAndDid,
         "ShouldHaveFailedAndDid"},
        {doctest::TestCaseFailureReason::DidntFailExactlyNumTimes,
         "DidntFailExactlyNumTimes"},
        {doctest::TestCaseFailureReason::FailedExactlyNumTimes,
         "FailedExactlyNumTimes"},
        {doctest::TestCaseFailureReason::CouldHaveFailedAndDid,
         "CouldHaveFailedAndDid"},
    };

    bool first = true;
    for (const auto& reason : reasons) {
        if ((failure_flags & reason.flag) == 0) {
            continue;
        }
        if (!first) {
            std::printf("|");
        }
        std::printf("%s", reason.name);
        first = false;
    }
    if (first) {
        std::printf("None");
    }
}

}  // namespace

VscodeResultsReporter::VscodeResultsReporter(const doctest::ContextOptions&) {}

void VscodeResultsReporter::report_query(const doctest::QueryData&) {}
void VscodeResultsReporter::test_run_start() {}
void VscodeResultsReporter::test_run_end(const doctest::TestRunStats&) {}

void VscodeResultsReporter::test_case_start(const doctest::TestCaseData& test) {
    current_ = &test;
    print_output_marker("VSCODE_TEST_OUTPUT_START", current_->m_name);
    std::printf("RUN %s:%u: %s\n",
                current_->m_file.c_str(),
                current_->m_line,
                current_->m_name);
    std::fflush(stdout);
}

void VscodeResultsReporter::test_case_reenter(const doctest::TestCaseData& test) {
    current_ = &test;
}

void VscodeResultsReporter::test_case_end(const doctest::CurrentTestCaseStats& stats) {
    if (current_ == nullptr) {
        return;
    }
    const int asserts_failed = stats.numAssertsFailedCurrentTest;
    const int asserts_passed = stats.numAssertsCurrentTest - asserts_failed;
    if (stats.testCaseSuccess) {
        std::printf("PASS (%d asserts passed)\n", asserts_passed);
    } else {
        std::printf("FAIL (%d asserts passed, %d asserts failed, failure reason: ",
                    asserts_passed,
                    asserts_failed);
        print_failure_reason(stats.failure_flags);
        std::printf(")\n");
    }
    std::fflush(stdout);
    print_output_marker("VSCODE_TEST_OUTPUT_END", current_->m_name);
    std::printf("VSCODE_TEST_RESULT\t");
    print_hex(stdout, current_->m_name);
    std::printf("\t%s\t%llu\n",
                stats.testCaseSuccess ? "passed" : "failed",
                static_cast<unsigned long long>(
                    std::llround(stats.seconds * 1000.0)));
    std::fflush(stdout);
}

void VscodeResultsReporter::test_case_exception(const doctest::TestCaseException&) {}
void VscodeResultsReporter::test_case_skipped(const doctest::TestCaseData&) {}
void VscodeResultsReporter::subcase_start(const doctest::SubcaseSignature&) {}
void VscodeResultsReporter::subcase_end() {}
void VscodeResultsReporter::log_assert(const doctest::AssertData&) {}
void VscodeResultsReporter::log_message(const doctest::MessageData&) {}

DOCTEST_REGISTER_REPORTER("vscode-results", 0, VscodeResultsReporter);
