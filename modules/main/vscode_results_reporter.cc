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

void append_hex(std::string& out, const char* value) {
    static constexpr char kHexDigits[] = "0123456789abcdef";
    out.reserve(out.size() + std::strlen(value) * 2);
    for (const unsigned char* cursor =
             reinterpret_cast<const unsigned char*>(value);
         *cursor != '\0'; ++cursor) {
        out.push_back(kHexDigits[*cursor >> 4]);
        out.push_back(kHexDigits[*cursor & 0x0F]);
    }
}

void append_output_marker(std::string& out, const char* marker, const char* case_name) {
    out.push_back('\n');
    out += marker;
    out.push_back('\t');
    append_hex(out, case_name);
    out.push_back('\n');
}

void print_output_marker(const char* marker, const char* case_name) {
    // Test code is allowed to leave either stream without a trailing newline.
    // Start every marker on its own line so the executor can frame output
    // without confusing a marker with a suffix of test output.
    std::string line;
    line.reserve(1 + std::strlen(marker) + 1 + std::strlen(case_name) * 2 + 1);
    append_output_marker(line, marker, case_name);

    std::fwrite(line.data(), 1, line.size(), stdout);
    std::fwrite(line.data(), 1, line.size(), stderr);
    std::fflush(stdout);
}

void append_failure_reason(std::string& out, int failure_flags) {
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
            out.push_back('|');
        }
        out += reason.name;
        first = false;
    }
    if (first) {
        out += "None";
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

    // The end marker goes to both streams; the pass/fail line and the result
    // record only to stdout. Build each stream's bytes in memory so the whole
    // per-case report is a single write per stream.
    std::string marker;
    append_output_marker(marker, "VSCODE_TEST_OUTPUT_END", current_->m_name);

    std::string out;
    out.reserve(marker.size() + 64);
    out += marker;
    if (stats.testCaseSuccess) {
        out += "PASS (";
        out += std::to_string(asserts_passed);
        out += " asserts passed)\n";
    } else {
        out += "FAIL (";
        out += std::to_string(asserts_passed);
        out += " asserts passed, ";
        out += std::to_string(asserts_failed);
        out += " asserts failed, failure reason: ";
        append_failure_reason(out, stats.failure_flags);
        out += ")\n";
    }
    out += "VSCODE_TEST_RESULT\t";
    append_hex(out, current_->m_name);
    out += "\t";
    out += stats.testCaseSuccess ? "passed" : "failed";
    out += "\t";
    out += std::to_string(static_cast<unsigned long long>(
        std::llround(stats.seconds * 1000.0)));
    out += "\n";

    std::fwrite(out.data(), 1, out.size(), stdout);
    std::fwrite(marker.data(), 1, marker.size(), stderr);
    std::fflush(stdout);
}

void VscodeResultsReporter::test_case_exception(const doctest::TestCaseException&) {}
void VscodeResultsReporter::test_case_skipped(const doctest::TestCaseData&) {}
void VscodeResultsReporter::subcase_start(const doctest::SubcaseSignature&) {}
void VscodeResultsReporter::subcase_end() {}
void VscodeResultsReporter::log_assert(const doctest::AssertData&) {}
void VscodeResultsReporter::log_message(const doctest::MessageData&) {}

DOCTEST_REGISTER_REPORTER("vscode-results", 0, VscodeResultsReporter);
