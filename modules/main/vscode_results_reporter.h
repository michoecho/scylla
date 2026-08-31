#pragma once

#include "doctest/doctest.h"

// Machine-readable doctest output consumed by the VS Code Buck2 test
// executor. Specialized reporters can derive from this class when they need
// to add setup around the same per-case output protocol.
class VscodeResultsReporter : public doctest::IReporter {
public:
    explicit VscodeResultsReporter(const doctest::ContextOptions& options);

    void report_query(const doctest::QueryData&) override;
    void test_run_start() override;
    void test_run_end(const doctest::TestRunStats&) override;

    void test_case_start(const doctest::TestCaseData&) override;
    void test_case_reenter(const doctest::TestCaseData&) override;
    void test_case_end(const doctest::CurrentTestCaseStats&) override;

    void test_case_exception(const doctest::TestCaseException&) override;
    void test_case_skipped(const doctest::TestCaseData&) override;
    void subcase_start(const doctest::SubcaseSignature&) override;
    void subcase_end() override;
    void log_assert(const doctest::AssertData&) override;
    void log_message(const doctest::MessageData&) override;

private:
    const doctest::TestCaseData* current_ = nullptr;
};
