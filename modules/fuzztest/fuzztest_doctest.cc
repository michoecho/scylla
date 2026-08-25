#include "fuzztest_doctest/fuzztest_doctest.h"

#include <cstdlib>
#include <string>

#include <doctest/doctest.h>

#ifdef BUILD_FUZZTEST
#include <cstdio>

#include "absl/time/time.h"

#include "fuzztest/internal/configuration.h"
#include "fuzztest/internal/coverage.h"
#include "fuzztest/internal/registry.h"
#include "fuzztest/internal/runtime.h"
#endif

namespace fuzztest_doctest {

#ifdef BUILD_FUZZTEST

std::string fuzzing_unusable_reason() {
    if (fuzztest::internal::GetExecutionCoverage() == nullptr)
        return "this binary is not instrumented for coverage (build with the "
               "root//:fuzztest Buck2 modifier)";
    return {};
}

void request_stop() {
    fuzztest::internal::Runtime::instance().SetTerminationRequested();
}

namespace {

// Whether a fuzzing-mode search has already run in this process. See the
// header's note on the termination flag: it is set-only, so the second search
// would stop before its first mutation and report a clean pass.
bool fuzzing_search_has_run = false;

// The doctest half of the assertion bridge.
//
// This is a port of FuzzTest's GTest_EventListener (googletest_adaptor.h), and
// it exists for the reason given at the top of the header: the engine learns
// about failures through signal handlers, so an assertion that merely records
// and returns is invisible to it. doctest's IReporter::log_assert is the same
// hook GoogleTest's OnTestPartResult provides, and the body below is the same
// body, with the same three outcomes.
//
// It is registered unconditionally rather than only around a run, because
// log_assert has no way to know which CHECK belongs to a property body. The
// runtime's own reporter_enabled() flag is the discriminator: it is set by
// FuzzTest only while a run is in progress, so assertions from ordinary test
// cases fall out on the first line and are left entirely alone.
struct FuzzTestBridge : doctest::IReporter {
    explicit FuzzTestBridge(const doctest::ContextOptions&) {}

    void log_assert(const doctest::AssertData& assert_data) override {
        if (!assert_data.m_failed)
            return;

        auto& runtime = fuzztest::internal::Runtime::instance();
        if (!runtime.reporter_enabled())
            return;  // Not inside a fuzz test run; an ordinary CHECK.

        runtime.SetCrashTypeIfUnset("doctest assertion failure");
        if (runtime.run_mode() == fuzztest::RunMode::kFuzz) {
            // In fuzzing mode the engine clears this flag while it searches and
            // sets it again once it has minimized the counterexample and wants
            // the crash. So the abort here is the *final* replay, not the first
            // find, and it is what carries the report and the input out.
            if (runtime.should_terminate_on_non_fatal_failure())
                std::abort();
        } else {
            // Unit-test mode: nothing aborts. Print the report by hand, the way
            // upstream's listener does, and let the loop break on the flag
            // below. The failed CHECK has already marked the doctest case red.
            runtime.PrintReportOnDefaultSink();
        }
        runtime.SetExternalFailureDetected(true);
    }

    // IReporter is a wide interface and only log_assert matters here.
    void report_query(const doctest::QueryData&) override {}
    void test_run_start() override {}
    void test_run_end(const doctest::TestRunStats&) override {}
    void test_case_start(const doctest::TestCaseData&) override {}
    void test_case_reenter(const doctest::TestCaseData&) override {}
    void test_case_end(const doctest::CurrentTestCaseStats&) override {}
    void test_case_exception(const doctest::TestCaseException&) override {}
    void subcase_start(const doctest::SubcaseSignature&) override {}
    void subcase_end() override {}
    void log_message(const doctest::MessageData&) override {}
    void test_case_skipped(const doctest::TestCaseData&) override {}
};

}  // namespace

bool drive(std::string_view full_name, const RunOptions& options) {
    const std::string name(full_name);

    if (options.mode == Mode::Fuzzing) {
        const std::string reason = fuzzing_unusable_reason();
        if (!reason.empty()) {
            MESSAGE("skipping " << name << ": " << reason);
            CHECK_MESSAGE(!options.require_engine, reason);
            return false;
        }
        if (fuzzing_search_has_run) {
            const char* reason_twice =
                "a fuzzing-mode search has already run in this process; the "
                "engine's termination flag cannot be cleared, so a second "
                "search would report a clean pass without searching";
            MESSAGE("skipping " << name << ": " << reason_twice);
            FAIL_CHECK(reason_twice);
            return false;
        }
        fuzzing_search_has_run = true;
    }

    fuzztest::internal::Configuration configuration;
    configuration.binary_identifier = "fuzztest_doctest";
    configuration.fuzz_tests = {name};
    configuration.fuzz_tests_in_current_shard = {name};
    // Left empty deliberately: findings are reported but not persisted. A test
    // that quietly wrote to ~/.cache between runs would stop being reproducible.
    configuration.corpus_database = "";
    configuration.time_limit = absl::Milliseconds(options.time_limit.count());
    configuration.time_budget_type = fuzztest::internal::TimeBudgetType::kPerTest;

    // The run cap has no Configuration field; the engine reads it from the
    // environment and nowhere else. Restore the previous value afterwards --
    // this is a test process that goes on to do other things.
    const char* saved_runs = std::getenv("FUZZTEST_MAX_FUZZING_RUNS");
    const std::string original_runs = saved_runs != nullptr ? saved_runs : "";
    if (options.max_runs != 0)
        ::setenv("FUZZTEST_MAX_FUZZING_RUNS",
                 std::to_string(options.max_runs).c_str(), 1);

    bool ran = false;
    fuzztest::internal::ForEachTest([&](fuzztest::internal::FuzzTest& test) {
        if (ran || test.full_name() != name)
            return;
        ran = true;
        auto fuzzer = test.make();
        if (options.mode == Mode::Fuzzing) {
            // argc/argv are unused by the in-process engine; it parses no flags
            // of its own here.
            fuzzer->RunInFuzzingMode(/*argc=*/nullptr, /*argv=*/nullptr,
                                     configuration);
        } else {
            fuzzer->RunInUnitTestMode(configuration);
        }
    });

    if (options.max_runs != 0) {
        if (saved_runs != nullptr)
            ::setenv("FUZZTEST_MAX_FUZZING_RUNS", original_runs.c_str(), 1);
        else
            ::unsetenv("FUZZTEST_MAX_FUZZING_RUNS");
    }

    // Both entry points return bool, and neither return value means "passed":
    // RunInUnitTestMode returns true even after breaking on a detected failure,
    // and a fuzzing-mode failure aborts the process rather than returning. The
    // doctest case is marked red by the bridge above, or by the CHECK in the
    // property body itself. So the return value is dropped on purpose.
    CHECK_MESSAGE(ran, "no fuzz test registered under '", name, "'");
    return ran;
}

#else  // !BUILD_FUZZTEST

std::string fuzzing_unusable_reason() {
    return "this build does not link the fuzztest cell (build with the "
           "root//:fuzztest Buck2 modifier)";
}

void request_stop() {}

bool drive(std::string_view full_name, const RunOptions& options) {
    const std::string reason = fuzzing_unusable_reason();
    MESSAGE("skipping " << std::string(full_name) << ": " << reason);
    CHECK_MESSAGE(!options.require_engine, reason);
    return false;
}

#endif  // BUILD_FUZZTEST

bool fuzzing_available() { return fuzzing_unusable_reason().empty(); }

}  // namespace fuzztest_doctest

#ifdef BUILD_FUZZTEST
// Priority 1: ahead of the reporters that only print, so the engine is told
// about a failure before anything else reacts to it.
REGISTER_LISTENER("fuzztest_bridge", 1, fuzztest_doctest::FuzzTestBridge);
#endif
