// Applying recorded snapshot updates at the end of a run.
//
// The rewrites cannot happen inside compare(): one update run must fix every
// snapshot in the suite, so the whole run has to finish recording before any
// file is touched. That means a hook that fires once, after the last test.
//
// A doctest reporter is that hook, and it is the reason this needs no change to
// the shared runner (buck/module_test_main.cc): a reporter is registered by a
// static initializer in this module's library, so any test binary that links
// the snapshot module gets the behaviour, and one that does not is unaffected.
// The alternative -- teaching run::execute about snapshots -- would put a
// module's concern into infrastructure shared by every module.
//
// Registered as a *listener* rather than a named reporter, so it is always
// active and does not displace the console output or the Buck2 results
// reporter.

#include <cstdio>
#include <cstdlib>

#include <doctest/doctest.h>

#include "snapshot/snapshot.h"

namespace {

struct SnapshotFlushListener : doctest::IReporter {
    explicit SnapshotFlushListener(const doctest::ContextOptions&) {}

    void test_run_end(const doctest::TestRunStats&) override {
        // No mode check: compare() decides what to record, so this fires for a
        // SNAPSHOT_UPDATE run and for individual .update() markers alike, and
        // does nothing when neither recorded anything.
        const std::string errors = snapshot_testing::flush_updates();
        if (errors.empty()) return;

        // A failed rewrite must not be a warning in a log. The run is already
        // failing (compare() returns false in update mode), but a *partial*
        // rewrite is worse than none: some snapshots in the file now hold new
        // values and some hold stale ones, and the next run would record a
        // different set. Aborting here makes that state impossible to mistake
        // for success.
        std::fprintf(stderr, "\nsnapshot: update failed:\n%s", errors.c_str());
        std::abort();
    }

    // The rest of the reporter interface: a listener observes, and everything
    // this one cares about is the end of the run.
    void report_query(const doctest::QueryData&) override {}
    void test_run_start() override {}
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

DOCTEST_REGISTER_LISTENER("snapshot_flush", /*priority=*/0, SnapshotFlushListener);

}  // namespace
