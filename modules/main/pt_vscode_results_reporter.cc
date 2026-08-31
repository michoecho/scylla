#include "modules/main/vscode_results_reporter.h"

#include "pt/pt_control.h"

class PtVscodeResultsReporter final : public VscodeResultsReporter {
public:
    using VscodeResultsReporter::VscodeResultsReporter;

    void test_run_start() override {
        pt::resolve();
        pt::enable();
        VscodeResultsReporter::test_run_start();
    }

    void test_run_end(const doctest::TestRunStats& stats) override {
        pt::disable();
        VscodeResultsReporter::test_run_end(stats);
    }
};

DOCTEST_REGISTER_REPORTER("vscode-results-pt", 0, PtVscodeResultsReporter);
