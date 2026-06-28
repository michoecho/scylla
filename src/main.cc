#include <print>
#include <string>
#include <vector>

#include <CLI/CLI.hpp>

#define DOCTEST_CONFIG_IMPLEMENT
#include "doctest/doctest.h"

TEST_CASE("sanity") {
    CHECK(1 + 1 == 2);
}

int main(int argc, char* argv[]) {
    CLI::App app{"cpp_template"};
    app.require_subcommand(0, 1);

    CLI::App* test = app.add_subcommand("test", "Run the test suite");
    // Everything after `test` is handed to doctest verbatim, untouched by CLI11.
    test->prefix_command();

    CLI11_PARSE(app, argc, argv);

    if (*test) {
        doctest::Context context;
        context.setAsDefaultForAssertsOutOfTestCases();

        // remaining() returns by value; hold it so the c_str() pointers stay valid.
        const std::vector<std::string> doctest_args = test->remaining();
        std::vector<const char*> forwarded;
        forwarded.push_back(argv[0]);
        for (const std::string& arg : doctest_args)
            forwarded.push_back(arg.c_str());
        context.applyCommandLine(static_cast<int>(forwarded.size()), forwarded.data());

        int res = context.run();
        if (context.shouldExit()) // important - query flags (and --exit) rely on the user doing this
            return res;
        return res;
    }

    std::println("Hello, world!");
    return 0;
}
