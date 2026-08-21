#define DOCTEST_CONFIG_IMPLEMENT
#include <doctest/doctest.h>

TEST_CASE("Hello") {
    REQUIRE(false);
}

int main(int argc, char* argv[]) {
    doctest::Context context;
    context.applyCommandLine(argc, argv);
    int res = context.run();
    if (context.shouldExit())  // query flags (--list-test-cases, --exit) rely on this
        return res;
    return res;
}
