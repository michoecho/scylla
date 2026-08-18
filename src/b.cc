#include <stdexcept>

#include <doctest/doctest.h>
#include <hegel/hegel.h>
#include <hegel/settings.h>

namespace gs = hegel::generators;

static hegel::Settings hegel_settings() {
    hegel::Settings settings;
    settings.test_cases = 100;
    settings.verbosity = hegel::Verbosity::Quiet;
    settings.derandomize = true;
    settings.print_blob = false;
    settings.phases = {hegel::Phase::Generate};
    return settings;
}

TEST_CASE("Static-library test") {
    CHECK(true);
}

TEST_CASE("Hegel test from static library") {
    hegel::test(
        [](hegel::TestCase& tc) {
            HEGEL_DRAW(tc, value,
                       gs::integers<int>({.min_value = 0, .max_value = 10}));
            if (value < 0 || value > 10)
                throw std::runtime_error("generated value is out of range");
        },
        {"generated_value_is_in_range", __FILE__, __LINE__},
        hegel_settings());
}
