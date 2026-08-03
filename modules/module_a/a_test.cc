#include <doctest/doctest.h>

#include "module_a/a.h"

TEST_CASE("module_a::twice doubles") {
    CHECK(module_a::twice(0) == 0);
    CHECK(module_a::twice(3) == 6);
    CHECK(module_a::twice(-4) == -8);
}
