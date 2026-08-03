#include <doctest/doctest.h>

#include "module_c/c.h"

TEST_CASE("module_c::combine sums twice and square") {
    CHECK(module_c::combine(0) == 0);
    CHECK(module_c::combine(3) == 15); // 6 + 9
    CHECK(module_c::combine(-4) == 8); // -8 + 16
}
