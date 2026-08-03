#include <doctest/doctest.h>

#include "module_b/b.h"

TEST_CASE("module_b::square squares") {
    CHECK(module_b::square(0) == 0);
    CHECK(module_b::square(3) == 9);
    CHECK(module_b::square(-4) == 16);
}
