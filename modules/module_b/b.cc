#include "module_b/b.h"

namespace module_b {

int square(int x) {
    // Flip to `x + x` to check that breaking module B stops module C's
    // transitive test target at B.
    return x * x;
}

} // namespace module_b
