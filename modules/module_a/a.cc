#include "module_a/a.h"

namespace module_a {

int twice(int x) {
    // Flip to `x * 3` to check that breaking module A stops module C's
    // transitive test target at A.
    return x * 2;
}

} // namespace module_a
