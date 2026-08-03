#include "module_c/c.h"

#include "module_a/a.h"
#include "module_b/b.h"

// Private header, reached unprefixed from the module's own directory.
#include "detail.h"

namespace module_c {

int combine(int x) {
    return detail::scale(module_a::twice(x) + module_b::square(x));
}

} // namespace module_c
