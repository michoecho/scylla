// Private to module_c: lives in the module directory rather than under
// include/, so it is on this module's own include path and on no one else's.
#pragma once

namespace module_c::detail {

inline int scale(int x) { return x; }

} // namespace module_c::detail
