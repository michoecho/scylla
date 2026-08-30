#include "source_location/source_location.h"

namespace srcloc {

// The one definition of the registry head; out of line for the reason given in
// tracer.cc -- a program that links this into a shared library gets one registry
// without needing -Wl,--export-dynamic on the final link.
location_table*& location_tables() {
    static location_table* head = nullptr;
    return head;
}

std::vector<location> locations() {
    std::vector<location> out;
    for (const location_table* table = location_tables(); table != nullptr; table = table->next) {
        for (const entry* const* at = table->start; at != table->stop; ++at) {
            out.push_back(location::at(*at));
        }
    }
    return out;
}

}  // namespace srcloc
