#include "source_location/source_location.h"

namespace srcloc {

// The one definition of the registry head; out of line for the reason given in
// tracer.cc -- a program that links this into a shared library gets one registry
// without needing -Wl,--export-dynamic on the final link.
location_table*& location_tables() {
    static location_table* head = nullptr;
    return head;
}

std::vector<const entry*> locations() {
    std::vector<const entry*> out;
    for (const location_table* table = location_tables(); table != nullptr; table = table->next) {
        for (const entry* e = table->start; e != table->stop; ++e) {
            out.push_back(e);
        }
    }
    return out;
}

location_index index_of(location loc) {
    const entry* e = loc.get();
    if (e == nullptr) {
        return {nullptr, 0};
    }
    for (const location_table* table = location_tables(); table != nullptr; table = table->next) {
        if (e >= table->start && e < table->stop) {
            return {table, static_cast<std::size_t>(e - table->start)};
        }
    }
    return {nullptr, 0};
}

}  // namespace srcloc
