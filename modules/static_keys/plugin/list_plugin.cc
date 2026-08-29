// A shared library that defines keys, for the listing test to find.
//
// Its own fixture rather than a reuse of linked_plugin: what
// list_static_keys() answers is process-wide, so every key in every loaded
// object lands in that snapshot. Sharing a plugin with the branch tests would
// mean adding a key there -- for a reason having nothing to do with the
// listing -- silently breaks the listing's expected value.
//
// `preferred_linkage = "shared"` in BUCK is what keeps this a real DSO with a
// `__static_keys` section of its own. Linked statically it would merge into the
// executable's section and the listing would have one object in it, which is
// the case that proves nothing.

#include "static_keys/static_keys.h"

// One of each naming form again, so the snapshot shows that the choice is the
// definition's and not the defining object's.
DEFINE_STATIC_KEY_FALSE(list_plugin_default_name);
DEFINE_STATIC_KEY_TRUE(list_plugin_renamed, "static_keys.plugin.named");

extern "C" {

[[gnu::noinline]] int sk_list_plugin_probe_default_name() {
    if (static_branch_unlikely(&list_plugin_default_name)) {
        return 42;
    }
    return 7;
}

[[gnu::noinline]] int sk_list_plugin_probe_renamed() {
    if (static_branch_likely(&list_plugin_renamed)) {
        return 42;
    }
    return 7;
}

}  // extern "C"
