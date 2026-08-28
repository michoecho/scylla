// A shared library that owns static keys, linked into the test at build time.
//
// Its keys and its branch sites live here, so the loader gives it its own
// __jump_table section, bracketed by its own __start/__stop symbols. That is
// the userspace analogue of a kernel module's jump table, and the thing the
// core has to notice exists.
//
// Everything the test observes goes through extern "C" entry points, so the
// only way it can reach this table is by running code that lives in this DSO.

#include "static_keys/static_keys.h"
#include "static_keys_test/shared_key.h"

// Deliberately at file scope rather than in an anonymous namespace: a key in an
// anonymous namespace is internal and would be non-preemptible for free, which
// would hide whether DEFINE_STATIC_KEY_* is itself safe to use in a DSO.
DEFINE_STATIC_KEY_FALSE(linked_false_unlikely);
DEFINE_STATIC_KEY_TRUE(linked_true_likely);

extern "C" {

[[gnu::noinline]] int sk_linked_probe_false_unlikely() {
    if (static_branch_unlikely(&linked_false_unlikely)) {
        return 42;
    }
    return 7;
}

[[gnu::noinline]] int sk_linked_probe_true_likely() {
    if (static_branch_likely(&linked_true_likely)) {
        return 42;
    }
    return 7;
}

void sk_linked_enable_false_unlikely() {
    static_keys::static_key_enable(&linked_false_unlikely.key);
}

void sk_linked_disable_false_unlikely() {
    static_keys::static_key_disable(&linked_false_unlikely.key);
}

void sk_linked_disable_true_likely() {
    static_keys::static_key_disable(&linked_true_likely.key);
}

void sk_linked_enable_true_likely() {
    static_keys::static_key_enable(&linked_true_likely.key);
}

// A branch on a key this library does not own. The key lives in the
// executable; only its address, filled in by the loader, reaches here.
[[gnu::noinline]] int sk_linked_probe_shared() {
    if (static_branch_unlikely(&sk_shared_key)) {
        return 42;
    }
    return 7;
}

// The bracket symbols as *this* DSO sees them. The test compares them against
// its own to confirm the two tables really are distinct.
const void* sk_linked_table_start() {
    return static_keys::__start___jump_table;
}
const void* sk_linked_table_stop() {
    return static_keys::__stop___jump_table;
}

}  // extern "C"
