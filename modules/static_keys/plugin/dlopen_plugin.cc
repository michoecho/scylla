// A shared library that is dlopen()ed after the process has already
// initialised its own jump table, and dlclose()d again afterwards.
//
// This is the case that matches a kernel module most closely: the table
// appears after jump_label_init() has run, has to be sorted and validated on
// its own, and has to be forgotten again when the code it points at is
// unmapped. A table registry that is only populated once, at startup, passes
// the linked-plugin test and fails this one.

#include "static_keys/static_keys.h"

DEFINE_STATIC_KEY_FALSE(dlopen_false_unlikely);
DEFINE_STATIC_KEY_TRUE(dlopen_true_unlikely);

extern "C" {

[[gnu::noinline]] int sk_dlopen_probe_false_unlikely() {
    if (static_branch_unlikely(&dlopen_false_unlikely)) {
        return 42;
    }
    return 7;
}

[[gnu::noinline]] int sk_dlopen_probe_true_unlikely() {
    if (static_branch_unlikely(&dlopen_true_unlikely)) {
        return 42;
    }
    return 7;
}

void sk_dlopen_enable_false_unlikely() {
    static_keys::static_key_enable(&dlopen_false_unlikely.key);
}

void sk_dlopen_disable_true_unlikely() {
    static_keys::static_key_disable(&dlopen_true_unlikely.key);
}

const void* sk_dlopen_table_start() {
    return static_keys::__start___jump_table;
}

// How many tables the process-wide registry currently holds. Read from inside
// the DSO so that the count is the one the *shared* core sees.
int sk_dlopen_registered_table_count() {
    return static_keys::jump_label_table_count();
}

}  // extern "C"
