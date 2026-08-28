#pragma once

// A key owned by the test executable that shared libraries branch on: the
// userspace shape of a kernel module branching on a key defined in vmlinux.
//
// Exported, so its address is not known until the loader resolves it -- which
// is the case the key_ref indirection exists for.

#include "static_keys/static_keys.h"

DECLARE_STATIC_KEY_FALSE_EXPORTED(sk_shared_key);
