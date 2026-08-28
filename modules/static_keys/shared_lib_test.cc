// Static keys across shared library boundaries.
//
// A DSO gets its own __jump_table section and its own __start/__stop brackets,
// so the process holds several tables rather than one. These tests cover both
// ways a table can arrive: linked at build time, and dlopen()ed after the
// executable has already initialised its own.

#include <doctest/doctest.h>

#include <cstdlib>

#include <dlfcn.h>

#include "static_keys/static_keys.h"

extern "C" {
int sk_linked_probe_false_unlikely();
int sk_linked_probe_true_likely();
void sk_linked_enable_false_unlikely();
void sk_linked_disable_false_unlikely();
void sk_linked_enable_true_likely();
void sk_linked_disable_true_likely();
const void* sk_linked_table_start();
const void* sk_linked_table_stop();
}

namespace {

// A key belonging to the executable, so the tests can show that patching a
// library's branch leaves the executable's alone.
DEFINE_STATIC_KEY_FALSE(exe_key);

[[gnu::noinline]] int probe_exe_key() {
    if (static_branch_unlikely(&exe_key)) {
        return 42;
    }
    return 7;
}

// Opens the dlopen-only plugin, whose path the build passes in the
// environment. Kept as a guard object so a failing CHECK cannot leak the
// handle into the next test case.
class plugin_handle {
   public:
    plugin_handle() {
        const char* path = std::getenv("STATIC_KEYS_DLOPEN_PLUGIN");
        REQUIRE_MESSAGE(path != nullptr, "STATIC_KEYS_DLOPEN_PLUGIN is not set");
        handle_ = ::dlopen(path, RTLD_NOW | RTLD_LOCAL);
        REQUIRE_MESSAGE(handle_ != nullptr, ::dlerror());
    }
    ~plugin_handle() {
        if (handle_ != nullptr) {
            ::dlclose(handle_);
        }
    }
    plugin_handle(const plugin_handle&) = delete;
    plugin_handle& operator=(const plugin_handle&) = delete;

    void close() {
        REQUIRE(::dlclose(handle_) == 0);
        handle_ = nullptr;
    }

    template <typename Fn>
    Fn sym(const char* name) const {
        void* found = ::dlsym(handle_, name);
        REQUIRE_MESSAGE(found != nullptr, name);
        return reinterpret_cast<Fn>(found);
    }

   private:
    void* handle_ = nullptr;
};

}  // namespace

TEST_CASE("static_keys: a linked library's jump table is not the executable's") {
    static_keys::jump_label_init();

    // Same symbol names, different sections: the linker synthesises a bracket
    // pair per output object, and each is PROTECTED so a DSO's own references
    // bind to its own copy.
    CHECK(sk_linked_table_start() != static_cast<const void*>(static_keys::__start___jump_table));
    CHECK(sk_linked_table_start() != sk_linked_table_stop());
}

TEST_CASE("static_keys: a key defined in a linked library toggles") {
    CHECK(sk_linked_probe_false_unlikely() == 7);

    sk_linked_enable_false_unlikely();
    CHECK(sk_linked_probe_false_unlikely() == 42);

    sk_linked_disable_false_unlikely();
    CHECK(sk_linked_probe_false_unlikely() == 7);
}

TEST_CASE("static_keys: a default-on key in a linked library toggles") {
    CHECK(sk_linked_probe_true_likely() == 42);

    sk_linked_disable_true_likely();
    CHECK(sk_linked_probe_true_likely() == 7);

    sk_linked_enable_true_likely();
    CHECK(sk_linked_probe_true_likely() == 42);
}

TEST_CASE("static_keys: patching a library's key leaves the executable's alone") {
    CHECK(probe_exe_key() == 7);

    sk_linked_enable_false_unlikely();
    CHECK(probe_exe_key() == 7);

    static_keys::static_key_enable(&exe_key.key);
    CHECK(probe_exe_key() == 42);
    CHECK(sk_linked_probe_false_unlikely() == 42);

    static_keys::static_key_disable(&exe_key.key);
    sk_linked_disable_false_unlikely();
    CHECK(probe_exe_key() == 7);
    CHECK(sk_linked_probe_false_unlikely() == 7);
}

TEST_CASE("static_keys: a dlopen()ed library registers and toggles its keys") {
    // Everything below runs long after the executable's own table was
    // initialised, which is what makes this the kernel-module case.
    static_keys::jump_label_init();
    const int before = static_keys::jump_label_table_count();

    plugin_handle plugin;

    // The DSO's constructor registers its table as it loads.
    CHECK(static_keys::jump_label_table_count() == before + 1);

    // Counted from inside the DSO, to confirm both sides share one registry
    // rather than each holding a private copy of the core's state.
    auto count_from_inside = plugin.sym<int (*)()>("sk_dlopen_registered_table_count");
    CHECK(count_from_inside() == before + 1);

    auto probe_false = plugin.sym<int (*)()>("sk_dlopen_probe_false_unlikely");
    auto enable_false = plugin.sym<void (*)()>("sk_dlopen_enable_false_unlikely");
    auto probe_true = plugin.sym<int (*)()>("sk_dlopen_probe_true_unlikely");
    auto disable_true = plugin.sym<void (*)()>("sk_dlopen_disable_true_unlikely");

    CHECK(probe_false() == 7);
    enable_false();
    CHECK(probe_false() == 42);

    CHECK(probe_true() == 42);
    disable_true();
    CHECK(probe_true() == 7);

    plugin.close();

    // Unloading drops the table: nothing may keep pointing into text that is
    // no longer mapped.
    CHECK(static_keys::jump_label_table_count() == before);
}

TEST_CASE("static_keys: a library reloaded after dlclose() starts over") {
    const int before = static_keys::jump_label_table_count();
    {
        plugin_handle plugin;
        auto probe = plugin.sym<int (*)()>("sk_dlopen_probe_false_unlikely");
        auto enable = plugin.sym<void (*)()>("sk_dlopen_enable_false_unlikely");
        CHECK(probe() == 7);
        enable();
        CHECK(probe() == 42);
    }
    CHECK(static_keys::jump_label_table_count() == before);

    // A fresh mapping brings a fresh copy of the key and of the branch site,
    // both back at their compiled-in defaults.
    {
        plugin_handle plugin;
        auto probe = plugin.sym<int (*)()>("sk_dlopen_probe_false_unlikely");
        CHECK(probe() == 7);
    }
    CHECK(static_keys::jump_label_table_count() == before);
}
