# LibAFL's in-process fuzzer, behind a C ABI, backing TEST_RNG=libafl.
#
# The Rust half of this module lives in rust/ and is *our* code, not a
# third-party dependency: it is the wrapper that decides what of LibAFL the C++
# side can see. So it is built here, in the build tree, by the ordinary
# incremental build -- adding a function to the C ABI is an edit and a rebuild,
# the same as it would be in C++.
#
# Only the crates it depends on come from Nix, vendored into a local registry
# from rust/Cargo.lock and pointed at by LIBAFL_CARGO_VENDOR_DIR (see flake.nix).
# That keeps the dependency set pinned and the build offline while leaving the
# part we edit in the tree. The split is deliberate: a derivation around our own
# source would put a `nix build` between every C ABI change and the C++ that
# consumes it.
#
# The cargo-to-CMake plumbing is Corrosion's rather than ours: it produces a
# real target named after the crate (libafl_c), with a build rule, correct
# link/RPATH handling and a per-build-tree cargo target directory. What is left
# here is the project-specific part -- the offline vendor registry, the release
# profile, and the RUSTFLAGS LibAFL needs.
#
# This is not an add_module(): a module is a C++ library with doctest cases and
# dependency-ordered test stamps, and this is a cargo build wrapped in an
# imported target. Its tests are the Rust crate's own, and the C++ tests that
# exercise the backend live in modules/test_rng, which is where the interface
# being tested is.

# Off unless the LibAfl preset asked for it. The whole module is skipped
# otherwise, so an ordinary Debug build neither needs a Rust toolchain nor pays
# for a cargo invocation.
if(NOT BUILD_LIBAFL)
    return()
endif()

find_package(Corrosion REQUIRED)

# The vendored registry, from the devShell. Required rather than optional: with
# no vendor directory cargo would resolve against the network, which would make
# the build non-reproducible and would fail in any sandbox -- both worth an
# error at configure time rather than a confusing fetch failure later.
if(NOT DEFINED ENV{LIBAFL_CARGO_VENDOR_DIR})
    message(FATAL_ERROR
        "LIBAFL_CARGO_VENDOR_DIR is not set, so modules/libafl's Rust "
        "dependencies cannot be resolved offline. It is exported by the "
        "devShell -- run this build inside `nix develop`.")
endif()

# PROFILE release because LibAFL is a fuzzer: a debug build of it is slow enough
# to change what the backend can find within a budget. It has to be said
# explicitly -- Corrosion otherwise follows CMAKE_BUILD_TYPE, and the LibAfl
# preset is a Debug build. Debug info is kept via the release profile in
# Cargo.toml, so backtraces still name Rust frames.
#
# The imported target is named after the crate: libafl_c.
corrosion_import_crate(
    MANIFEST_PATH "${CMAKE_CURRENT_SOURCE_DIR}/rust/Cargo.toml"
    PROFILE release
    CRATE_TYPES cdylib
    LOCKED
    # --offline with a vendored registry, and the source replacement that makes
    # the two agree. `cargo vendor` normally writes this stanza into
    # .cargo/config.toml; it is passed on the command line instead so the source
    # tree carries no generated config and the path can come from the
    # environment.
    FLAGS
        --offline
        --config "source.crates-io.replace-with='vendored-sources'"
        --config "source.vendored-sources.directory='$ENV{LIBAFL_CARGO_VENDOR_DIR}'")

# LibAFL builds with `-D warnings`, so a lint from a rustc newer than upstream
# released against is a hard error in a dependency we do not own. --cap-lints
# demotes those to warnings, which is what cargo already does for crates.io
# dependencies of a non-workspace build.
#
# corrosion_add_target_rustflags rather than _local_: these have to reach the
# dependencies, which is exactly where the lints fire.
corrosion_add_target_rustflags(libafl_c
    "--cap-lints=allow"
    "-Aunstable_name_collisions")

# The C++-facing half: the public header. Corrosion's target carries the library
# and its link requirements, so a consumer writes
# target_link_libraries(... libafl_c) and gets both.
target_include_directories(libafl_c INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}/include")
