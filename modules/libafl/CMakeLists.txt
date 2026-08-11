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

find_program(CARGO cargo REQUIRED)
# For the SONAME fixup below, which cargo cannot do itself.
find_program(PATCHELF patchelf REQUIRED)

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
set(LIBAFL_VENDOR_DIR "$ENV{LIBAFL_CARGO_VENDOR_DIR}")

set(LIBAFL_RUST_DIR "${CMAKE_CURRENT_SOURCE_DIR}/rust")

# Cargo's target directory, kept inside the CMake build tree so it is per-preset
# and `rm -rf out/build/<preset>` really does clean everything. A shared target/
# in the source tree would leak artifacts between presets built with different
# flags.
set(LIBAFL_CARGO_TARGET_DIR "${CMAKE_CURRENT_BINARY_DIR}/cargo")

# Cargo emits liblibafl_c.so: it prefixes `lib` onto the crate's lib name, which
# is itself libafl_c. The doubled name is what the build tree contains and what
# every rule below refers to; nothing renames it, because unlike an installed
# prefix there is no consumer here writing -lafl_c by hand.
set(LIBAFL_C_SO "${LIBAFL_CARGO_TARGET_DIR}/release/liblibafl_c.so")

# Every input cargo reads, so Ninja knows when to re-run it. Listing the sources
# explicitly (rather than globbing, or depending on the directory) is what makes
# `touch rust/src/lib.rs && ninja` rebuild and an untouched tree not.
set(LIBAFL_RUST_SOURCES
    "${LIBAFL_RUST_DIR}/Cargo.toml"
    "${LIBAFL_RUST_DIR}/Cargo.lock"
    "${LIBAFL_RUST_DIR}/src/lib.rs")

# --offline with a vendored registry, and the source replacement that makes the
# two agree. `cargo vendor` normally writes this stanza into .cargo/config.toml;
# it is passed on the command line instead so the source tree carries no
# generated config and the path can come from the environment.
#
# --release because LibAFL is a fuzzer: a debug build of it is slow enough to
# change what the backend can find within a budget. Debug info is kept via the
# release profile in Cargo.toml, so backtraces still name Rust frames.
add_custom_command(
    OUTPUT "${LIBAFL_C_SO}"
    COMMAND ${CMAKE_COMMAND} -E env
        "CARGO_TARGET_DIR=${LIBAFL_CARGO_TARGET_DIR}"
        "CARGO_HOME=${LIBAFL_CARGO_TARGET_DIR}/home"
        # LibAFL builds with `-D warnings`, so a lint from a rustc newer than
        # upstream released against is a hard error in a dependency we do not
        # own. --cap-lints demotes those to warnings, which is what cargo
        # already does for crates.io dependencies of a non-workspace build.
        "RUSTFLAGS=--cap-lints=allow -A unstable_name_collisions"
        ${CARGO} build
            --release
            --offline
            --manifest-path "${LIBAFL_RUST_DIR}/Cargo.toml"
            --config "source.crates-io.replace-with='vendored-sources'"
            --config "source.vendored-sources.directory='${LIBAFL_VENDOR_DIR}'"
    # Cargo does not record a SONAME in a cdylib, and that is not cosmetic: with
    # none, the linker copies whatever path it was handed into the consumer's
    # DT_NEEDED. Here that path is relative to the build directory, so every
    # test binary asks the loader for "modules/libafl/cargo/release/..." and
    # dies with exit 127 unless it happens to be run from the build root.
    #
    # Stamping a plain filename makes DT_NEEDED a name rather than a path, which
    # is what the RPATH below is then able to resolve.
    COMMAND ${PATCHELF} --set-soname liblibafl_c.so "${LIBAFL_C_SO}"
    DEPENDS ${LIBAFL_RUST_SOURCES}
    COMMENT "Building modules/libafl Rust wrapper (cargo)"
    VERBATIM)

add_custom_target(libafl_rust DEPENDS "${LIBAFL_C_SO}")

# The C++-facing face of all this: an imported library that carries the header
# search path, so a consumer writes target_link_libraries(... libafl) and gets
# both halves.
#
# UNKNOWN rather than SHARED because the file does not exist at configure time
# -- cargo has not run yet -- and SHARED would have CMake stat it.
add_library(libafl UNKNOWN IMPORTED GLOBAL)
set_target_properties(libafl PROPERTIES
    IMPORTED_LOCATION "${LIBAFL_C_SO}"
    INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_CURRENT_SOURCE_DIR}/include"
    # The library lives in the build tree rather than in an installed prefix, so
    # the loader has to be told where to find it at runtime. Without this every
    # test binary links fine and then dies with exit 127 ("not found") the
    # moment ctest runs it: cargo records the SONAME as a bare filename, and the
    # only copy is in a directory no default search path covers.
    #
    # INTERFACE_LINK_OPTIONS rather than a global CMAKE_BUILD_RPATH so the
    # dependency travels with the target that links it.
    INTERFACE_LINK_OPTIONS "-Wl,-rpath,${LIBAFL_CARGO_TARGET_DIR}/release")

# An imported target cannot depend on a custom target, so the ordering edge is
# added to consumers instead: add_dependencies(<consumer> libafl_rust). Doing it
# here, once, means a consumer only has to link.
#
# The .so is also listed as a source-level dependency through IMPORTED_LOCATION,
# so a rebuilt library relinks its consumers even though the custom target
# itself is always considered out of date.
