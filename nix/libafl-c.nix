{ lib
, rustPlatform
, patchelf
}:

# libafl-c — LibAFL's in-process fuzzer, exposed as a C ABI shared library.
#
# This is the local crate in tools/libafl-c, which drives LibAFL's
# InProcessExecutor and lets the C++ side supply the harness. It backs
# TEST_RNG=libafl; see modules/test_rng/include/test_rng/test_rng.h for what
# that backend is and tools/libafl-c/include/libafl_c.h for the boundary.
#
# The structure follows nix/libhegel.nix, which packages the other Rust engine
# in this tree: build from source, install the cdylib and its header, and let a
# consumer link it as an ordinary shared library. What differs is that the crate
# is *ours*, so the LibAFL dependency is vendored from a lockfile we generate
# rather than one upstream ships.
let
  # The LibAFL release tools/libafl-c/Cargo.toml depends on. Kept here as the
  # single source of truth and asserted against the manifest below, the same
  # guard nix/hegel-cpp.nix applies to libhegel's version.
  libaflVersion = "0.15.4";

  cargoToml = builtins.readFile ../tools/libafl-c/Cargo.toml;
  manifestPinsLibafl =
    builtins.match ".*libafl = \\{ version = \"${libaflVersion}\".*" cargoToml != null;
in
assert lib.assertMsg manifestPinsLibafl
  "nix/libafl-c.nix expects LibAFL ${libaflVersion}, which tools/libafl-c/Cargo.toml no longer pins";

rustPlatform.buildRustPackage {
  pname = "libafl-c";
  version = "0.1.0";

  src = lib.cleanSource ../tools/libafl-c;

  # For the SONAME fixup in preInstall below.
  nativeBuildInputs = [ patchelf ];

  # Every dependency resolves to a crates.io release, so Nix vendors straight
  # from the lockfile with no extra hash to maintain -- the same situation
  # nix/libhegel.nix is in.
  #
  # Worth knowing if this ever changes: building LibAFL from its *git workspace*
  # instead drags in a git dependency (tinyinst, via libafl_tinyinst) that Nix
  # refuses to vendor without an explicit outputHashes entry, even though
  # nothing here uses that crate. Depending on the published releases avoids the
  # problem rather than papering over it.
  cargoLock.lockFile = ../tools/libafl-c/Cargo.lock;

  # Two lint problems, both from LibAFL being compiled by a newer rustc than
  # upstream released against, and neither ours to fix in a vendored dependency:
  #
  #   - LibAFL builds with `-D warnings`, so any new rustc lint is a hard error.
  #     --cap-lints=allow demotes warnings in dependencies, which is what cargo
  #     does for crates.io deps by default and does not do for a workspace built
  #     from source.
  #   - unstable_name_collisions specifically: libafl_bolts' AsSlice::as_slice
  #     now collides with an inherent method rustc gained later. Harmless (the
  #     inherent method wins, and the two agree), but it is denied by that same
  #     `-D warnings` and is not covered by --cap-lints in every position.
  RUSTFLAGS = "--cap-lints=allow -A unstable_name_collisions";

  # nixpkgs wraps cargo in cargo-auditable, which runs `cargo metadata` over the
  # whole workspace to record a dependency manifest. That fails here: LibAFL's
  # libafl_targets names a `nix` feature that libafl exposes only through
  # `dep:nix`, which cargo metadata rejects even though the build itself
  # resolves it. The audit data is not worth carrying a patched dependency for.
  auditable = false;

  # No tests in this crate, and `cargo test` would rebuild the LibAFL tree for
  # the test harness.
  doCheck = false;

  # Debug info for the same reason nix/libhegel.nix keeps it: a backtrace or a
  # profile that crosses into the fuzzer should name its Rust frames. Cargo
  # emits it (see the release profile in Cargo.toml) and nixpkgs' fixup phase
  # would otherwise strip it straight back out.
  dontStrip = true;

  # preInstall rather than postInstall, which is the one place this differs from
  # nix/libhegel.nix and is worth stating plainly: buildRustPackage installs
  # binaries but not cdylibs, and its cargoInstallHook clears the build tree on
  # its way out. By postInstall there is no target/ left to copy from, and the
  # `find` silently yields nothing. Running before the hook is what makes the
  # .so reachable at all.
  preInstall = ''
    # Cargo emits liblibafl_c.so (it prefixes `lib` onto the crate's lib name,
    # which is already libafl_c). It is installed under the un-doubled name so a
    # consumer can write -lafl_c, which means the recorded SONAME -- still the
    # doubled one -- has to be patched to match, or the runtime loader would go
    # looking for a file that does not exist.
    #
    # The search is rooted at target/ rather than a fixed path because the build
    # is per-architecture: the artefact lives under target/<triple>/release,
    # and hardcoding the triple would break the moment this is built anywhere
    # else.
    # Installed writable so patchelf can rewrite the SONAME in place, then
    # sealed to the read-only mode the store expects.
    install -Dm755 \
      "$(find target -name 'liblibafl_c.so' -print -quit)" \
      "$out/lib/libafl_c.so"
    patchelf --set-soname libafl_c.so "$out/lib/libafl_c.so"
    chmod 555 "$out/lib/libafl_c.so"

    install -Dm444 include/libafl_c.h "$out/include/libafl_c.h"
  '';

  # cargoInstallHook copies cdylibs into $out/lib under cargo's own name, so
  # without this the prefix ships the same library twice -- once as
  # libafl_c.so (installed above, with the patched SONAME) and once as
  # liblibafl_c.so. Two copies of a fuzzer's coverage map in one prefix is an
  # invitation to link both and wonder why edges go missing, so the duplicate
  # is removed rather than left as a harmless-looking alias.
  postInstall = ''
    rm -f "$out/lib/liblibafl_c.so"
  '';

  meta = {
    description = "LibAFL's in-process fuzzer as a C ABI shared library, for the test_rng libafl backend";
    homepage = "https://github.com/AFLplusplus/LibAFL";
    license = lib.licenses.mit;
    platforms = lib.platforms.linux;
  };
}
