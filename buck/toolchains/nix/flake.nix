{
  description = "Toolchain and dependency flake for the buck2 example";

  # Pinned to the exact revision that nixpkgs-stable resolves to in the
  # repository root's flake.lock. buck2.nix shells out to `nix build` from
  # inside build actions, so this flake cannot use `follows` to reach the
  # root flake -- it is evaluated standalone, from this directory. Keeping the
  # rev identical by hand is what makes the compiler and zstd here the same
  # store paths the outer devShell uses.
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/6b316287bae2ee04c9b93c8c858d930fd07d7338";

  outputs =
    { self, nixpkgs }:
    let
      inherit (nixpkgs) lib;
      systems = [ "x86_64-linux" "aarch64-linux" ];
      forAllSystems = fn: lib.genAttrs systems (system: fn nixpkgs.legacyPackages.${system});
    in
    {
      packages = forAllSystems (pkgs: {
        # zstd, consumed by //src:zstd_hello as a prebuilt C++ library. The
        # headers and the library land in separate store paths -- `dev` has
        # include/zstd.h, `out` has lib/ -- so the BUCK file takes one target
        # per output and stitches them back together with
        # prebuilt_cxx_library.
        #
        # The stock package, which builds libzstd.so.1 and no static archive.
        # It is linked dynamically; see the `soname` attribute on the
        # prebuilt_cxx_library in ../../BUCK for what makes the loader find it.
        inherit (pkgs) zstd;

        # Not used by the example's own code. The prelude's internal C++
        # helpers (the dep-file processor, the header-unit stub) are Python
        # scripts, so a python_bootstrap toolchain has to exist before any
        # cxx_binary can be analysed.
        inherit (pkgs) python3;

        # The C/C++ toolchain, shaped the way nix_cxx_toolchain expects: a
        # single package with ar/cc/c++/nm/objcopy/ranlib/strip in $out/bin.
        #
        # cc and c++ have to be *wrappers* rather than symlinks. nixpkgs' cc
        # wrapper is a shell script that reads NIX_CFLAGS_COMPILE, NIX_LDFLAGS
        # and friends from the environment to find libc and the linker; buck2
        # runs its actions in a scrubbed environment, so those variables would
        # be missing and every compile would fail to find <stdio.h>. Capturing
        # them into the wrapper at build time is what makes the compiler
        # self-contained enough to survive that.
        cxx = pkgs.stdenv.mkDerivation {
          name = "buck2-cxx";
          dontUnpack = true;
          dontCheck = true;
          nativeBuildInputs = [ pkgs.makeWrapper ];
          buildPhase = ''
            function capture_env() {
                local -ar vars=(
                    NIX_CC_WRAPPER_TARGET_HOST_
                    NIX_CFLAGS_COMPILE
                    NIX_DONT_SET_RPATH
                    NIX_ENFORCE_NO_NATIVE
                    NIX_HARDENING_ENABLE
                    NIX_IGNORE_LD_THROUGH_GCC
                    NIX_LDFLAGS
                    NIX_NO_SELF_RPATH
                )
                for prefix in "''${vars[@]}"; do
                    for v in $( eval 'echo "''${!'"$prefix"'@}"' ); do
                        echo "--set"
                        echo "$v"
                        echo "''${!v}"
                    done
                done
            }

            mkdir -p "$out/bin"

            for tool in ar nm objcopy ranlib strip; do
                ln -st "$out/bin" "$NIX_CC/bin/$tool"
            done

            mapfile -t < <(capture_env)

            makeWrapper "$NIX_CC/bin/$CC" "$out/bin/cc" "''${MAPFILE[@]}"
            makeWrapper "$NIX_CC/bin/$CXX" "$out/bin/c++" "''${MAPFILE[@]}"
          '';
        };
      });
    };
}
