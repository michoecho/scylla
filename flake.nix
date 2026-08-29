{
  description = "Template for C++ projects";

  inputs = {
    nixpkgs-stable.url = "github:NixOS/nixpkgs/nixos-26.05";
    nixpkgs-unstable.url = "github:NixOS/nixpkgs/nixos-unstable";
    nativelink.url = "github:TraceMachina/nativelink";
    nativelink.inputs.nixpkgs.follows = "nixpkgs-stable";
  };

  outputs = { self, nixpkgs-stable, nixpkgs-unstable, nativelink, ... }:
    let
      supportedSystems = [ "x86_64-linux" "aarch64-linux" ];
      forAllSystems = f: nixpkgs-stable.lib.genAttrs supportedSystems (system: f system);

      pkgsStableFor = system: import nixpkgs-stable {
        inherit system;
        config.allowUnfree = true;
      };
      pkgsUnstableFor = system: import nixpkgs-unstable {
        inherit system;
        config.allowUnfree = true;
      };
      nativelinkInput = nativelink;
      nativelinkFor = pkgs:
        pkgs.rustPlatform.buildRustPackage {
          pname = "nativelink";
          version = "1.6.4";
          src = nativelinkInput.sourceInfo.outPath;
          cargoLock = {
            lockFile = "${nativelinkInput.sourceInfo.outPath}/Cargo.lock";
            outputHashes = {
              "ginepro-0.9.3" = "sha256-rsFgm5T3b2W3Bd23Bo0/dgCJeV6VaFxdH25JYvvXvTs=";
            };
          };
          cargoBuildFlags = [ "--bin" "nativelink" ];
          doCheck = false;
        };

      # The Hegel stack, built entirely from source. Upstream's own build files and
      # flake fetch a prebuilt libhegel from a GitHub release at configure
      # time; we compile the Rust engine ourselves and hand it to the C++
      # binding via HEGEL_LIBHEGEL_LIBRARY, so no build step downloads
      # anything. reflect-cpp is hegel's one third-party dependency (pulled by
      # FetchContent upstream) and is not in nixpkgs, so it is packaged too.
      #
      # Kept as one function because the three are wired to each other:
      # hegel-cpp asserts it got the exact libhegel/reflect-cpp versions its
      # release pins.
      hegelPackagesFor = pkgs: rec {
        libhegel = pkgs.callPackage ./nix/libhegel.nix { };
        reflectcpp = pkgs.callPackage ./nix/reflectcpp.nix { };
        hegel-cpp = pkgs.callPackage ./nix/hegel-cpp.nix {
          inherit libhegel reflectcpp;
        };
      };

      boostFor = pkgs:
        pkgs.boost.overrideAttrs (old: {
          patches = (old.patches or [ ]) ++ [ ./nix/patches/boost-stacktrace-from-exception-ptr.patch ];
        });


      # FuzzTest's C++ dependencies, rebuilt at this project's language standard.
      # Abseil's headers key several ABI decisions off the standard they were
      # compiled under, and re2 links Abseil, so the two have to agree -- hence
      # the `re2.override` rather than two independently-pinned packages.
      #
      # Abseil installs ~214 fine-grained `.pc` files (absl_strings,
      # absl_flat_hash_map, ...) and no umbrella, which would otherwise force
      # every consumer to enumerate the modules it needs. `absl-all.pc` is
      # synthesised here instead: a `.pc` whose `Requires:` names every module
      # Abseil shipped, so buck asks for one module and pkg-config does the
      # topological ordering of the ~214 archives itself.
      fuzztestDepsFor = pkgs: rec {
        # Pinned to the release FuzzTest's MODULE.bazel declares. nixpkgs is on
        # 20260107.1, which predates `absl/random/mocking_access.h` -- a header
        # FuzzTest's `fuzzing_bit_gen` includes, and which `core_domains_impl`
        # depends on, so the skew is not optional to resolve.
        abseil-cpp = (pkgs.abseil-cpp_202601.override {
          cxxStandard = "23";
        }).overrideAttrs (old: rec {
          version = "20260526.0";
          src = pkgs.fetchFromGitHub {
            owner = "abseil";
            repo = "abseil-cpp";
            rev = version;
            hash = "sha256-O9ClnGm4WSTX3g1Q2VYTMhUtGG52XBwxzgHtWW9WSG0=";
          };
        });

        # ICU switched off, which is both a want and a need.
        #
        # It only buys RE2's `\p{...}` Unicode property classes, which nothing
        # here uses. And nixpkgs' icu4c sets `libdir` in its `.pc` to the *dev*
        # output, which holds pkg-config metadata and no libraries at all -- so
        # `pkg-config --libs --static re2` hands back a `-L` into that output
        # next to the `-licuuc -licudata` it cannot satisfy, and the link fails
        # with "cannot find -licuuc". Dropping the dependency removes both the
        # `Requires: icu-uc` from re2.pc and the reason to care.
        re2 = (pkgs.re2.override { abseil-cpp = abseil-cpp; }).overrideAttrs (old: {
          propagatedBuildInputs = [ abseil-cpp ];
          cmakeFlags =
            (builtins.filter
              (flag: !(pkgs.lib.hasInfix "RE2_USE_ICU" flag))
              (old.cmakeFlags or [ ]))
            ++ [ (pkgs.lib.cmakeBool "RE2_USE_ICU" false) ];
        });

        # Only the pkgconfig directory is assembled; the `.pc` files carry
        # absolute store paths for includedir/libdir, so the headers and
        # archives are found without joining those trees too.
        absl = pkgs.runCommand "abseil-cpp-all"
          { nativeBuildInputs = [ pkgs.pkg-config ]; } ''
          mkdir -p "$out/lib/pkgconfig"
          ln -st "$out/lib/pkgconfig" ${abseil-cpp.dev}/lib/pkgconfig/*.pc
          export PKG_CONFIG_PATH="$out/lib/pkgconfig"

          # Each module is kept only if pkg-config can actually resolve it.
          # Abseil installs `.pc` files for a few test-only helpers whose own
          # `Requires:` name modules it does not install (absl_test_instance_tracker,
          # for one), and a single unresolvable entry fails the whole query. Probing
          # rather than hardcoding an exclusion list keeps this correct across
          # Abseil releases.
          modules=""
          for pc in "$out"/lib/pkgconfig/*.pc; do
            module=$(basename "$pc" .pc)
            if pkg-config --libs "$module" >/dev/null 2>&1; then
              modules="''${modules:+$modules, }$module"
            fi
          done

          cat > "$out/lib/pkgconfig/absl-all.pc" <<EOF
          Name: absl-all
          Description: Every resolvable Abseil module, as one pkg-config module
          Version: ${abseil-cpp.version}
          Requires: $modules
          EOF
          sed -i 's/^ *//' "$out/lib/pkgconfig/absl-all.pc"
        '';
      };

      # Buck needs one prefix containing both Boost's headers and its compiled
      # stacktrace library. Nix keeps those in separate outputs, while the
      # Buck2 dev shell consumes them separately.
      boostBundleFor = pkgs:
        let boost = boostFor pkgs;
        in pkgs.symlinkJoin {
          name = "boost-buck";
          paths = [ boost boost.dev ];
        };

      # VS Code pre-loaded with the extensions this project needs, built from
      # Nix so it's reproducible and identical inside and outside the sandbox.
      # cpptools is unfree (allowUnfree is set on pkgsUnstableFor).
      #
      # The result is additionally wrapped to keep its user-data-dir (settings,
      # state, and the singleton lock/socket) project-local: at runtime it
      # resolves to <git-root>/.local/vscode (or $PWD when not in a repo).
      # This means each project gets its own isolated VS Code instance, so the
      # sandboxed editor never collides with a host instance's lock/socket, and
      # `nix run .#code` behaves the same way outside the sandbox.
      vscodeFor = pkgs-unstable:
        let
          # Bound separately so the wrapper below can seed a writable copy of
          # exactly this set into the project-local extensions directory.
          extensions = (with pkgs-unstable.vscode-extensions; [
              anthropic.claude-code
              eamodio.gitlens
              ms-vscode.cpptools
              ms-python.python
              ms-python.vscode-pylance
              ms-python.debugpy
              # Nix language support: syntax, formatting, and (once nixd or
              # nil is on PATH) an LSP for this flake and nix/*.nix.
              jnoortheen.nix-ide
              llvm-vs-code-extensions.vscode-clangd
              vadimcn.vscode-lldb
            ])
            # Not packaged in nixpkgs, so pull it straight from the marketplace.
            ++ pkgs-unstable.vscode-utils.extensionsFromVscodeMarketplace [
              {
                name = "chatgpt";
                publisher = "openai";
                version = "26.5818.61809";
                sha256 = "sha256-1/dinrtnp1WigzDzp1rBeO9QOSsQM/xuFvj66xY1Ngg=";
              }
            ];

          # The extensions as one directory, which is what the wrapper copies
          # from. Built here so the path is a Nix reference rather than a guess
          # at another derivation's internal layout.
          extensionsDir = pkgs-unstable.symlinkJoin {
            name = "vscode-extensions-dir";
            paths = extensions;
          };
        in
        pkgs-unstable.symlinkJoin {
          name = "code-project-local";
          # Plain VS Code, not `vscode-with-extensions`. That wrapper appends
          # its own read-only `--extensions-dir` in the Nix store, and since
          # VS Code honours the last such option it would win over ours, and
          # the editor would also warn that extensions-dir was given twice.
          # The extensions are seeded into the project-local directory below,
          # so this build only needs the editor itself.
          paths = [ pkgs-unstable.vscode ];
          nativeBuildInputs = [ pkgs-unstable.makeWrapper ];
          # `nix run` resolves the derivation name unless told otherwise, and
          # this one installs its entrypoint as `code`.
          meta.mainProgram = "code";
          # Rewrite the `code` entrypoint so it injects a project-local
          # --user-data-dir and --extensions-dir computed at launch time. Users
          # can still override either explicitly; VS Code honours the last one
          # given, and ours are prepended, so a user-supplied one wins.
          #
          # The extensions directory has to be writable, and anything in the
          # Nix store is read-only. Making it project-local lets locally built
          # extensions, such as the Buck2 test extension, be installed without
          # modifying the Nix store.
          #
          # Seeding copies rather than symlinks is deliberate: VS Code writes
          # inside extension directories, and the store is read-only. Each
          # extension is copied once and then left alone, so an installed
          # override is never clobbered on a later launch.
          postBuild = ''
            rm "$out/bin/code"
            makeWrapper "${pkgs-unstable.vscode}/bin/code" "$out/bin/code" \
              --run '
                root="$(git rev-parse --show-toplevel 2>/dev/null || echo "$PWD")"
                udd="$root/.local/vscode"
                extdir="$udd/extensions"
                mkdir -p "$extdir"
                for ext in ${extensionsDir}/share/vscode/extensions/*/; do
                  name="$(basename "$ext")"
                  # Any directory whose name starts with the extension id
                  # counts as present: `--install-extension` appends a version
                  # suffix, and copying the packaged one back in would shadow
                  # the installed override.
                  # The glob is matched with `set --` in a subshell rather than
                  # compgen, which this non-interactive bash does not provide.
                  if ! ( set -- "$extdir/$name" "$extdir/$name"-*; [ -e "$1" ] || [ -e "$2" ] ); then
                    # Dereference entries produced by symlinkJoin. If extension code
                    # resolves back into /nix/store, VS Code cannot associate
                    # its `require("vscode")` call with the project-local
                    # extension and rejects extension-owned API registrations.
                    cp -rL "$ext" "$extdir/$name"
                  fi
                done
                set -- --user-data-dir "$udd" --extensions-dir "$extdir" "$@"
              '
          '';
        };
    in
    {
      # `nix run .#code` / `nix build .#code` — the same wrapped editor the
      # devShell (and the sandbox) uses.
      packages = forAllSystems (system:
        let pkgs = pkgsStableFor system;
        in {
          code = vscodeFor (pkgsUnstableFor system);
          nativelink = nativelinkFor pkgs;
          perf2perfetto = pkgs.callPackage ./nix/perf2perfetto.nix { };

          # Hegel (property-based testing), packaged nixpkgs-style from source.
          # Upstream ships a flake, but it downloads a prebuilt engine .so from
          # a GitHub release; these build the whole stack from source instead.
          # Three packages because they are three separate builds: a Rust
          # cdylib, a native library, and the C++ binding that consumes both.
          inherit (hegelPackagesFor pkgs) libhegel reflectcpp hegel-cpp;

          # FuzzTest's dependencies. `absl` is the umbrella pkg-config view of
          # abseil-cpp; `re2` is the matching build. Both get a
          # `pkgconfig-<name>` dev shell from the mapAttrs' below, which is how
          # buck's `flake.prebuilt_pkgconfig_library` queries them.
          inherit (fuzztestDepsFor pkgs) absl re2;

          boost = boostBundleFor pkgs;
          libbacktrace = pkgs.libbacktrace;

          doctest = pkgs.doctest;

          fmt = pkgs.fmt;
          # The OpenGL loader and development headers used by SDL's GL
          # context and Dear ImGui's OpenGL3 renderer.
          libGL = pkgs.libglvnd;

          inherit (pkgs)
            python3
            sdl3
            vulkan-loader
            vulkan-headers
            vulkan-memory-allocator
            vk-bootstrap
            shader-slang
            rustc
            ;

          cxx = pkgs.llvmPackages_22.stdenv.mkDerivation {
            name = "buck2-cxx";
            dontUnpack = true;
            dontCheck = true;
            nativeBuildInputs = [ pkgs.makeWrapper ];
            buildPhase = ''
              function capture_env() {
                  # variables to export, all variables with names beginning with one of these are exported
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

              for tool in ar nm objcopy objdump ranlib strip; do
                  ln -st "$out/bin" "$NIX_CC/bin/$tool"
              done

              mapfile -t < <(capture_env)

              makeWrapper "$NIX_CC/bin/$CC" "$out/bin/cc" "''${MAPFILE[@]}"
              makeWrapper "$NIX_CC/bin/$CXX" "$out/bin/c++" "''${MAPFILE[@]}"
            '';
          };
        });

      devShells = forAllSystems (system:
        let
          my_packages = self.packages.${system};
          pkgs = pkgsStableFor system;
          pkgs-unstable = pkgsUnstableFor system;
          code = vscodeFor pkgs-unstable;
          llvmPkgs = pkgs.llvmPackages_22;
          nativelinkPackage = my_packages.nativelink;

          # doctest is consumed directly by Buck2; test discovery and coverage
          # are implemented by the Buck2 VS Code executor.
          doctest = my_packages.doctest;

          # nixpkgs' nanobench with our patch making the perf counters ask for
          # real CPU cycles (PERF_COUNT_HW_CPU_CYCLES) rather than preferring
          # ref cycles, which don't scale with the core's actual clock.
          #
          # Only the header is consumed: src/bench.cc defines
          # ANKERL_NANOBENCH_IMPLEMENT and compiles the implementation itself,
          # so linking the package's libnanobench.a would duplicate symbols.
          nanobench = pkgs.nanobench.overrideAttrs (old: {
            patches = (old.patches or [ ]) ++ [ ./nix/patches/nanobench-real-cpu-cycles.patch ];
          });

          # nixpkgs' boost plus a from_exception library that can look up the
          # trace of any std::exception_ptr, not only of the exception being
          # handled right now. Upstream's lookup already goes through an
          # exception_ptr internally, so the patch mostly just exposes it; see
          # the patch header and modules/exception_hacks/exception_hacks.cc.
          boost = boostFor pkgs;

          # The `perf script` dlfilter that turns an Intel PT trace into a
          # Perfetto/Fuchsia trace, used by tools/pt-trace. Built from the
          # upstream cargo project; see nix/perf2perfetto.nix.
          perf2perfetto = pkgs.callPackage ./nix/perf2perfetto.nix { };

          # Local REAPI uses tiny HTTP/2 messages, so the default TCP behavior
          # can introduce delayed-ACK/Nagle stalls on loopback. These wrappers
          # force TCP_NODELAY without changing the underlying Buck2 or
          # NativeLink binaries.
          tcpNodelay = pkgs.stdenv.mkDerivation {
            pname = "tcp-nodelay-preload";
            version = "0.1.0";
            src = ./nix/tcp-nodelay.c;
            dontUnpack = true;
            dontConfigure = true;
            buildPhase = ''
              $CC -shared -fPIC -O2 -Wall -Wextra \
                -o libtcp-nodelay.so "$src" -ldl
            '';
            installPhase = ''
              install -Dm755 libtcp-nodelay.so "$out/lib/libtcp-nodelay.so"
            '';
          };

          buck2Wrapped = pkgs.writeShellScriptBin "buck2" ''
            preload="${tcpNodelay}/lib/libtcp-nodelay.so"
            if [ -n "''${LD_PRELOAD:-}" ]; then
              preload="$preload:''${LD_PRELOAD}"
            fi
            export LD_PRELOAD="$preload"
            exec ${pkgs-unstable.buck2}/bin/buck2 "$@"
          '';

          nativelinkWrapped = pkgs.writeShellScriptBin "nativelink" ''
            preload="${tcpNodelay}/lib/libtcp-nodelay.so"
            if [ -n "''${LD_PRELOAD:-}" ]; then
              preload="$preload:''${LD_PRELOAD}"
            fi
            export LD_PRELOAD="$preload"
            exec ${nativelinkPackage}/bin/nativelink "$@"
          '';
        in
        {
          default = pkgs.mkShell.override { stdenv = pkgs.overrideCC pkgs.stdenv (pkgs.ccacheWrapper.override { cc = llvmPkgs.clang; }); } {
            packages = with pkgs; [
              aflplusplus
              cli11
              doctest
              nanobench
              llvmPkgs.clang-tools
              llvmPkgs.llvm
              gdb
              boost.dev
              boost
              # Name resolution backend for Boost.Stacktrace.
              libbacktrace
              zstd
              lz4

              # Test runner for the Python tools under tools/, plus the
              # scientific stack the map-lookup cost study analyses its sweep
              # with (modules/playground/map_lookup_report.ipynb).
              (python3.withPackages (ps: [
                ps.pytest
                ps.numpy
                ps.pandas
                ps.scipy
                ps.matplotlib
                ps.jupyter
                ps.nbconvert
              ]))

              # Property-based testing; see src/hegel_test.cc. hegel-cpp
              # propagates reflect-cpp and the engine shared library needed by
              # the Buck2 C++ targets.
              (hegelPackagesFor pkgs).hegel-cpp

              # Rust, for modules/libafl -- the wrapper that puts LibAFL's
              # in-process fuzzer behind a C ABI for TEST_RNG=libafl.
              cargo
              rustc
              # Regenerates third-party/rust/BUCK from Cargo manifests for
              # Buck2 consumption.
              reindeer

              pkgs-unstable.claude-code
              code
              pkgs-unstable.codex

              # Build tools for the Buck2 VS Code extension.
              nodejs
              yarn

              # Language server and formatter behind the nix-ide extension
              # baked into `code` above. Referenced by name from
              # .vscode/settings.json, so they are found on PATH -- which means
              # Nix language support works in an editor launched from this
              # shell, and degrades to syntax highlighting in one that isn't.
              nixd
              nixfmt

              shader-slang
              vulkan-loader
              vulkan-headers
              vulkan-tools
              vulkan-validation-layers
              vulkan-memory-allocator
              glslang
              freetype
              libglvnd
              libxkbcommon
              wayland
              wayland-protocols
              wayland-scanner
              pkg-config
              dbus
              abseil-cpp
              sdl3
              # Instance/device selection and swapchain building for the
              # vulkan module.
              vk-bootstrap

              buck2Wrapped
              nativelinkWrapped

              # Command runner for the recipes in ./justfile.
              just
              # File watcher behind `just watch-compdb`, which stands in for
              # the reconfigure step buck2 does not have: it re-runs the
              # compilation-database BXL when a build file changes.
              watchexec
              # Supervisor for the long-running dev processes; see
              # ./process-compose.yaml and `just dev`.
              process-compose
              spdlog
              fmt
            ];

            # For Vulkan on wayland
            VK_LAYER_PATH = "${pkgs.vulkan-validation-layers}/share/vulkan/explicit_layer.d";
            FIRA_CODE_PATH = "${pkgs.nerd-fonts.fira-code}";

            # Absolute path to the prebuilt dlfilter. tools/pt-trace passes this
            # to `perf script --dlfilter`; there is nothing to build by hand.
            PERF2PERFETTO_DLFILTER = "${perf2perfetto}/lib/libperf2perfetto.so";

            # Keep Python bytecode out of the source tree: without this, running
            # anything in tools/ drops a __pycache__/ next to it. The prefix
            # must be absolute, and the repo root isn't known until the shell
            # starts, hence shellHook rather than a plain attribute.
            shellHook = ''
              export PYTHONPYCACHEPREFIX="''${PYTHONPYCACHEPREFIX:-$(git rev-parse --show-toplevel 2>/dev/null || echo "$PWD")/.cache/pycache}"

            '';

            hardeningDisable = [ "all" ];
          };

        }
        # A shell per package, which is how buck2's
        # `flake.prebuilt_pkgconfig_library` asks a package where its headers
        # and libraries are: it runs `nix develop .#pkgconfig-<name> --command
        # pkg-config ...`. Entering the shell runs nixpkgs' own setup hooks, so
        # PKG_CONFIG_PATH is assembled exactly as it would be for a nix build
        # that depends on the package -- propagated inputs included -- rather
        # than by buck reconstructing that itself.
        #
        # `buildInputs` also picks the `dev` output where a package has one,
        # which is where `.pc` files live, so nothing has to name it.
        // pkgs.lib.mapAttrs'
          (name: package: pkgs.lib.nameValuePair "pkgconfig-${name}" (pkgs.mkShell {
            nativeBuildInputs = [ pkgs.pkg-config ];
            buildInputs = [ package ];
          }))
          my_packages);
    };
}
