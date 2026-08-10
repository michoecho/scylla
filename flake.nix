{
  description = "Template for C++ projects";

  inputs = {
    nixpkgs-stable.url = "github:NixOS/nixpkgs/nixos-26.05";
    nixpkgs-unstable.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs-stable, nixpkgs-unstable }:
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

      # The Hegel stack, built entirely from source. Upstream's own CMake and
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
          # exactly this set. `vscode-with-extensions` keeps them in their own
          # derivation and only references it, so there is no extensions
          # directory inside `withExts` itself to read.
          extensions = (with pkgs-unstable.vscode-extensions; [
              anthropic.claude-code
              eamodio.gitlens
              ms-vscode.cpptools
              # Upstream CMake Tools, and the default until the fork in
              # tools/vscode-cmake-tools (which adds per-test coverage) is
              # installed over it by that directory's build-and-install. The
              # fork is deliberately *not* built here: its dependencies come
              # from yarn at dev time rather than from Nix, so baking it in
              # would make `nix build .#code` need network access. The wrapper
              # below is what makes overriding it possible at all.
              ms-vscode.cmake-tools
              ms-python.python
              ms-python.vscode-pylance
              ms-python.debugpy
              llvm-vs-code-extensions.vscode-clangd
              vadimcn.vscode-lldb
            ])
            # Not packaged in nixpkgs, so pull it straight from the marketplace.
            ++ pkgs-unstable.vscode-utils.extensionsFromVscodeMarketplace [
              {
                name = "chatgpt";
                publisher = "openai";
                version = "26.727.40816";
                sha256 = "0ql0a58b69j2806s5m85gc21v5ksxibxvks5yf7q462s3mwflihd";
              }
            ];

          withExts = pkgs-unstable.vscode-with-extensions.override {
            vscode = pkgs-unstable.vscode;
            vscodeExtensions = extensions;
          };

          # The same extensions as one directory, which is what the wrapper
          # copies from. Built here rather than dug out of `withExts` so the
          # path is a Nix reference and not a guess at its internal layout.
          extensionsDir = pkgs-unstable.symlinkJoin {
            name = "vscode-extensions-dir";
            paths = extensions;
          };
        in
        pkgs-unstable.symlinkJoin {
          name = "code-project-local";
          paths = [ withExts ];
          nativeBuildInputs = [ pkgs-unstable.makeWrapper ];
          # Rewrite the `code` entrypoint so it injects a project-local
          # --user-data-dir and --extensions-dir computed at launch time. Users
          # can still override either explicitly; VS Code honours the last one
          # given, and ours are prepended, so a user-supplied one wins.
          #
          # The extensions directory has to be writable, and the one baked into
          # `withExts` is a read-only store path. Making it project-local is
          # what lets a locally built extension override a packaged one -- in
          # particular the CMake Tools fork in tools/vscode-cmake-tools, which
          # shares upstream's identity and so replaces it once installed here.
          # Without this the fork would sit on disk unused, since the store
          # path always wins.
          #
          # Seeding copies rather than symlinks is deliberate: VS Code writes
          # inside extension directories, and the store is read-only. Each
          # extension is copied once and then left alone, so an installed
          # override is never clobbered on a later launch.
          postBuild = ''
            rm "$out/bin/code"
            makeWrapper "${withExts}/bin/code" "$out/bin/code" \
              --run '
                root="$(git rev-parse --show-toplevel 2>/dev/null || echo "$PWD")"
                udd="$root/.local/vscode"
                extdir="$udd/extensions"
                mkdir -p "$extdir"
                for ext in ${extensionsDir}/share/vscode/extensions/*/; do
                  name="$(basename "$ext")"
                  # Any directory whose name starts with the extension id
                  # counts as present: `--install-extension` appends a version
                  # suffix (ms-vscode.cmake-tools-1.13.0), and copying the
                  # packaged one back in would shadow the installed override.
                  # The glob is matched with `set --` in a subshell rather than
                  # compgen, which this non-interactive bash does not provide.
                  if ! ( set -- "$extdir/$name" "$extdir/$name"-*; [ -e "$1" ] || [ -e "$2" ] ); then
                    cp -r --no-preserve=mode "$ext" "$extdir/$name"
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
          perf2perfetto = pkgs.callPackage ./nix/perf2perfetto.nix { };

          # Hegel (property-based testing), packaged nixpkgs-style from source.
          # Upstream ships a flake, but it downloads a prebuilt engine .so from
          # a GitHub release; these build the whole stack from source instead.
          # Three packages because they are three separate builds: a Rust
          # cdylib, a CMake library, and the C++ binding that consumes both.
          inherit (hegelPackagesFor pkgs) libhegel reflectcpp hegel-cpp;
        });

      devShells = forAllSystems (system:
        let
          pkgs = pkgsStableFor system;
          pkgs-unstable = pkgsUnstableFor system;
          code = vscodeFor pkgs-unstable;
          llvmPkgs = pkgs.llvmPackages_22;

          # nixpkgs' doctest plus our two extensions to doctest_discover_tests:
          # a TEST_SUBCOMMAND argument, which lets the discovered runner be
          # invoked through a subcommand (`cpp_template test ...`), and a
          # DEF_SOURCE_LINE property on each registered test, which is what the
          # VS Code test explorer reads to make "go to test" work. Upstream
          # ships the patched scripts/cmake/*.cmake into lib/cmake/doctest, so
          # CMakeLists picks the change up via find_package(doctest).
          doctest = pkgs.doctest.overrideAttrs (old: {
            patches = (old.patches or [ ]) ++ [ ./nix/patches/doctest-discover-tests.patch ];
          });

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
          boost = pkgs.boost.overrideAttrs (old: {
            patches = (old.patches or [ ]) ++ [ ./nix/patches/boost-stacktrace-from-exception-ptr.patch ];
          });

          # The `perf script` dlfilter that turns an Intel PT trace into a
          # Perfetto/Fuchsia trace, used by tools/pt-trace. Built from the
          # upstream cargo project; see nix/perf2perfetto.nix.
          perf2perfetto = pkgs.callPackage ./nix/perf2perfetto.nix { };
        in
        {
          default = pkgs.mkShell.override { stdenv = pkgs.overrideCC pkgs.stdenv (pkgs.ccacheWrapper.override { cc = llvmPkgs.clang; }); } {
            packages = with pkgs; [
              aflplusplus
              cli11
              cmake
              doctest
              nanobench
              ninja
              llvmPkgs.clang-tools
              llvmPkgs.llvm
              gdb
              boost.dev
              boost
              # Name resolution backend for Boost.Stacktrace; see CMakeLists.
              libbacktrace
              zstd
              lz4

              # Test runner for the Python tools under tools/, plus the
              # scientific stack the map-lookup cost study analyses its sweep
              # with (modules/playground/map_lookup_report.ipynb). CMake only
              # locates an interpreter (find_package(Python3)); the packages
              # come from here, so no build step ever installs anything.
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
              # propagates reflect-cpp, and its CMake config finds the engine
              # shared library shipped inside its own prefix, so only this one
              # entry is needed for find_package(hegel) to work.
              (hegelPackagesFor pkgs).hegel-cpp

              pkgs-unstable.claude-code
              code
              pkgs-unstable.codex

              # Toolchain for building the forked CMake Tools extension in
              # tools/vscode-cmake-tools (see tools/vscode-cmake-tools/README).
              # Unlike everything else here, its dependencies are *not*
              # vendored through Nix: the fork's build runs `yarn install`
              # against the network into a gitignored node_modules. That is a
              # deliberate exception -- packaging a large TypeScript
              # dependency tree reproducibly is a project of its own, and the
              # extension is a developer tool rather than part of the build.
              nodejs
              yarn

              shader-slang
              vulkan-loader
              vulkan-headers
              vulkan-tools
              vulkan-validation-layers
              vulkan-memory-allocator
              glslang
              freetype
              libxkbcommon
              wayland
              wayland-protocols
              wayland-scanner
              pkg-config
              dbus
              abseil-cpp
              sdl3
              # Instance/device selection and swapchain building for the vulkan
              # module. Packaged in nixpkgs, so no submodule and no local
              # derivation; it ships a CMake config, hence
              # find_package(vk-bootstrap).
              vk-bootstrap
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
        });
    };
}
