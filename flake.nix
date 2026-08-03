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
          withExts = pkgs-unstable.vscode-with-extensions.override {
            vscode = pkgs-unstable.vscode;
            vscodeExtensions = (with pkgs-unstable.vscode-extensions; [
              anthropic.claude-code
              eamodio.gitlens
              ms-vscode.cpptools
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
          };
        in
        pkgs-unstable.symlinkJoin {
          name = "code-project-local";
          paths = [ withExts ];
          nativeBuildInputs = [ pkgs-unstable.makeWrapper ];
          # Rewrite the `code` entrypoint so it injects a project-local
          # --user-data-dir computed at launch time. Users can still override
          # it explicitly; VS Code honours the last --user-data-dir given, and
          # ours is prepended, so a user-supplied one wins.
          postBuild = ''
            rm "$out/bin/code"
            makeWrapper "${withExts}/bin/code" "$out/bin/code" \
              --run '
                root="$(git rev-parse --show-toplevel 2>/dev/null || echo "$PWD")"
                udd="$root/.local/vscode"
                mkdir -p "$udd"
                set -- --user-data-dir "$udd" "$@"
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
          llvmPkgs = pkgs.llvmPackages;

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
          # the patch header and src/exception_hacks.cc.
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

              # Test runner for the Python tools under tools/. CMake only
              # locates an interpreter (find_package(Python3)); the packages
              # come from here, so no build step ever installs anything.
              (python3.withPackages (ps: [ ps.pytest ]))

              # Property-based testing; see src/hegel_test.cc. hegel-cpp
              # propagates reflect-cpp, and its CMake config finds the engine
              # shared library shipped inside its own prefix, so only this one
              # entry is needed for find_package(hegel) to work.
              (hegelPackagesFor pkgs).hegel-cpp

              pkgs-unstable.claude-code
              code
              codex
            ];

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
