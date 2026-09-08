{
  description = "Template for C++ projects";

  inputs = {
    nixpkgs-stable.url = "github:NixOS/nixpkgs/nixos-26.05";
    nixpkgs-unstable.url = "github:NixOS/nixpkgs/nixos-unstable";

    # The clang packages are pinned to an immutable revision rather than to
    # the nixos-26.05 branch, so that moving nixpkgs-stable forward does not
    # invalidate the optimized compiler.  Rebuilding it costs three full
    # LLVM+clang builds and two full Scylla builds (the PGO training runs),
    # and the profiles in nix/profiles/ are tied to this LLVM version anyway.
    # This revision is where nixpkgs-stable happened to sit when the profiles
    # were collected; bump it deliberately, and re-run
    # tools/toolchain/nix/train.sh when you do.
    nixpkgs-clang.url = "github:NixOS/nixpkgs/a9e6d84f9c2f9012f5fe7d964a7851352300e61a";
  };

  outputs = { self, nixpkgs-stable, nixpkgs-unstable, nixpkgs-clang, ... }:
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
      pkgsClangFor = system: import nixpkgs-clang {
        inherit system;
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
              #ms-vscode.cmake-tools
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
          # `withExts` is a read-only store path. Making it project-local lets
          # locally built extensions, such as the Buck2 test extension, be
          # installed without modifying the Nix store.
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
                  # suffix, and copying the packaged one back in would shadow
                  # the installed override.
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
        let
          # Everything clang-related comes from the pinned nixpkgs, not from
          # nixpkgs-stable.
          clangPkgs = pkgsClangFor system;
          mkClang = args:
            import ./nix/optimized-clang.nix ({ pkgs = clangPkgs; } // args);

          # The profiles are produced outside Nix by
          # tools/toolchain/nix/train.sh and handed back in as source files.
          # Note that a flake only sees files git knows about, hence the
          # `git add -N` the script performs.
          profile = name:
            let p = ./nix/profiles + "/${name}";
            in if builtins.pathExists p then p
               else throw ("nix/profiles/${name} is missing; run "
                           + "tools/toolchain/nix/train.sh first");
        in {
          code = vscodeFor (pkgsUnstableFor system);

          # llvm-profdata & friends, matching the instrumented compiler.
          llvm = clangPkgs.llvmPackages_22.libllvm;

          # Stock nixpkgs clang 22, as the baseline to measure against.
          clang-stock = clangPkgs.llvmPackages_22.clang;

          # Staging posts on the way to `clang-optimized`; see
          # nix/optimized-clang.nix and tools/toolchain/nix/train.sh.
          clang-static = mkClang { stage = "plain"; lto = false; enableClangToolsExtra = false; };
          clang-lto = mkClang { stage = "plain"; };
          clang-instrumented-ir = mkClang { stage = "ir"; };
          clang-instrumented-cs = mkClang { stage = "cs"; profdata = profile "ir.profdata"; };
          clang-optimized = mkClang { stage = "final"; profdata = profile "combined.profdata"; };
        });

      devShells = forAllSystems (system:
        let
          my_packages = self.packages.${system};
          pkgs = pkgsStableFor system;
          pkgs-unstable = pkgsUnstableFor system;
          code = vscodeFor pkgs-unstable;
          llvmPkgs = pkgs.llvmPackages_22;
          wasmClang = llvmPkgs.clang-unwrapped;
          antlr3Patched = pkgs.antlr3.overrideAttrs (old: {
            patches = (old.patches or []) ++ [
              ./tools/antlr3-patches/0008-unconst-cyclicdfa-gcc-14.patch
            ];
          });
          cxxbridge = pkgs.rustPlatform.buildRustPackage rec {
            pname = "cxxbridge-cmd";
            version = "1.0.83";
            src = pkgs.fetchCrate {
              inherit pname version;
              hash = "sha256-+E3YldAQ0lGyMnYoS4qOqwMDlU+U99h1aPDYaKcBUNU=";
            };
            cargoHash = "sha256-akiOZ88fjeE9yOMlXFbe7Z/CbOKrxBwWv6fNg7P5VhE=";
          };
        in
        {
          #default = pkgs.mkShell.override { stdenv = pkgs.overrideCC pkgs.stdenv (pkgs.ccacheWrapper.override { cc = llvmPkgs.clang; }); } {
          #
          # The shell compiler is the LTO+PGO+CSPGO clang from
          # nix/optimized-clang.nix, so `clang`/`clang++` on PATH -- and hence
          # configure.py's defaults -- are the fast ones.  This also moves the
          # shell from the nixpkgs default clang to the pinned clang 22.
          default = pkgs.mkShell.override {
            #stdenv = pkgs.overrideCC pkgs.stdenv (pkgs.ccacheWrapper.override { cc = my_packages.clang-optimized; });
            stdenv = pkgs.overrideCC pkgs.stdenv (pkgs.ccacheWrapper.override { cc = my_packages.clang-stock; });
          } {
            shellHook = ''
              export SCYLLA_WASM_CLANG="${wasmClang}/bin/clang"
              export SCYLLA_NIX_SHELL=1
              export CARGO_HOME="$PWD/build/cargo-home"
              export CXXFLAGS="-isystem ${pkgs.boost188}/include $CXXFLAGS"
              export PATH="${pkgs.binutils}/bin:$PATH"
            '';
            packages = with pkgs; [
              cmake
              python3Packages.pyparsing
              python3Packages.python-magic
              python3Packages.shiv
              python3Packages.cassandra-driver
              python3Packages.pyyaml
              python3Packages.click
              python3Packages.lz4
              boost188
              c-ares
              fmt
              (lz4.overrideAttrs (old: {
                cmakeFlags = (old.cmakeFlags or []) ++ [
                  "-DBUILD_SHARED_LIBS=ON"
                  "-DBUILD_STATIC_LIBS=ON"
                ];
              }))
              liburing
              hwloc
              lksctp-tools
              xfsprogs
              llvmPkgs.bintools
              binutils
              yaml-cpp
              zlib
              ninja
              pkg-config
              protobuf
              ragel
              valgrind
              openssl
              gnutls
              doxygen
              icu
              antlr3Patched
              openldap
              cpp-jwt
              nlohmann_json
              cryptopp
              cargo
              cxxbridge
              wabt
              binaryen
              rustc
              rapidxml
              libdeflate
              libxcrypt
              snappy
              rapidjson
              xxhash
              (zstd.override { enableStatic = true; })
              jsoncpp
              lua5_4
              p11-kit
              systemd
              lttng-ust
              ccache

              elfutils
              systemtap-sdt.stapBuild
              jq
              zip
              pigz
              dpkg
              debian-devscripts
              rpm
            ];
            hardeningDisable = [ "all" ];
          };
        });
    };
}
