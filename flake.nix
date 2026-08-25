{
  description = "Template for C++ projects";

  inputs = {
    nixpkgs-stable.url = "github:NixOS/nixpkgs/nixos-26.05";
    nixpkgs-unstable.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs-stable, nixpkgs-unstable, ... }:
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
        let pkgs = pkgsStableFor system;
        in {
          code = vscodeFor (pkgsUnstableFor system);
        });

      devShells = forAllSystems (system:
        let
          my_packages = self.packages.${system};
          pkgs = pkgsStableFor system;
          pkgs-unstable = pkgsUnstableFor system;
          code = vscodeFor pkgs-unstable;
          llvmPkgs = pkgs.llvmPackages_22;
        in
        {
          default = pkgs.mkShell.override { stdenv = pkgs.overrideCC pkgs.stdenv (pkgs.ccacheWrapper.override { cc = llvmPkgs.clang; }); } {
            packages = with pkgs; [
              cmake
              ninja
              code
              pkgs-unstable.claude-code
              pkgs-unstable.codex
            ];
            hardeningDisable = [ "all" ];
          };
        });
    };
}
