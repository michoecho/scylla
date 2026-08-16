{
  description = "Nix inputs for exception_hacks' folder-local Buck2 build";

  # Keep this folder-local bridge on the same nixpkgs revision as the root
  # flake. Buck cannot reference files above its .buckroot, so it cannot load
  # the root flake directly.
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/9f78f44a87948854445dae0b6bf82b2e87e4efb5";

  outputs =
    { nixpkgs, ... }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
    in
    {
      packages = nixpkgs.lib.genAttrs systems (
        system:
        import ./buck2-packages.nix {
          pkgs = import nixpkgs { inherit system; };
          boostPatch = ./boost-stacktrace-from-exception-ptr.patch;
        }
      );
    };
}
