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
    in
    {
      devShells = forAllSystems (system:
        let
          pkgs = import nixpkgs-stable {
            inherit system;
            config = {
              allowUnfree = true;
            };
          };
          llvmPkgs = pkgs.llvmPackages;
        in
        {
          default = pkgs.mkShell.override { stdenv = pkgs.overrideCC pkgs.stdenv (pkgs.ccacheWrapper.override { cc = llvmPkgs.clang; }); } {
            packages = with pkgs; [
              cli11
              cmake
              ninja
              llvmPkgs.clang-tools
              llvmPkgs.llvm
              gdb
              boost.dev
              boost
              zstd
              lz4
            ];

            hardeningDisable = [ "all" ];
          };
        });
    };
}
