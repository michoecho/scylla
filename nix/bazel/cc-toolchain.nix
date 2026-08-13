# The C++ toolchain Bazel builds with.
#
# What rules_nixpkgs wants from this file, when it is passed as `nix_file`, is
# a *wrapped compiler* -- a `stdenv.cc`-shaped derivation. Its cc.nix reads
# `.targetPrefix` off the result and looks for cc, c++, ld, ar, nm, strip and
# friends under `${cc}/bin/`, substituting `false` for anything missing. So
# returning a stdenv (or a bare unwrapped clang) produces a toolchain full of
# `false`, or an evaluation error about the missing attribute.
#
# llvmPackages_22.clang is the wrapper, and it is the same LLVM the CMake
# devShell uses, so both builds compile with the same compiler rather than
# merely with "a clang".
#
# Deliberately not the ccache-wrapped compiler that shell layers on: Bazel has
# its own action cache, and a compiler consulting a second cache underneath it
# makes Bazel's cache-hit accounting describe something other than the work
# actually done.
#
# <nixpkgs> rather than importing ./nixpkgs.nix beside this file: rules_nixpkgs
# copies this file alone into the toolchain's repository, so a relative import
# would resolve to a path that does not exist there. It puts the nixpkgs it
# already resolved -- the @nixpkgs repository, which *is* ./nixpkgs.nix -- on
# the nix-build search path instead. Same pin, reached the way the caller
# intends.
let
  pkgs = import <nixpkgs> { };
in
pkgs.llvmPackages_22.clang
