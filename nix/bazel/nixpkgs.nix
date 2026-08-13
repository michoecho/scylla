# The nixpkgs that the Bazel build provisions its dependencies from.
#
# rules_nixpkgs evaluates this file directly with `nix-build`, outside of the
# flake evaluation -- a repository rule shells out to Nix, and Nix's flake
# machinery is not in that path. So this cannot be `self.inputs.nixpkgs`; it
# has to fetch nixpkgs itself.
#
# What keeps it from becoming a second, independently drifting pin is that it
# reads the revision out of the project's own flake.lock rather than naming
# one. `nix develop` and `bazel build` therefore resolve to the same nixpkgs
# by construction: bumping the flake bumps both, and there is no second lock
# file to forget to update.
#
# fetchTarball with a sha256 is what makes this pure -- evaluatable inside a
# restricted-eval sandbox, and cached after the first fetch. The hash is
# flake.lock's own narHash, which is exactly the hash of the same tree.
let
  lock = builtins.fromJSON (builtins.readFile ../../flake.lock);
  node = lock.nodes.nixpkgs-stable.locked;

  nixpkgs = builtins.fetchTarball {
    url = "https://github.com/${node.owner}/${node.repo}/archive/${node.rev}.tar.gz";
    sha256 = node.narHash;
  };
in
import nixpkgs
