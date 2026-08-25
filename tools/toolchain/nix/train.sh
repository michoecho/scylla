#!/usr/bin/env bash
#
# Collects the PGO profiles that nix/optimized-clang.nix feeds back into the
# final compiler.  This is the part that deliberately lives outside Nix: it
# builds an instrumented clang from the flake, compiles Scylla with it, and
# merges the raw counters into nix/profiles/.
#
#   tools/toolchain/nix/train.sh ir     # stage 1 -> nix/profiles/ir.profdata
#   tools/toolchain/nix/train.sh cs     # stage 2 -> nix/profiles/combined.profdata
#   tools/toolchain/nix/train.sh all    # both, in order
#
# Afterwards `nix build .#clang-optimized` produces the real thing.
set -ueo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
PROFILES="${REPO}/nix/profiles"
# Kept out of the source tree proper: these are many GiB of throwaway objects.
WORK="${REPO}/build_clang_profile"

usage() { sed -n '2,12p' "${BASH_SOURCE[0]}"; exit 1; }

nix_out() { nix build --no-link --print-out-paths "${REPO}#$1"; }

# Build Scylla with $1 (a wrapped clang), dropping the counters into $2.
#
# --compiler-cache=none matters: a cache hit produces no profile data, so a
# warm ccache would silently gut the training set.
train() {
    local clang="$1" profraw="$2" builddir="${WORK}/build"

    rm -rf "${profraw}" "${builddir}"
    mkdir -p "${profraw}"

    ( cd "${REPO}" && ./configure.py \
        --mode=dev \
        --use-cmake \
        --build-dir="${builddir}" \
        --compiler-cache=none \
        --c-compiler="${clang}/bin/clang" \
        --compiler="${clang}/bin/clang++" \
        --use-profile="" )

    # %8m asks the profile runtime to merge on the fly into a pool of eight
    # files.  Without it a full Scylla build leaves one raw profile per
    # translation unit -- thousands of files and tens of GiB.
    LLVM_PROFILE_FILE="${profraw}/clang-%8m.profraw" \
        ninja -C "${builddir}" compiler-training
}

merge() {
    local out="$1"; shift
    mkdir -p "$(dirname "${out}")"
    "${LLVM_PROFDATA}" merge -output="${out}" "$@"
    ls -lh "${out}"
}

# Flakes only see files git knows about.  --intent-to-add registers the path
# without writing a (very large) blob into .git.
publish() {
    git -C "${REPO}" add --intent-to-add -- "$1"
}

stage_ir() {
    echo "[stage 1] building the IR-instrumented clang"
    local clang; clang="$(nix_out clang-instrumented-ir)"
    echo "[stage 1] training on Scylla with ${clang}"
    train "${clang}" "${WORK}/profraw-ir"
    merge "${PROFILES}/ir.profdata" "${WORK}"/profraw-ir/*.profraw
    publish "${PROFILES}/ir.profdata"
}

stage_cs() {
    echo "[stage 2] building the CS-instrumented clang (applies ir.profdata)"
    local clang; clang="$(nix_out clang-instrumented-cs)"
    echo "[stage 2] training on Scylla with ${clang}"
    train "${clang}" "${WORK}/profraw-cs"
    merge "${WORK}/csir.profdata" "${WORK}"/profraw-cs/*.profraw
    # The context-sensitive counters are an overlay, not a replacement: the
    # final build wants both sets in one file.
    merge "${PROFILES}/combined.profdata" \
        "${PROFILES}/ir.profdata" "${WORK}/csir.profdata"
    publish "${PROFILES}/combined.profdata"
}

[[ $# -eq 1 ]] || usage
LLVM_PROFDATA="$(nix_out llvm)/bin/llvm-profdata"

case "$1" in
    ir)  stage_ir ;;
    cs)  stage_cs ;;
    all) stage_ir; stage_cs ;;
    *)   usage ;;
esac
