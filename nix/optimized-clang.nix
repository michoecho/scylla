# A clang built the way tools/toolchain/optimized_clang.sh builds it for
# Fedora: statically linked, non-PIE, ThinLTO'd, and (optionally) PGO+CSPGO
# optimized against a profile collected by compiling Scylla itself.
#
# The build itself is nixpkgs' own llvm/clang derivations -- only the extra
# cmake flags are ours.  The PGO stages are deliberately *not* chained inside
# Nix: each instrumented compiler is built as its own package, the training
# run happens outside the store (see tools/toolchain/nix/train.sh), and the
# resulting .profdata is fed back in as a plain source file.
{
  pkgs,
  llvmPackages ? pkgs.llvmPackages_22,

  # "plain" -- no PGO at all, just static/non-PIE (+LTO).
  # "ir"    -- IR-instrumented, used to collect the first-pass profile.
  # "cs"    -- context-sensitive-instrumented, applies `profdata` and collects
  #            a second-pass profile on top of it.
  # "final" -- applies `profdata` (the IR+CS merge).
  stage ? "plain",
  profdata ? null,

  lto ? true,

  # clangd/clang-tidy roughly double the build; the instrumented stages have
  # no use for them.
  enableClangToolsExtra ? (stage == "plain" || stage == "final"),

  # Matches the -march the Fedora script picks per architecture.
  march ? null,
}:

let
  inherit (pkgs) lib;
  inherit (pkgs.stdenv) hostPlatform;

  llvmTargetArch =
    if hostPlatform.isx86_64 then "X86"
    else if hostPlatform.isAarch64 then "AArch64"
    else throw "optimized-clang: unsupported platform ${hostPlatform.system}";

  archFlags =
    if march != null then march
    else if hostPlatform.isx86_64 then "-march=x86-64-v3"
    # Based on the same references the Fedora script cites.
    else "-march=armv8.2-a+crc+crypto";

  # Build the compiler with a compiler of the same vintage: ThinLTO has to work
  # across the llvm -> clang derivation boundary, so both halves need the same
  # bitcode producer and a bitcode-aware linker/archiver.
  #
  # llvmPackages.stdenv pairs clang with the *binutils* bintools, whose ar and
  # ranlib cannot index a bitcode archive and whose ld.bfd cannot link one.
  # Swapping in llvmPackages.bintools points ar/ranlib/nm at llvm-ar/llvm-nm
  # and ld at lld, and -- because it is the wrapped bintools, not raw lld on
  # PATH -- the nixpkgs ld-wrapper still gets to inject NIX_LDFLAGS, which is
  # what puts glibc.static and zlib.static on the link line.
  buildStdenv = pkgs.overrideCC llvmPackages.stdenv (llvmPackages.clang.override {
    bintools = llvmPackages.bintools;
  });

  major = lib.versions.major llvmPackages.release_version;

  requireProfdata =
    if profdata != null then profdata
    else throw "optimized-clang: stage \"${stage}\" needs `profdata`";

  stageFlags = {
    plain = [ ];
    ir = [ (lib.cmakeFeature "LLVM_BUILD_INSTRUMENTED" "IR") ];
    cs = [
      (lib.cmakeFeature "LLVM_BUILD_INSTRUMENTED" "CSIR")
      (lib.cmakeFeature "LLVM_PROFDATA_FILE" "${requireProfdata}")
    ];
    final = [ (lib.cmakeFeature "LLVM_PROFDATA_FILE" "${requireProfdata}") ];
  }.${stage};

  commonFlags = [
    (lib.cmakeFeature "LLVM_TARGETS_TO_BUILD" "${llvmTargetArch};WebAssembly")
    (lib.cmakeFeature "LLVM_TARGET_ARCH" llvmTargetArch)

    # Static, non-PIE.  LLVM_ENABLE_PIC=OFF keeps the code out of PIC mode
    # (and, as a side effect, stops the shared libclang/libclang-cpp from
    # being built at all, which is what we want here).
    #
    # LLVM_BUILD_STATIC is only acted on by llvm/CMakeLists.txt, not by the
    # shared HandleLLVMOptions.cmake, so it does nothing whatsoever in
    # nixpkgs' *standalone* clang build -- clang would link dynamically
    # against a non-PIC libLLVM and die on __tls_get_addr.  Setting the
    # linker flag directly is what actually covers both halves; the llvm half
    # then just gets a harmless second `-static`.
    (lib.cmakeBool "LLVM_BUILD_STATIC" true)
    (lib.cmakeFeature "CMAKE_EXE_LINKER_FLAGS" "-static")
    (lib.cmakeBool "LLVM_ENABLE_PIC" false)
    (lib.cmakeBool "CMAKE_SKIP_INSTALL_RPATH" true)

    (lib.cmakeBool "LLVM_INCLUDE_BENCHMARKS" false)
    (lib.cmakeBool "LLVM_INCLUDE_EXAMPLES" false)
    (lib.cmakeBool "LLVM_INCLUDE_TESTS" false)
    (lib.cmakeBool "LLVM_BUILD_TESTS" false)
    (lib.cmakeBool "LLVM_ENABLE_BINDINGS" false)

    # Optional deps clang has no use for are off: each one would otherwise
    # have to exist as a static archive for `-static` to succeed.
    (lib.cmakeBool "LLVM_ENABLE_LIBXML2" false)
    (lib.cmakeBool "LLVM_ENABLE_CURL" false)
    (lib.cmakeBool "LLVM_ENABLE_HTTPLIB" false)
    (lib.cmakeBool "LLVM_ENABLE_FFI" false)
    (lib.cmakeBool "LLVM_ENABLE_LIBEDIT" false)

    # zlib and zstd are *not* optional for us: Scylla compiles with `-gz`
    # (cmake/mode.common.cmake and seastar both add it unconditionally), and
    # a clang without compression support rejects that outright.  FORCE_ON so
    # a failed probe is a build error rather than a compiler that silently
    # cannot build Scylla.  Both have to resolve to static archives, or the
    # `-static` link trips over a shared object.
    (lib.cmakeFeature "LLVM_ENABLE_ZLIB" "FORCE_ON")
    (lib.cmakeBool "ZLIB_USE_STATIC_LIBS" true)
    (lib.cmakeFeature "LLVM_ENABLE_ZSTD" "FORCE_ON")
    (lib.cmakeBool "LLVM_USE_STATIC_ZSTD" true)

    # More value profiling slots than the default 1: the Fedora script raises
    # this so indirect-call promotion has something to work with.
    (lib.cmakeFeature "LLVM_VP_COUNTERS_PER_SITE" "6")

    (lib.cmakeFeature "CMAKE_C_FLAGS" archFlags)
    (lib.cmakeFeature "CMAKE_CXX_FLAGS" archFlags)
  ]
  ++ lib.optionals lto [
    (lib.cmakeFeature "LLVM_ENABLE_LTO" "Thin")
    # The stdenv's ld is already lld; saying so explicitly also buys the
    # --thinlto-cache-dir handling in HandleLLVMOptions.cmake.
    (lib.cmakeFeature "LLVM_USE_LINKER" "lld")
  ]
  ++ stageFlags;

  # nixpkgs' llvm turns its checkPhase on by default; none of it is meaningful
  # for an instrumented or LTO'd build and all of it is expensive.
  #
  # glibc.static is what makes the `-static` above actually link: the default
  # cc-wrapper only puts the shared glibc on the link path, so without it even
  # cmake's first LLVM_LIBSTDCXX_MIN probe fails with `cannot find -lc`.
  staticNoChecks = old: {
    doCheck = false;
    buildInputs = (old.buildInputs or [ ]) ++ [
      pkgs.glibc.static
      # zlib splits its archive into a `static` output; zstd only builds one
      # when asked.
      pkgs.zlib.static
      (pkgs.zstd.override { enableStatic = true; })
    ];
  };

  libllvm = (llvmPackages.libllvm.override {
    stdenv = buildStdenv;
    enableSharedLibraries = false;
    enablePolly = false;
    enablePFM = false;
    enableTerminfo = false;
    devExtraCmakeFlags = commonFlags;
  }).overrideAttrs staticNoChecks;

  libclang = (llvmPackages.libclang.override {
    stdenv = buildStdenv;
    inherit libllvm enableClangToolsExtra;
    devExtraCmakeFlags = commonFlags ++ [
      (lib.cmakeBool "CLANG_DEFAULT_PIE_ON_LINUX" false)
    ];
  }).overrideAttrs staticNoChecks;

  # The same wrapper nixpkgs builds for `llvmPackages.clang` on a gcc host
  # (libstdc++, compiler-rt runtimes), pointed at our clang instead.
  clang = pkgs.wrapCCWith {
    cc = libclang;
    libcxx = null;
    extraPackages = [ llvmPackages.compiler-rt ];
    extraBuildCommands = ''
      rsrc="$out/resource-root"
      mkdir "$rsrc"
      echo "-resource-dir=$rsrc" >> $out/nix-support/cc-cflags
      ln -s "${lib.getLib libclang}/lib/clang/${major}/include" "$rsrc"
      ln -s "${llvmPackages.compiler-rt.out}/lib" "$rsrc/lib"
      ln -s "${llvmPackages.compiler-rt.out}/share" "$rsrc/share"
    '';
  };
in
clang // {
  inherit libllvm libclang;
  unwrapped = libclang;
}
