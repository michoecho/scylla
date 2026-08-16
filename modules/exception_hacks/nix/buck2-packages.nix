{ pkgs, boostPatch }:

let
  llvmPkgs = pkgs.llvmPackages_22;
  boost = pkgs.boost.overrideAttrs (old: {
    patches = (old.patches or [ ]) ++ [ boostPatch ];
  });
in
{
  buck2-cxx = llvmPkgs.stdenv.cc;
  buck2-python = pkgs.python3;

  buck2-doctest-headers = pkgs.symlinkJoin {
    name = "buck2-doctest-headers";
    paths = [ "${pkgs.doctest}/include" ];
  };
  buck2-boost-headers = pkgs.symlinkJoin {
    name = "buck2-boost-headers";
    paths = [ "${boost.dev}/include" ];
  };
  buck2-backtrace-headers = pkgs.symlinkJoin {
    name = "buck2-backtrace-headers";
    paths = [ "${pkgs.libbacktrace}/include" ];
  };

  # File-shaped outputs let prebuilt_cxx_library consume the exact shared
  # objects, while the header outputs above remain directory artifacts. Buck
  # stages them under their unversioned names, so make their ELF SONAMEs agree
  # with those staged names as well.
  buck2-boost-stacktrace-from-exception =
    pkgs.runCommand "buck2-boost-stacktrace-from-exception.so"
      {
        nativeBuildInputs = [ pkgs.patchelf ];
      }
      ''
        cp ${boost}/lib/libboost_stacktrace_from_exception.so "$out"
        chmod +w "$out"
        patchelf --set-soname libboost_stacktrace_from_exception.so "$out"
      '';
  buck2-backtrace =
    pkgs.runCommand "buck2-backtrace.so"
      {
        nativeBuildInputs = [ pkgs.patchelf ];
      }
      ''
        cp ${pkgs.libbacktrace}/lib/libbacktrace.so "$out"
        chmod +w "$out"
        patchelf --set-soname libbacktrace.so "$out"
      '';
}
