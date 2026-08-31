{ lib
, stdenv
, source
, protobuf
, python3
}:

# Generate the native C++ protobuf bindings from the same upstream Perfetto
# checkout that defines the trace format. Keeping this as a Nix package makes
# the proto definitions and the protobuf runtime available to Buck through the
# same pkg-config-backed dependency mechanism as the other third-party libs.
stdenv.mkDerivation {
  pname = "perfetto-protos";
  version = "2026-08-28";
  src = source;

  nativeBuildInputs = [ protobuf python3 ];
  propagatedBuildInputs = [ protobuf ];
  enableParallelBuilding = true;

  dontConfigure = true;

  buildPhase = ''
    runHook preBuild
    mkdir -p generated objects

    # trace.proto imports the complete TrackEvent schema. Walk the import
    # closure instead of compiling all of Perfetto's unrelated protos.
    python3 - "$src" > proto-files <<'PY'
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])
pending = ["protos/perfetto/trace/trace.proto"]
seen = set()
while pending:
    name = pending.pop()
    if name in seen or name.startswith("google/protobuf/"):
        continue
    path = root / name
    if not path.exists():
        raise SystemExit(f"missing imported Perfetto proto: {name}")
    seen.add(name)
    for imported in re.findall(r'^import(?: public)? "([^"]+)";',
                               path.read_text(), re.MULTILINE):
        pending.append(imported)

for name in sorted(seen):
    print(name)
PY

    mapfile -t proto_files < proto-files
    protoc -I "$src" -I "${protobuf}/include" \
      --cpp_out=generated "''${proto_files[@]}"

    compile_generated_source() {
      generated_source="$1"
      object_name="''${generated_source#generated/}"
      object_name="''${object_name//\//_}.o"
      "$CXX" -std=c++17 -fPIC -Igenerated -I"${protobuf}/include" \
        -c "$generated_source" -o "objects/$object_name"
    }
    export -f compile_generated_source
    find generated -name '*.pb.cc' -print0 | sort -z | \
      xargs -0 -r -n1 -P "''${NIX_BUILD_CORES:-1}" bash -c \
        'compile_generated_source "$1"' _
    mkdir -p lib
    ar rcs lib/libperfetto-protos.a objects/*.o
    runHook postBuild
  '';

  installPhase = ''
    mkdir -p "$out/include" "$out/lib/pkgconfig"
    cp -r generated/protos "$out/include/"
    install -Dm644 lib/libperfetto-protos.a "$out/lib/libperfetto-protos.a"
    cat > "$out/lib/pkgconfig/perfetto-protos.pc" <<EOF
    prefix=$out
    includedir=$out/include
    libdir=$out/lib

    Name: Perfetto trace protobufs
    Description: Native Perfetto Trace protobufs and generated C++ bindings
    Version: 2026-08-28
    Requires: protobuf
    Libs: -L$out/lib -lperfetto-protos
    Cflags: -I$out/include
    EOF
  '';

  meta = {
    description = "Native Perfetto trace protobufs generated from upstream definitions";
    homepage = "https://perfetto.dev";
    license = lib.licenses.asl20;
  };
}
