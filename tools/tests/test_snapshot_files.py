import subprocess
from pathlib import Path


TOOL = Path(__file__).parents[1] / "snapshot-files"
UUID = "12345678-1234-4234-8234-123456789abc"
SHA1 = "f32b67c7e26342af42efabc674d441dca0a281c5"


def run(root, *args):
    return subprocess.run([str(TOOL), "--root", str(root), *args],
                          text=True, capture_output=True)


def write_reference(root, uuid=UUID):
    (root / "test.cc").write_text(f'auto value = "{uuid}|{SHA1}"_filesnap;\n')


def test_accepts_matching_reference(tmp_path):
    write_reference(tmp_path)
    path = tmp_path / ".snapshots" / UUID[:2] / f"{UUID}.snap"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"\x00binary\n")
    assert run(tmp_path).returncode == 0


def test_reports_malformed_duplicate_missing_and_orphan(tmp_path):
    write_reference(tmp_path)
    (tmp_path / "other.cc").write_text(
        f'auto duplicate = "{UUID}|{SHA1}"_filesnap;\nauto malformed = "bad"_filesnap;\n')
    orphan = "abcdefab-cdef-4abc-8def-abcdefabcdef"
    path = tmp_path / ".snapshots" / orphan[:2] / f"{orphan}.snap"
    path.parent.mkdir(parents=True)
    path.write_text("orphan")
    result = run(tmp_path)
    assert result.returncode == 1
    assert "duplicate" in result.stderr
    assert "not immediately preceded" in result.stderr
    assert "missing" in result.stderr
    assert "orphan" in result.stderr


def test_ignores_operator_declaration(tmp_path):
    # `operator""_filesnap` is the literal's definition, not a use of it; the
    # empty "" would otherwise be reported as a malformed UUID.
    (tmp_path / "decl.cc").write_text(
        'using snapshot_testing::operator""_filesnap;\n')
    assert run(tmp_path).returncode == 0


def test_reports_filesnap_nested_in_a_string_literal(tmp_path):
    # The line-wise scan cannot tell `\"...\"_filesnap` used as test data from a
    # real reference, so it reports it. The snapshot mechanism's own sources are
    # the only place this arises, and SKIP excludes them by name.
    (tmp_path / "data.cc").write_text(
        f'const std::string s = "x(\\"{UUID}|{SHA1}\\"_filesnap);\\n";\n')
    result = run(tmp_path)
    assert result.returncode == 1
    assert "not immediately preceded" in result.stderr


def test_skips_the_snapshot_mechanisms_own_sources(tmp_path):
    path = tmp_path / "modules" / "snapshot"
    path.mkdir(parents=True)
    (path / "updater_test.cc").write_text(
        f'const std::string s = "x(\\"{UUID}|{SHA1}\\"_filesnap);\\n";\n')
    assert run(tmp_path).returncode == 0


def test_cleanup_only_removes_orphans_when_they_are_the_only_problem(tmp_path):
    orphan = "abcdefab-cdef-4abc-8def-abcdefabcdef"
    path = tmp_path / ".snapshots" / orphan[:2] / f"{orphan}.snap"
    path.parent.mkdir(parents=True)
    path.write_text("orphan")
    assert run(tmp_path, "--cleanup").returncode == 0
    assert not path.exists()
