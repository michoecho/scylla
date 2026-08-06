import subprocess
from pathlib import Path


TOOL = Path(__file__).parents[1] / "snapshot-files"
UUID = "12345678-1234-4234-8234-123456789abc"


def run(root, *args):
    return subprocess.run([str(TOOL), "--root", str(root), *args],
                          text=True, capture_output=True)


def write_reference(root, uuid=UUID):
    (root / "test.cc").write_text(f'auto value = "{uuid}"_filesnap;\n')


def test_accepts_matching_reference(tmp_path):
    write_reference(tmp_path)
    path = tmp_path / ".snapshots" / UUID[:2] / f"{UUID}.snap"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"\x00binary\n")
    assert run(tmp_path).returncode == 0


def test_reports_malformed_duplicate_missing_and_orphan(tmp_path):
    write_reference(tmp_path)
    (tmp_path / "other.cc").write_text(
        f'auto duplicate = "{UUID}"_filesnap;\nauto malformed = "bad"_filesnap;\n')
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


def test_cleanup_only_removes_orphans_when_they_are_the_only_problem(tmp_path):
    orphan = "abcdefab-cdef-4abc-8def-abcdefabcdef"
    path = tmp_path / ".snapshots" / orphan[:2] / f"{orphan}.snap"
    path.parent.mkdir(parents=True)
    path.write_text("orphan")
    assert run(tmp_path, "--cleanup").returncode == 0
    assert not path.exists()
