"""The per-test coverage manifest against the tests CTest actually knows about.

The VS Code extension (tools/vscode-cmake-tools) feeds `FileCoverage.includesTests`
by matching each manifest entry's `test` field against a `TestItem` id, and CMake
Tools gives every discovered test an id equal to its full CTest name. The editor's
`codeCoverageDecorations.ts` *asserts* that every test named in `includesTests`
exists in the controller, so a manifest name that no longer matches a discovered
test is not a silent miss -- it throws in the UI.

That makes the name correspondence the one part of the extension worth testing
without a running editor, and this is what these tests check. They deliberately do
not verify the gutter rendering or the "Filter Coverage by Test" command, which
need a real VS Code instance.
"""

import json
import os
import subprocess
from pathlib import Path

import pytest


def _build_dir():
    """The Coverage-preset build directory, or a skip if it has no reports.

    These tests read a real coverage run rather than constructing a fixture: the
    thing under test is agreement between two independently produced artefacts
    (the manifest and CTest's test list), which a fixture would assume rather
    than check.
    """
    root = Path(__file__).parents[2]
    build = Path(os.environ.get("COVERAGE_BUILD_DIR", root / "out/build/Coverage"))
    if not (build / "coverage/per-test/manifest.json").exists():
        pytest.skip(
            f"no per-test coverage manifest under {build}; "
            "run the Coverage preset with ENABLE_PER_TEST_COVERAGE first")
    return build


@pytest.fixture(scope="module")
def manifest():
    entries = json.loads((_build_dir() / "coverage/per-test/manifest.json").read_text())
    return entries["tests"]


@pytest.fixture(scope="module")
def ctest_names():
    build = _build_dir()
    result = subprocess.run(["ctest", "--show-only=json-v1"],
                            cwd=build, text=True, capture_output=True)
    if result.returncode != 0:
        pytest.fail(f"ctest --show-only failed in {build}: {result.stderr}")
    return {test["name"] for test in json.loads(result.stdout)["tests"]}


def test_every_manifest_test_is_a_real_ctest_test(manifest, ctest_names):
    """The invariant the editor asserts: no unknown names in the manifest."""
    unknown = sorted(e["test"] for e in manifest if e["test"] not in ctest_names)
    assert not unknown, (
        "manifest names no longer registered with CTest; these would throw in "
        f"codeCoverageDecorations.ts: {unknown}")


def test_manifest_entries_have_their_lcov_on_disk(manifest):
    """merge-coverage only lists a test once its report exists beside it."""
    per_test = _build_dir() / "coverage/per-test"
    missing = sorted(e["lcov"] for e in manifest if not (per_test / e["lcov"]).exists())
    assert not missing, f"manifest lists reports that are not on disk: {missing}"


def test_manifest_ids_are_unique(manifest):
    """Two tests sharing an id would silently merge into one report.

    The id is an MD5 of the full CTest name, so a collision means either a
    genuine hash collision or -- far more likely -- the same name registered
    twice.
    """
    ids = [e["id"] for e in manifest]
    assert len(ids) == len(set(ids)), "duplicate ids in the per-test manifest"


def test_some_tests_have_coverage(manifest):
    """Guard against the manifest degenerating to empty and passing vacuously.

    Every other test here is satisfied by an empty manifest, which is exactly
    what a broken profile-collection step would produce.
    """
    assert len(manifest) > 1, f"suspiciously few per-test reports: {len(manifest)}"


def test_untested_ctest_tests_are_tolerated(manifest, ctest_names):
    """Not every CTest test has a report, and the extension must accept that.

    Three of the four `python.*` tests run no instrumented binary, so they
    legitimately never appear in the manifest. This asserts the situation is
    real rather than hypothetical, so the extension's tolerance of it stays
    exercised by the actual data.
    """
    covered = {e["test"] for e in manifest}
    assert ctest_names - covered, (
        "every CTest test has a per-test report; the extension's handling of "
        "reportless tests is no longer covered by real data")


# --- The line -> tests reverse lookup -----------------------------------
#
# `PerTestCoverageIndex.testsForLine` inverts the same reports the forward
# direction reads. What can be checked without an editor is that the inversion
# is answerable at all from real data -- that lines exist which resolve to a
# small, named set of tests, and that the "kept only hit > 0" rule the index
# relies on is what the reports actually support. The quick-pick and the
# reveal-in-explorer half stay interactive.


@pytest.fixture(scope="module")
def lines_to_tests(manifest):
    """(file, 1-based line) -> the tests whose report records a hit there.

    Deliberately a re-implementation rather than a shared helper: it is the
    independent second opinion on what `testsForLine` should return.
    """
    per_test = _build_dir() / "coverage/per-test"
    index = {}
    for entry in manifest:
        source = None
        for line in (per_test / entry["lcov"]).read_text().splitlines():
            if line.startswith("SF:"):
                source = line[3:]
            elif line.startswith("DA:"):
                number, hits = line[3:].split(",")[:2]
                if int(hits) > 0:
                    index.setdefault((source, int(number)), set()).add(entry["test"])
    return index


def test_the_reverse_lookup_has_answers(lines_to_tests):
    """Real data must contain lines attributable to at least one test.

    An empty index would make the command permanently answer "no test covers
    this line", which is indistinguishable from a genuine coverage gap.
    """
    assert len(lines_to_tests) > 100, (
        f"only {len(lines_to_tests)} covered lines across all per-test reports; "
        "the reverse lookup would have nothing to answer with")


def test_the_reverse_lookup_discriminates(lines_to_tests, manifest):
    """Some line must resolve to fewer tests than the whole suite.

    This is the property that makes the command worth having. If every covered
    line named every test -- which is what keeping the `hit == 0` lines would
    produce -- the answer would carry no information.
    """
    smallest = min(len(tests) for tests in lines_to_tests.values())
    assert smallest < len(manifest), (
        "every covered line is attributed to every test; per-test attribution "
        "has collapsed")
    assert smallest == 1, (
        f"no line is covered by exactly one test (smallest set is {smallest}); "
        "expected the suite to contain at least one uniquely-covered line")


def test_reverse_lookup_names_are_real_ctest_tests(lines_to_tests, ctest_names):
    """The names the quick-pick would show must be revealable in the explorer.

    The forward direction drops unmatched names before the editor sees them
    because it asserts; this direction shows them greyed out instead. Either
    way, real data naming a test CTest does not know is a bug upstream of both.
    """
    named = {test for tests in lines_to_tests.values() for test in tests}
    assert not named - ctest_names, (
        f"reverse lookup names tests CTest does not know: {sorted(named - ctest_names)}")
