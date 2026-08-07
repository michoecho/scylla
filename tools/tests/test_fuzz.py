"""End-to-end test for tools/fuzz, and the only check that the AFL backend works.

This is the claim modules/test_rng/backend_demo_test.cc cannot make about
itself: that AFL, and only AFL, guesses the five magic bytes. That file holds
the target -- the "afl guesses the magic bytes" case, inert unless something is
fuzzing it -- and the thing which does the fuzzing is tools/fuzz, so the test
that closes the loop belongs here rather than in the C++ suite. It used to be a
doctest case that shelled out to afl-fuzz; running it from the tool means the
tool itself is what gets exercised, rather than a second copy of its recipe
drifting alongside it.

tools/fuzz is imported rather than executed, so this runs afl-fuzz in a
subprocess but not Python. That is what `fuzz()` returning a Result -- and
raising FuzzError instead of calling sys.exit -- is for.

The real search is fast: the magic-bytes case is measured at well under a second
from the default seed. What is not fast is the two builds it depends on, which
is why the Fuzz binaries are built by the ctest fixture rather than timed here.
"""

import importlib.machinery
import importlib.util
import shutil
import sys
from pathlib import Path

import pytest


TOOLS = Path(__file__).parents[1]

# tools/fuzz has no .py extension -- it is a command first -- so it cannot be
# imported by name. Loading it from an explicit spec is the supported way to
# import a file whose name is not a module name, and it is exactly what the
# module's docstring points at.
def _load():
    spec = importlib.util.spec_from_loader(
        "fuzz", importlib.machinery.SourceFileLoader("fuzz", str(TOOLS / "fuzz")))
    module = importlib.util.module_from_spec(spec)
    # Registered before execution so the module is importable by name from here
    # on, matching what `import fuzz` would have left behind.
    sys.modules["fuzz"] = module
    spec.loader.exec_module(module)
    return module


fuzz = _load()


# Fuzzing needs both AFL-instrumented presets configured, and AFL itself. None
# of that is true of a plain Debug tree, and a tree configured only for Debug is
# not a broken tree -- so this is a skip rather than a failure. It is also what
# keeps `ctest --preset DebugTest` green: this file is registered for every
# preset, and only the Fuzz ones can actually run it.
def _requirement():
    if shutil.which("afl-fuzz") is None:
        return "afl-fuzz is not on PATH"
    for preset in (fuzz.FUZZ_PRESET, fuzz.CMPLOG_PRESET):
        if not fuzz.configured(preset):
            return f"the {preset} preset is not configured"
    return None


needs_afl = pytest.mark.skipif(_requirement() is not None,
                               reason=_requirement() or "")

# The case under test, anchored so it cannot also match "AFL guesses the magic
# bytes the other backends cannot" -- which is exactly the ambiguity the tool is
# supposed to refuse, and is checked for below.
MAGIC_TEST = "afl guesses the magic bytes$"

# What the planted bug is looking for. The saved crash input starts with these
# five bytes; AFL's havoc stage usually leaves trailing noise after them, so
# this is a prefix check rather than an equality one.
MAGIC = b"hegel"


@needs_afl
def test_afl_finds_the_magic_bytes(tmp_path):
    """The whole claim: AFL guesses five bytes the other backends cannot.

    `until_crash` stops the run at the first find, which is what makes this a
    test rather than an open-ended search. There is deliberately no internal
    timeout -- if the backend regresses, AFL searches forever and the ctest
    TIMEOUT on this file is what turns that into a failure, exactly as the
    doctest version relied on its own TIMEOUT property.
    """
    result = fuzz.fuzz(MAGIC_TEST, work=tmp_path, until_crash=True,
                       capture=True, quiet=True)

    # Instrumentation and the persistent loop are working. A failure here is a
    # far more useful message than "no crash found".
    assert "Persistent mode binary detected" in result.output
    # And the CmpLog binary was actually consulted, which is the half of the
    # setup that a plain -i/-o run would silently skip.
    assert "CMPLOG forkserver successfully started" in result.output

    assert result.crashes, (
        f"AFL saved no crash; it did not guess {MAGIC!r}\n{result.output}")
    assert any(crash.startswith(MAGIC) for crash in result.crashes), (
        f"crashes were saved but none start with {MAGIC!r}: {result.crashes}")


@needs_afl
def test_resolves_the_test_through_ctest(tmp_path):
    """The filter names one ctest test, and the tool reports which."""
    test = fuzz.find_test(MAGIC_TEST)
    assert test["name"] == "test_rng:::afl guesses the magic bytes"
    # The command ctest reports is the binary plus the case selector, and it is
    # the Fuzz preset's binary rather than some other tree's.
    binary = Path(test["command"][0])
    assert binary.is_relative_to(fuzz.BUILD / fuzz.FUZZ_PRESET)
    assert test["command"][1:] == ["--test-case=afl guesses the magic bytes"]
    # The CmpLog twin is the same relative path under the other preset.
    assert fuzz.cmplog_binary(binary).is_relative_to(
        fuzz.BUILD / fuzz.CMPLOG_PRESET)


@needs_afl
def test_rejects_a_filter_matching_several_tests():
    """An ambiguous filter is refused, since everything after it is singular."""
    with pytest.raises(fuzz.FuzzError) as error:
        fuzz.find_test("magic bytes")
    assert "need exactly one" in str(error.value)
    # The candidates are named, so the message is enough to fix the filter.
    assert "afl guesses the magic bytes" in str(error.value)


@needs_afl
def test_rejects_a_filter_matching_nothing():
    with pytest.raises(fuzz.FuzzError) as error:
        fuzz.find_test("no such test exists anywhere")
    assert "no test matches" in str(error.value)


def test_reports_an_unconfigured_preset(tmp_path):
    """A missing preset is an error naming the command that fixes it.

    Not gated on AFL: it is the failure a tree without the Fuzz presets should
    produce, so it is worth checking precisely where that is the situation.
    """
    with pytest.raises(fuzz.FuzzError) as error:
        fuzz.build("NoSuchPreset", tmp_path / "binary", quiet=True)
    assert "not configured" in str(error.value)
    assert "cmake --preset NoSuchPreset" in str(error.value)
