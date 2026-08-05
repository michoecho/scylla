"""Shared fixtures for the tools/ tests.

These tests drive built binaries, so they need to know which ones to run -- and
those must come from the preset under test, not a hardcoded path, or a
`ctest --preset SanitizeTest` run would silently exercise the Debug build. CMake
passes the answers in as environment variables resolved from $<TARGET_FILE:...>;
see CMakeLists.txt.
"""

import os
from pathlib import Path

import pytest


def _binary_from_env(var):
    """Resolve an env var naming a built binary, skipping if it is unset.

    Unset means "not launched from ctest", which is a skip: the path is the
    build system's to supply. Set but missing is a failure -- the variable came
    from a $<TARGET_FILE:...> whose target was never built, and silently
    skipping there would hide a broken build.
    """
    raw = os.environ.get(var)
    if not raw:
        pytest.skip(f"{var} unset; run these tests via ctest")
    path = Path(raw)
    if not path.exists():
        pytest.fail(f"{var} points at a missing file: {path}")
    return path


@pytest.fixture(scope="session")
def cpp_template_bin():
    """Path to the cpp_template binary for the preset being tested."""
    return _binary_from_env("CPP_TEMPLATE_BIN")


@pytest.fixture(scope="session")
def pt_test_bin():
    """Path to the `pt` module's test binary for the preset being tested.

    The Intel PT tests trace this rather than cpp_template: the traced case
    lives in the pt module, and running it from that module's own runner is
    what keeps the tracing test independent of whether the program happens to
    link pt.
    """
    return _binary_from_env("PT_TEST_BIN")
