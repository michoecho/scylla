"""Shared fixtures for integration tests that drive built binaries.

The Buck2 test invocation supplies the binary path through the environment;
running these tests directly without that context skips them.
"""

import os
from pathlib import Path

import pytest


def _binary_from_env(var):
    """Resolve an environment-provided binary, skipping if it is unset."""
    raw = os.environ.get(var)
    if not raw:
        pytest.skip(f"{var} unset; run these tests with a Buck2-provided binary")
    path = Path(raw)
    if not path.exists():
        pytest.fail(f"{var} points at a missing file: {path}")
    return path


@pytest.fixture(scope="session")
def pt_test_bin():
    """Path to the `pt` module's test binary for the preset being tested.

    The Intel PT tests trace this rather than cpp_template: the traced case
    lives in the pt module, and running it from that module's own runner is
    what keeps the tracing test independent of whether the program happens to
    link pt.
    """
    return _binary_from_env("PT_TEST_BIN")
