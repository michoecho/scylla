"""Shared fixtures for the tools/ tests.

These tests drive the built cpp_template binary, so they need to know which one
to run -- and that must be the binary from the preset under test, not a
hardcoded path, or a `ctest --preset SanitizeTest` run would silently exercise
the Debug build. CMake passes the answer in via CPP_TEMPLATE_BIN, resolved from
$<TARGET_FILE:cpp_template>; see CMakeLists.txt.
"""

import os
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def cpp_template_bin():
    """Path to the cpp_template binary for the preset being tested."""
    raw = os.environ.get("CPP_TEMPLATE_BIN")
    if not raw:
        pytest.skip("CPP_TEMPLATE_BIN unset; run these tests via ctest")
    path = Path(raw)
    if not path.exists():
        pytest.fail(f"CPP_TEMPLATE_BIN points at a missing file: {path}")
    return path
