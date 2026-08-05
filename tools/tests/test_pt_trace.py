"""End-to-end test for tools/pt-trace.

Runs the real thing: traces the `intel pt nested calls` doctest case under
Intel PT, decodes the result to a Fuchsia trace (.ftf) via the perf2perfetto
dlfilter, and checks the decoded trace names the functions the test calls.

The case is traced in the `pt` module's own test binary rather than in the
cpp_template executable. Both contain it, but pt_test is where it belongs: this
tests the pt module's control protocol, so it should not also depend on the
program choosing to link that module.

This is deliberately an integration test rather than a unit test of pt-trace's
internals. What can break here is the interaction between the pieces -- the
control fifo protocol in modules/pt/pt_control.cc, perf's --control handling, and the
dlfilter's symbolization -- and none of that is exercised by mocking.

Requires real hardware support: an intel_pt PMU, a readable perf_event_paranoid,
and the dlfilter from the Nix devshell. Each is checked up front and skips the
test rather than failing it, since none of them are under the code's control.
"""

import os
import shutil
import subprocess

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PT_TRACE = os.path.join(REPO_ROOT, "tools", "pt-trace")

# The doctest case to trace, and the functions its traced region calls. These
# are the noinline helpers in modules/pt/pt_control_test.cc, which nest as
# pt_outer -> pt_middle -> pt_leaf; the decoded trace should name each one.
TEST_CASE = "intel pt nested calls"
EXPECTED_SYMBOLS = ["pt_leaf", "pt_middle", "pt_outer"]


def _skip_unless_intel_pt_available():
    """Skip unless this machine can actually record an Intel PT trace."""
    if not os.path.exists("/sys/bus/event_source/devices/intel_pt"):
        pytest.skip("no intel_pt PMU on this machine (needs Intel CPU + PT support)")

    if shutil.which("perf") is None:
        pytest.skip("perf not on PATH")

    # perf_event_paranoid > 2 blocks even user-space-only tracing.
    try:
        with open("/proc/sys/kernel/perf_event_paranoid") as f:
            paranoid = int(f.read().strip())
    except (OSError, ValueError):
        pytest.skip("cannot read /proc/sys/kernel/perf_event_paranoid")
    if paranoid > 2:
        pytest.skip(f"perf_event_paranoid={paranoid} forbids user-space tracing")

    dlfilter = os.environ.get("PERF2PERFETTO_DLFILTER")
    if not dlfilter or not os.path.exists(dlfilter):
        pytest.skip("PERF2PERFETTO_DLFILTER unset or missing; run inside `nix develop`")


def test_pt_trace_produces_ftf_naming_traced_functions(pt_test_bin, tmp_path):
    """tools/pt-trace --ftf decodes a trace that names the traced functions."""
    _skip_unless_intel_pt_available()

    ftf = tmp_path / "perf.ftf"
    perf_data = tmp_path / "perf.data"

    result = subprocess.run(
        [
            PT_TRACE,
            "--ftf", str(ftf),
            "-o", str(perf_data),
            "--",
            str(pt_test_bin), "test", f"--test-case={TEST_CASE}", "--exit",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=300,
    )

    assert result.returncode == 0, (
        f"pt-trace failed with {result.returncode}\n"
        f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
    )
    assert ftf.exists(), (
        f"pt-trace reported success but wrote no {ftf}\n"
        f"--- stderr ---\n{result.stderr}"
    )
    assert ftf.stat().st_size > 0, f"{ftf} is empty"

    # The .ftf is a binary Fuchsia trace, but symbol names are stored in it as
    # plain interned strings, so a substring search over the bytes is enough
    # and avoids depending on a trace-parsing library.
    blob = ftf.read_bytes()
    missing = [s for s in EXPECTED_SYMBOLS if s.encode() not in blob]
    assert not missing, (
        f"decoded trace does not name {missing} "
        f"(found: {[s for s in EXPECTED_SYMBOLS if s.encode() in blob]}); "
        f"{ftf} is {len(blob)} bytes"
    )
