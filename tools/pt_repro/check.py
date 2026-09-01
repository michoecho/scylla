#!/usr/bin/env python3
"""Build the perf2perfetto reproducers, trace them, and check the call trees.

Each C program in this directory isolates one thing that breaks a naive
shadow stack: a syscall, a scheduler preemption, a PLT trampoline, a tail
call. All four used to leave frames open forever, so the checks here are
mostly about balance and nesting rather than timing.

Usage: tools/pt_repro/check.py [scenario ...]     (default: all)
"""

import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ftfdump import build_trees, parse

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
PT_TRACE = os.path.join(ROOT, "tools", "pt-trace")


def walk(frames, depth=0):
    for f in frames:
        yield depth, f
        yield from walk(f.children, depth + 1)


def find(frames, name):
    return [f for _, f in walk(frames) if f.name == name]


def children_of(frames, name):
    for _, f in walk(frames):
        if f.name == name:
            return [c.name for c in f.children]
    return None


def check_common(roots, problems):
    """Properties every scenario must have, whatever it exercises."""
    # One TRACE frame means one contiguous segment: the stack was never reset.
    traces = [f for f in roots if f.name == "TRACE"]
    if len(traces) != 1:
        problems.append(
            f"call stack was reset: expected 1 TRACE segment, found {len(traces)}")
    for _, f in walk(roots):
        if f.end is None:
            problems.append(f"frame {f.name!r} never closed")


def check_syscall(roots, problems):
    # The syscall must not truncate the stack: the frames around it stay put
    # and the kernel side shows up as its own closed frame.
    if children_of(roots, "do_getpid") != ["[kernel]"]:
        problems.append(
            f"expected do_getpid to contain exactly [kernel], "
            f"got {children_of(roots, 'do_getpid')}")
    if "do_getpid" not in (children_of(roots, "syscall_leaf") or []):
        problems.append("syscall_leaf did not contain do_getpid")


def check_tailcall(roots, problems):
    # `jmp tail_callee` ends tail_caller, so the two are siblings sharing a
    # parent -- not nested, and neither left dangling.
    kids = children_of(roots, "repro_root")
    if kids != ["tail_caller", "tail_callee"]:
        problems.append(
            f"expected repro_root's children to be [tail_caller, tail_callee], "
            f"got {kids}")


def check_plt(roots, problems):
    # The stub and the tail-call chain behind it are siblings inside the
    # caller, and the caller's own frame still closes around them.
    kids = children_of(roots, "plt_call")
    if kids[:3] != ["sigaction@plt", "__sigaction", "__libc_sigaction"]:
        problems.append(
            f"expected plt_call to contain the stub and its tail-call chain "
            f"as siblings, got {kids}")
    if "plt_call" not in (children_of(roots, "plt_outer") or []):
        problems.append("plt_outer did not contain plt_call")


def check_preempt(roots, problems):
    # Surviving preemption is exactly check_common's single-TRACE property;
    # all that is left is that the frame we were preempted inside is there.
    if not find(roots, "preempt_leaf"):
        problems.append("preempt_leaf missing from the trace")


SCENARIOS = {
    "syscall": check_syscall,
    "tailcall": check_tailcall,
    "plt": check_plt,
    "preempt": check_preempt,
}


def run(scenario, check):
    binary = os.path.join(HERE, f"{scenario}_repro")
    subprocess.run(
        ["gcc", "-O2", "-g", "-o", binary, f"{binary}.c", "-lpthread"],
        check=True)

    ftf = os.path.join(HERE, f"{scenario}.ftf")
    data = os.path.join(HERE, f"{scenario}.perf.data")
    subprocess.run(
        [PT_TRACE, "run", "--ftf", ftf, "-o", data, "--", binary],
        check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    roots, _, _ = build_trees(parse(ftf))
    # The traced thread is the one that opened frames; a helper thread that
    # only spins has nothing to check.
    main = max(roots.values(), key=lambda fs: sum(1 for _ in walk(fs)))

    problems = []
    check_common(main, problems)
    check(main, problems)
    return main, problems


def main():
    wanted = sys.argv[1:] or list(SCENARIOS)
    failed = False
    for scenario in wanted:
        roots, problems = run(scenario, SCENARIOS[scenario])
        if problems:
            failed = True
            print(f"FAIL {scenario}")
            for p in problems:
                print(f"       {p}")
            for depth, f in walk(roots):
                print("       " + "  " * depth + f.name)
        else:
            print(f"ok   {scenario}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
