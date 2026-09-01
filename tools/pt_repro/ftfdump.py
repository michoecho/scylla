#!/usr/bin/env python3
"""Read a .ftf written by the perf2perfetto dlfilter and print its call tree.

Only the record types that dlfilter emits are handled: string records (2),
thread records (3) and event records (4) with the duration-begin (2),
duration-end (3) and duration-complete (4) subtypes. That is enough to
reconstruct the frame nesting, which is what the reproducers assert on.
"""

import struct
import sys

INTERNAL_STRINGS = {
    0: "", 1: "Instructions", 2: "Cycles", 3: "Footprint", 4: "Symbol",
    5: "Timespan",
}


def bits(word, lo, hi):
    return (word >> lo) & ((1 << (hi - lo)) - 1)


def parse(path):
    with open(path, "rb") as f:
        data = f.read()

    strings = dict(INTERNAL_STRINGS)
    threads = {}
    events = []
    off = 8  # magic
    while off + 8 <= len(data):
        (word,) = struct.unpack_from("<Q", data, off)
        rtype = bits(word, 0, 4)
        rsize = bits(word, 4, 16)
        if rsize == 0:
            break
        end = off + rsize * 8
        if rtype == 2:  # string record
            idx = bits(word, 16, 32)
            slen = bits(word, 32, 47)
            strings[idx] = data[off + 8:off + 8 + slen].decode("utf-8", "replace")
        elif rtype == 3:  # thread record
            idx = bits(word, 16, 24)
            pid, tid = struct.unpack_from("<QQ", data, off + 8)
            threads[idx] = (pid, tid)
        elif rtype == 4:  # event record
            etype = bits(word, 16, 20)
            name = strings.get(bits(word, 48, 64), "?")
            thread = threads.get(bits(word, 24, 32), ("?", "?"))
            (ts,) = struct.unpack_from("<Q", data, off + 8)
            events.append((etype, name, ts, thread))
        off = end
    return events


class Frame:
    def __init__(self, name, start):
        self.name = name
        self.start = start
        self.end = None
        self.children = []


def build_trees(events):
    """Replay begin/end events into one forest per thread.

    The dlfilter keeps a separate shadow stack per thread and the .ftf
    interleaves them, so nesting is only meaningful within a thread.
    """
    roots = {}
    stacks = {}
    unmatched_ends = {}
    for etype, name, ts, thread in events:
        roots.setdefault(thread, [])
        stack = stacks.setdefault(thread, [])
        if etype == 2:  # duration begin
            frame = Frame(name, ts)
            (stack[-1].children if stack else roots[thread]).append(frame)
            stack.append(frame)
        elif etype == 3:  # duration end
            if not stack:
                unmatched_ends[thread] = unmatched_ends.get(thread, 0) + 1
                continue
            stack.pop().end = ts
        elif etype == 4:  # duration complete (a frame we only saw return)
            frame = Frame(name, ts)
            frame.end = ts
            (stack[-1].children if stack else roots[thread]).append(frame)
    return roots, stacks, unmatched_ends


def render(frames, depth=0, out=None):
    out = out if out is not None else []
    for f in frames:
        state = "" if f.end is not None else "   <-- NEVER CLOSED"
        out.append("  " * depth + f.name + state)
        render(f.children, depth + 1, out)
    return out


def main():
    events = parse(sys.argv[1])
    roots, stacks, unmatched_ends = build_trees(events)
    for thread in sorted(roots):
        pid, tid = thread
        print(f"=== pid {pid} tid {tid} ===")
        print("\n".join(render(roots[thread])))
        print(f"  [{len(stacks[thread])} frames left open, "
              f"{unmatched_ends.get(thread, 0)} unmatched ends]\n")
    print(f"{len(events)} events total")


if __name__ == "__main__":
    main()
