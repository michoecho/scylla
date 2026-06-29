#pragma once

#include <cstddef>
#include <cstdint>

// Fuzz targets: each is a function that consumes one opaque input buffer and
// exercises some code under test. They are the unit AFL drives — main.cc's
// `fuzz <name>` subcommand wraps the chosen target in AFL's persistent loop
// (or, in a non-AFL build, runs it once on stdin). See skills/afl-fuzz.
//
// A target must be safe to call repeatedly in one process (persistent mode
// reuses the process across many inputs) and must NOT read past `size` of the
// buffer it is given. Keep targets tiny: whatever a target touches is the
// surface AFL explores.

namespace fuzz {

// Decode-a-varint target. Built into the main binary; the actual bug it can
// trip lives behind DELIBERATE_BUGS_FOR_FUZZING in varint.cc.
void varint(const std::uint8_t* data, std::size_t size);

}  // namespace fuzz
