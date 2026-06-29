#pragma once

#include <string>

// Entry point for the `fuzz <name>` subcommand. Selects a fuzz target by name
// and drives it:
//
//   * Built with afl-clang-fast++ (the Fuzz preset): runs AFL's persistent loop
//     — many inputs per process, fed from AFL's shared-memory buffer.
//   * Built with a plain compiler: reads ONE input from stdin and runs the
//     target once, then exits. This is how you reproduce/replay a saved crash
//     without AFL:  ./cpp_template fuzz varint < crash_input
//
// Returns a process exit code. An unknown target name returns non-zero.
namespace fuzz {

int run(const std::string& target);

}  // namespace fuzz
