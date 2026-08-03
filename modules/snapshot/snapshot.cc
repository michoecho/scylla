#include "snapshot/snapshot.h"

#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <format>
#include <fstream>
#include <map>
#include <sstream>

#include "snapshot/updater.h"

namespace snapshot_testing {
namespace {

// Every update recorded this run, keyed by nothing -- order does not matter,
// because apply_updates sorts by position itself.
std::vector<PendingUpdate>& updates() {
    static std::vector<PendingUpdate> instance;
    return instance;
}

}  // namespace

bool update_mode() {
    // Read once: the environment cannot meaningfully change mid-run, and a
    // single read keeps every snapshot in a suite agreeing about the mode.
    static const bool enabled = [] {
        const char* value = std::getenv("SNAPSHOT_UPDATE");
        return value != nullptr && std::string_view(value) == "1";
    }();
    return enabled;
}

Comparison compare(std::string_view got, const Snapshot& expected) {
    if (got == expected.value) {
        // The values agree, so the only thing that can be wrong here is a
        // leftover marker -- and it is reported as exactly that, never as a
        // mismatch, so the output cannot claim the values differ when they do
        // not.
        //
        // The passing path writes nothing and records nothing: a green suite
        // must not depend on its own sources being present, let alone writable.
        return expected.forced ? Comparison::StaleUpdateMarker : Comparison::Matched;
    }

    // Either the whole run is in update mode, or this one snapshot opted in.
    const bool recording = update_mode() || expected.forced;
    if (recording) {
        updates().push_back(PendingUpdate{
            .file = expected.location.file_name(),
            .line = expected.location.line(),
            .column = expected.location.column(),
            .old_value = std::string(expected.value),
            .new_value = std::string(got),
        });
    }

    // A mismatch either way -- when rewriting too. A run that rewrote sources
    // has changed the meaning of the test and must say so; see snapshot.h.
    return recording ? Comparison::MismatchedAndRecorded : Comparison::Mismatched;
}

std::string render_mismatch(std::string_view got, const Snapshot& expected) {
    return std::format("snapshot mismatch at {}:{}:{}\n"
                       "--- expected (in source) ---\n{}\n"
                       "--- actual ---\n{}",
                       expected.location.file_name(), expected.location.line(),
                       expected.location.column(), expected.value, got);
}

const std::vector<PendingUpdate>& pending_updates() { return updates(); }

void discard_updates() { updates().clear(); }

std::string flush_updates() {
    if (updates().empty()) return {};

    // Group by file, because apply_updates rewrites a whole file in one pass --
    // which is what lets it apply many updates to one file bottom-up.
    std::map<std::string, std::vector<Update>> by_file;
    for (const PendingUpdate& pending : updates()) {
        by_file[pending.file].push_back(Update{
            .line = pending.line,
            .column = pending.column,
            .old_value = pending.old_value,
            .new_value = pending.new_value,
        });
    }
    updates().clear();

    std::string errors;
    for (const auto& [path, file_updates] : by_file) {
        // The path is whatever the compiler was handed for the translation
        // unit: std::source_location::file_name() is __FILE__, verbatim, so an
        // absolute path on the command line yields an absolute path here and a
        // relative one yields a relative path. CMake passes absolute paths, and
        // this depends on that -- a test binary's working directory is the
        // build tree, not the source tree, so a relative path would resolve
        // against the wrong root.
        //
        // Checked rather than assumed, because the failure mode otherwise is
        // either a baffling "cannot read", or -- far worse -- writing a
        // rewritten source file into the build directory, where it would be
        // silently ignored and the real test would never be updated.
        if (!std::filesystem::path(path).is_absolute()) {
            errors += "refusing to update " + path +
                      ": std::source_location reported a relative path, which "
                      "cannot be resolved from the test's working directory. "
                      "Build with absolute source paths.\n";
            continue;
        }

        std::ifstream in(path, std::ios::binary);
        if (!in) {
            errors += "cannot read " + path + "\n";
            continue;
        }
        std::ostringstream buffer;
        buffer << in.rdbuf();
        in.close();

        const UpdateResult rewritten = apply_updates(buffer.str(), file_updates);
        if (!rewritten.ok) {
            errors += path + ": " + rewritten.error + "\n";
            continue;
        }

        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        if (!out) {
            errors += "cannot write " + path + "\n";
            continue;
        }
        out << rewritten.text;
        if (!out) {
            errors += "write failed for " + path + "\n";
            continue;
        }
        std::fprintf(stderr, "snapshot: updated %zu snapshot(s) in %s\n",
                     file_updates.size(), path.c_str());
    }
    return errors;
}

}  // namespace snapshot_testing
