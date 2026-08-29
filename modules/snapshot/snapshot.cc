#include "snapshot/snapshot.h"

#include <array>
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <filesystem>
#include <format>
#include <fstream>
#include <iomanip>
#include <map>
#include <random>
#include <re2/re2.h>
#include <set>
#include <sstream>
#include <sys/file.h>
#include <system_error>
#include <unistd.h>

#include "snapshot/updater.h"

namespace snapshot_testing {
namespace {

std::vector<PendingUpdate> &updates() {
  static std::vector<PendingUpdate> instance;
  return instance;
}

struct Owner {
  std::string file;
  unsigned line;
  unsigned column;
};
std::map<std::string, Owner> &owners() {
  static std::map<std::string, Owner> instance;
  return instance;
}

bool canonical_uuid(std::string_view id) {
  if (id.size() != 36)
    return false;
  for (std::size_t i = 0; i < id.size(); ++i) {
    if (i == 8 || i == 13 || i == 18 || i == 23) {
      if (id[i] != '-')
        return false;
    } else if (!((id[i] >= '0' && id[i] <= '9') ||
                 (id[i] >= 'a' && id[i] <= 'f'))) {
      return false;
    }
  }
  return true;
}

std::string generate_uuid() {
  std::array<unsigned char, 16> bytes{};
  std::random_device random;
  for (unsigned char &byte : bytes)
    byte = static_cast<unsigned char>(random());
  bytes[6] = static_cast<unsigned char>((bytes[6] & 0x0f) | 0x40);
  bytes[8] = static_cast<unsigned char>((bytes[8] & 0x3f) | 0x80);
  std::ostringstream out;
  out << std::hex << std::setfill('0');
  for (std::size_t i = 0; i < bytes.size(); ++i) {
    if (i == 4 || i == 6 || i == 8 || i == 10)
      out << '-';
    out << std::setw(2) << static_cast<unsigned>(bytes[i]);
  }
  return out.str();
}

std::filesystem::path snapshot_layout(const std::filesystem::path &root,
                                      std::string_view id) {
  return root / std::string(id.substr(0, 2)) / (std::string(id) + ".snap");
}

// Where a file snapshot is *read* from: the build's copy of the store.
//
// Hermetic, and materialised into the sandbox by the module's filegroup, which
// is what lets a test that compares against file snapshots run remotely.
std::filesystem::path snapshot_path(std::string_view id) {
  const char* const root = std::getenv("SNAPSHOT_ROOT");
  return snapshot_layout(std::filesystem::path(root ? root : ""), id);
}

// Where a file snapshot is *written*: the store's path in the source tree.
//
// Not the read path. That one lives under buck-out, so a snapshot written there
// would be discarded by the next `buck2 clean`, never reach the repository, and
// be reported missing by tools/snapshot-files -- which audits the source tree.
// Recording a snapshot has to land where the snapshot is kept.
std::filesystem::path snapshot_source_path(std::string_view id) {
  const char* const root = std::getenv("SNAPSHOT_SOURCE_ROOT");
  return snapshot_layout(std::filesystem::path(root ? root : ""), id);
}

struct ReadResult {
  bool ok = false;
  bool missing = false;
  std::string bytes;
  std::string error;
};
ReadResult read_bytes(const std::filesystem::path &path) {
  std::error_code ec;
  if (!std::filesystem::exists(path, ec)) {
    if (ec)
      return {.ok = false,
              .missing = false,
              .bytes = {},
              .error = "cannot inspect " + path.string() + ": " + ec.message()};
    return {.ok = false,
            .missing = true,
            .bytes = {},
            .error = "missing file snapshot " + path.string()};
  }
  std::ifstream in(path, std::ios::binary);
  if (!in)
    return {.ok = false,
            .missing = false,
            .bytes = {},
            .error = "cannot read file snapshot " + path.string()};
  std::ostringstream contents;
  contents << in.rdbuf();
  if (in.bad())
    return {.ok = false,
            .missing = false,
            .bytes = {},
            .error = "read failed for file snapshot " + path.string()};
  return {.ok = true, .missing = false, .bytes = contents.str(), .error = {}};
}

bool printable_text(std::string_view value) {
  if (value.size() > 8192)
    return false;
  for (unsigned char c : value)
    if ((c < 0x20 && c != '\n' && c != '\r' && c != '\t') || c == 0x7f)
      return false;
  return true;
}

std::string byte_summary(std::string_view expected, std::string_view got) {
  std::size_t first = 0;
  while (first < expected.size() && first < got.size() &&
         expected[first] == got[first])
    ++first;
  return std::format(
      "expected {} bytes, actual {} bytes; first difference at byte {}",
      expected.size(), got.size(), first);
}

// A full match of `pattern` against the whole of `value`.
//
// RE2 rather than std::regex, for both of the reasons RE2 exists. It matches in
// time linear in the value, and a snapshot is routinely thousands of characters
// long -- a backtracking engine walks a plain concatenation of that length by
// recursing once per character, which is a stack overflow disguised as an
// assertion. And a malformed pattern is a value here, not an exception: the
// pattern comes from the test's own serializer, so a bad one has to surface as
// a failing assertion naming the pattern, not as a throw unwinding out of the
// test case.
struct PatternMatch {
  bool matched = false;
  bool valid = false;
  std::string error;
};

PatternMatch full_match(const std::string &pattern, std::string_view value) {
  // Quiet, because a bad pattern is reported through the return value and
  // then again in the assertion message. RE2's default is to also log it to
  // stderr, which under a parallel test run arrives detached from the failure
  // it belongs to.
  const RE2 expression(pattern, RE2::Quiet);
  if (!expression.ok())
    return {.matched = false, .valid = false, .error = expression.error()};
  return {.matched = RE2::FullMatch(value, expression),
          .valid = true,
          .error = {}};
}

std::string location_key(const std::source_location &location) {
  return std::format("{}:{}:{}", location.file_name(), location.line(),
                     location.column());
}

} // namespace

bool update_mode() {
  static const bool enabled = [] {
    const char *value = std::getenv("SNAPSHOT_UPDATE");
    return value != nullptr && std::string_view(value) == "1";
  }();
  return enabled;
}

Comparison compare(std::string_view got, const Snapshot &expected) {
  if (got == expected.value)
    return expected.forced ? Comparison::StaleUpdateMarker
                           : Comparison::Matched;
  const bool recording = update_mode() || expected.forced;
  if (recording)
    updates().push_back(PendingUpdate{.kind = PendingUpdate::Kind::Inline,
                                      .file = expected.location.file_name(),
                                      .line = expected.location.line(),
                                      .column = expected.location.column(),
                                      .old_value = std::string(expected.value),
                                      .new_value = std::string(got),
                                      .id = {},
                                      .initialize = false,
                                      .existed = true});
  return recording ? Comparison::MismatchedAndRecorded : Comparison::Mismatched;
}

Comparison compare(const RegexText &got, const Snapshot &expected) {
  // The recorded value is asked to satisfy the pattern this run produced --
  // not the other way round. What is being checked is that the sample in the
  // source is still one of the serializations this code can emit.
  if (full_match(got.pattern(), expected.value).matched)
    return expected.forced ? Comparison::StaleUpdateMarker
                           : Comparison::Matched;

  const bool recording = update_mode() || expected.forced;
  if (recording)
    updates().push_back(PendingUpdate{.kind = PendingUpdate::Kind::Inline,
                                      .file = expected.location.file_name(),
                                      .line = expected.location.line(),
                                      .column = expected.location.column(),
                                      .old_value = std::string(expected.value),
                                      .new_value = got.text(),
                                      .id = {},
                                      .initialize = false,
                                      .existed = true});
  return recording ? Comparison::MismatchedAndRecorded : Comparison::Mismatched;
}

Comparison compare(std::string_view got, const FileSnapshot &expected) {
  const bool recording = update_mode() || expected.forced;
  const bool initialize = expected.id.empty();
  std::string id(expected.id);
  if (id.empty()) {
    if (!recording)
      return Comparison::Mismatched;
    id = generate_uuid();
  } else if (!canonical_uuid(id)) {
    return Comparison::Mismatched;
  }

  const Owner here{expected.location.file_name(), expected.location.line(),
                   expected.location.column()};
  const auto [owner, inserted] = owners().emplace(id, here);
  if (!inserted &&
      (owner->second.file != here.file || owner->second.line != here.line ||
       owner->second.column != here.column)) {
    return Comparison::Mismatched;
  }

  const ReadResult old = read_bytes(snapshot_path(id));
  if (old.ok && old.bytes == got)
    return expected.forced ? Comparison::StaleUpdateMarker
                           : Comparison::Matched;
  if (!recording || (!old.ok && !old.missing))
    return Comparison::Mismatched;

  updates().push_back(
      PendingUpdate{.kind = PendingUpdate::Kind::File,
                    .file = expected.location.file_name(),
                    .line = expected.location.line(),
                    .column = expected.location.column(),
                    .old_value = old.ok ? old.bytes : std::string{},
                    .new_value = std::string(got),
                    .id = id,
                    .initialize = initialize,
                    .existed = old.ok});
  return Comparison::MismatchedAndRecorded;
}

std::string render_mismatch(std::string_view got, const Snapshot &expected) {
  return std::format(
      "snapshot mismatch at {}:{}:{}\n--- expected (in source) ---\n{}\n"
      "--- actual ---\n{}",
      expected.location.file_name(), expected.location.line(),
      expected.location.column(), expected.value, got);
}

std::string render_mismatch(const RegexText &got, const Snapshot &expected) {
  const PatternMatch result = full_match(got.pattern(), expected.value);
  if (!result.valid)
    return std::format(
        "malformed snapshot pattern at {}:{}:{}: {}\n--- pattern ---\n{}",
        expected.location.file_name(), expected.location.line(),
        expected.location.column(), result.error, got.pattern());
  return std::format(
      "snapshot mismatch at {}:{}:{}\n--- expected (in source) ---\n{}\n"
      "--- actual ---\n{}\n--- pattern the expected value must match ---\n{}",
      expected.location.file_name(), expected.location.line(),
      expected.location.column(), expected.value, got.text(), got.pattern());
}

std::string render_mismatch(std::string_view got,
                            const FileSnapshot &expected) {
  if (expected.id.empty())
    return std::format("uninitialized file snapshot at {}",
                       location_key(expected.location));
  if (!canonical_uuid(expected.id))
    return std::format("malformed file snapshot id '{}' at {}", expected.id,
                       location_key(expected.location));
  const auto found = owners().find(std::string(expected.id));
  if (found != owners().end()) {
    const Owner here{expected.location.file_name(), expected.location.line(),
                     expected.location.column()};
    if (found->second.file != here.file || found->second.line != here.line ||
        found->second.column != here.column)
      return std::format(
          "duplicate file snapshot id '{}' at {}; first owned by {}:{}:{}",
          expected.id, location_key(expected.location), found->second.file,
          found->second.line, found->second.column);
  }
  const ReadResult old = read_bytes(snapshot_path(expected.id));
  if (!old.ok)
    return old.error + " (referenced at " + location_key(expected.location) +
           ")";
  if (printable_text(old.bytes) && printable_text(got))
    return std::format(
        "file snapshot mismatch at {}\n--- expected ({}) ---\n{}\n"
        "--- actual ---\n{}",
        location_key(expected.location), snapshot_path(expected.id).string(),
        old.bytes, got);
  return "file snapshot mismatch at " + location_key(expected.location) + ": " +
         byte_summary(old.bytes, got);
}

const std::vector<PendingUpdate> &pending_updates() { return updates(); }
void discard_updates() {
  updates().clear();
  owners().clear();
}

std::string flush_updates() {
  if (updates().empty())
    return {};
  // Source locations arrive project-relative, because that is what Buck2
  // compiles with, so everything below is anchored on the working directory.
  // That is sound because an update is necessarily a local run -- `buck2 run`,
  // or `buck2 test -c snapshot.update=1`, which selects a local-only executor
  // that starts at the project root. A remotely executed test has no source
  // tree to rewrite at all, and fails below on reading the source rather than
  // corrupting anything.
  std::error_code cwd_error;
  const std::filesystem::path cwd = std::filesystem::current_path(cwd_error);
  if (cwd_error) {
    updates().clear();
    return "cannot determine the working directory: " + cwd_error.message() + "\n";
  }

  // A store is needed only by the updates that use one.
  //
  // An inline rewrite goes to the path std::source_location reported and never
  // touches a store, so demanding SNAPSHOT_ROOT up front made inline snapshots
  // unupdatable in any module without a `.snapshots/` directory -- which is
  // every module that has not yet created a file snapshot.
  bool has_file_updates = false;
  for (const PendingUpdate &pending : updates()) {
    if (pending.kind != PendingUpdate::Kind::Inline) {
      has_file_updates = true;
      break;
    }
  }
  if (has_file_updates) {
    const char* const store = std::getenv("SNAPSHOT_SOURCE_ROOT");
    if (store == nullptr || *store == '\0') {
      updates().clear();
      return "SNAPSHOT_SOURCE_ROOT must be set to update file snapshots\n";
    }
  }

  struct UpdateLock {
    int fd = -1;
    UpdateLock() = default;
    UpdateLock(const UpdateLock &) = delete;
    UpdateLock &operator=(const UpdateLock &) = delete;
    UpdateLock(UpdateLock &&other) noexcept : fd(other.fd) { other.fd = -1; }
    UpdateLock &operator=(UpdateLock &&) = delete;
    ~UpdateLock() {
      if (fd >= 0) {
        ::flock(fd, LOCK_UN);
        ::close(fd);
      }
    }
  };

  // One lock per directory holding a file to rewrite.
  //
  // Rewrites are per source file, and two test processes can only collide over
  // a file they both rewrite -- which in this layout means the same module
  // directory. Locking those directories needs no store to exist, which is what
  // lets a module with no `.snapshots/` update its inline snapshots. File
  // snapshot writes need no lock of their own: each is keyed by a UUID that
  // exactly one assertion owns.
  //
  // Taken in sorted order, so two processes whose directory sets overlap cannot
  // deadlock against each other.
  std::set<std::filesystem::path> lock_dirs;
  for (const PendingUpdate &pending : updates())
    lock_dirs.insert((cwd / pending.file).parent_path());

  std::vector<UpdateLock> locks;
  for (const std::filesystem::path &dir : lock_dirs) {
    const std::filesystem::path lock_path = dir / ".update.lock";
    UpdateLock lock;
    lock.fd = ::open(lock_path.c_str(), O_CREAT | O_RDWR, 0666);
    if (lock.fd < 0 || ::flock(lock.fd, LOCK_EX) != 0) {
      updates().clear();
      return "cannot lock snapshot updates at " + lock_path.string() + "\n";
    }
    locks.push_back(std::move(lock));
  }
  struct Write {
    std::filesystem::path target;
    std::string bytes;
    std::filesystem::path temp;
  };
  std::map<std::string, std::vector<Update>> inline_by_file;
  std::map<std::string, std::vector<Update>> ids_by_file;
  std::vector<Write> writes;
  std::string errors;

  for (const PendingUpdate &pending : updates()) {
    if (pending.kind == PendingUpdate::Kind::Inline) {
      inline_by_file[pending.file].push_back(
          {pending.line, pending.column, pending.old_value, pending.new_value});
      continue;
    }
    if (pending.id.empty() || !canonical_uuid(pending.id)) {
      errors += "invalid generated file snapshot id\n";
      continue;
    }
    // Re-read through the read path, to confirm the snapshot still holds what
    // the test compared against; write through the source path.
    const auto path = snapshot_path(pending.id);
    const ReadResult current = read_bytes(path);
    if ((current.ok &&
         (!pending.existed || current.bytes != pending.old_value)) ||
        (current.missing && pending.existed) ||
        (!current.ok && !current.missing)) {
      errors += current.ok ? "file snapshot changed since the test ran: " +
                                 path.string() + "\n"
                           : current.error + "\n";
      continue;
    }
    writes.push_back({cwd / snapshot_source_path(pending.id),
                      pending.new_value, {}});
    if (pending.initialize)
      ids_by_file[pending.file].push_back(
          {pending.line, pending.column, "", pending.id});
  }

  std::map<std::string, bool> source_names;
  for (const auto &item : inline_by_file)
    source_names[item.first] = true;
  for (const auto &item : ids_by_file)
    source_names[item.first] = true;
  for (const auto &item : source_names) {
    const std::string &name = item.first;
    // Project-relative, as compiled; anchored like everything else here.
    const std::filesystem::path path = cwd / name;
    const ReadResult source = read_bytes(path);
    if (!source.ok) {
      errors += source.error + "\n";
      continue;
    }

    std::string changed_text = source.bytes;
    const auto ids = ids_by_file.find(name);
    if (ids != ids_by_file.end()) {
      const UpdateResult changed =
          apply_filesnap_id_updates(changed_text, ids->second);
      if (!changed.ok) {
        errors += name + ": " + changed.error + "\n";
        continue;
      }
      changed_text = changed.text;
    }

    auto inline_updates = inline_by_file[name];
    // UUID insertion changes only columns later on that same source line.
    if (ids != ids_by_file.end()) {
      for (Update &update : inline_updates) {
        for (const Update &id : ids->second) {
          if (id.line == update.line && id.column < update.column)
            update.column += static_cast<unsigned>(id.new_value.size());
        }
      }
    }
    if (!inline_updates.empty()) {
      const UpdateResult changed = apply_updates(changed_text, inline_updates);
      if (!changed.ok) {
        errors += name + ": " + changed.error + "\n";
        continue;
      }
      changed_text = changed.text;
    }
    writes.push_back({path, std::move(changed_text), {}});
  }
  if (!errors.empty()) {
    updates().clear();
    return errors;
  }

  // Stage every target before replacing any of them.
  for (std::size_t i = 0; i < writes.size(); ++i) {
    std::error_code ec;
    std::filesystem::create_directories(writes[i].target.parent_path(), ec);
    if (ec) {
      errors += "cannot create " + writes[i].target.parent_path().string() +
                ": " + ec.message() + "\n";
      break;
    }
    writes[i].temp = writes[i].target;
    writes[i].temp +=
        ".tmp." + std::to_string(::getpid()) + "." + std::to_string(i);
    std::ofstream out(writes[i].temp, std::ios::binary | std::ios::trunc);
    if (!out ||
        !(out.write(writes[i].bytes.data(),
                    static_cast<std::streamsize>(writes[i].bytes.size())))) {
      errors += "cannot stage " + writes[i].target.string() + "\n";
      break;
    }
  }
  if (!errors.empty()) {
    for (const Write &write : writes)
      if (!write.temp.empty()) {
        std::error_code ec;
        std::filesystem::remove(write.temp, ec);
      }
    updates().clear();
    return errors;
  }

  std::size_t committed = 0;
  for (Write &write : writes) {
    std::error_code ec;
    std::filesystem::rename(write.temp, write.target, ec);
    if (ec) {
      errors += std::format(
          "commit failed for {} after {}/{} targets were replaced: {}\n",
          write.target.string(), committed, writes.size(), ec.message());
      break;
    }
    ++committed;
  }
  for (Write &write : writes)
    if (!write.temp.empty()) {
      std::error_code ec;
      std::filesystem::remove(write.temp, ec);
    }
  if (errors.empty())
    std::fprintf(stderr, "snapshot: updated %zu target(s)\n", writes.size());
  updates().clear();
  return errors;
}

} // namespace snapshot_testing
