#include "pt_control.h"

#include <cstdlib>
#include <cstring>
#include <string>

#include <cerrno>
#include <fcntl.h>
#include <unistd.h>

// Implementation notes
// --------------------
// perf's --control protocol is line oriented: we write "<command>\n" to the
// ctl fifo and, once the command has taken effect, perf writes an
// acknowledgement to the ack fifo. We block on that ack so the trace boundary
// is synchronous with the call.
//
// fd caching: opening a fifo blocks until the other end is open, so we don't
// want to pay that (or risk it) on every enable()/disable(). We open each fifo
// once, lazily, and keep the fd. The ctl fifo is opened O_WRONLY (we only
// write commands) and the ack fifo O_RDONLY (we only read acks). perf keeps
// both ends open for the whole session, so the open() calls don't block beyond
// the first round trip.
//
// EOF / reconnect: if perf restarts (or its end of a pipe is otherwise closed)
// a read returns 0 (EOF) and a write fails with EPIPE. We treat that as "the
// cached fd is stale", drop it, and reopen on the next call -- "reconnecting"
// to a fresh perf without tearing down the program.
//
// The ack quirk: the kernel writes the acknowledgement as "ack\n\0" -- four
// bytes including a stray NUL -- rather than "ack\n". We therefore can't match
// on an exact byte count; we just read whatever is available and look for the
// "ack" prefix, tolerating the trailing NUL (and any framing perf may add).

namespace pt {
namespace {

// Env var names; see pt_control.h for the protocol.
constexpr const char* kCtlEnv = "PERF_CTL_FIFO";
constexpr const char* kAckEnv = "PERF_ACK_FIFO";

// Cached fifo file descriptors. -1 means "not open"; we (re)open lazily.
int g_ctl_fd = -1;
int g_ack_fd = -1;

// True once we've checked the environment, so we don't getenv() repeatedly.
bool g_resolved = false;
bool g_active = false; // both fifo paths present in the environment
const char* g_ctl_path = nullptr;
const char* g_ack_path = nullptr;

void resolve() {
    if (g_resolved)
        return;
    g_resolved = true;
    g_ctl_path = std::getenv(kCtlEnv);
    g_ack_path = std::getenv(kAckEnv);
    g_active = g_ctl_path != nullptr && g_ack_path != nullptr;
}

// Open `path` with `flags` if `fd` is not already open. Returns the (possibly
// freshly opened) fd, or -1 on failure. O_CLOEXEC so the traced child we may
// fork later doesn't inherit our control handles.
int ensure_open(int& fd, const char* path, int flags) {
    if (fd >= 0)
        return fd;
    int opened;
    do {
        opened = ::open(path, flags | O_CLOEXEC);
    } while (opened < 0 && errno == EINTR);
    fd = opened;
    return fd;
}

void drop(int& fd) {
    if (fd >= 0) {
        ::close(fd);
        fd = -1;
    }
}

// Write the whole buffer, retrying short writes and EINTR. Returns false on a
// hard error (e.g. EPIPE because perf went away), in which case the caller
// should drop the cached fd and may retry once on a fresh connection.
bool write_all(int fd, const char* buf, size_t len) {
    size_t off = 0;
    while (off < len) {
        ssize_t n = ::write(fd, buf + off, len - off);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            return false;
        }
        off += static_cast<size_t>(n);
    }
    return true;
}

// Send a command + '\n' on the ctl fifo, reopening once if the cached fd is
// stale (EOF/EPIPE from a restarted perf). Returns false if we couldn't get the
// bytes out even on a fresh connection.
bool send_command(const char* cmd) {
    std::string line = cmd;
    line += '\n';

    for (int attempt = 0; attempt < 2; ++attempt) {
        if (ensure_open(g_ctl_fd, g_ctl_path, O_WRONLY) < 0)
            return false;
        if (write_all(g_ctl_fd, line.data(), line.size()))
            return true;
        // Stale connection: drop and reconnect for the second attempt.
        drop(g_ctl_fd);
    }
    return false;
}

// Wait for perf's acknowledgement on the ack fifo. The ack is "ack\n\0" (note
// the kernel's stray trailing NUL), so we read what's available and accept any
// frame containing "ack". A read of 0 is EOF -> reconnect and try once more.
bool wait_ack() {
    for (int attempt = 0; attempt < 2; ++attempt) {
        if (ensure_open(g_ack_fd, g_ack_path, O_RDONLY) < 0)
            return false;

        char buf[16];
        ssize_t n;
        do {
            n = ::read(g_ack_fd, buf, sizeof(buf));
        } while (n < 0 && errno == EINTR);

        if (n >= 3)
            // Accept the ack regardless of the trailing NUL / exact length.
            return std::strncmp(buf, "ack", 3) == 0;
        if (n > 0)
            // A short, non-empty frame is unexpected but not a disconnect;
            // don't reconnect, just report it as not-acked.
            return false;

        // n == 0 (EOF) or error: perf's write end closed. Reconnect and retry.
        drop(g_ack_fd);
    }
    return false;
}

} // namespace

bool enable() {
    resolve();
    if (!g_active)
        return false;
    if (!send_command("enable"))
        return false;
    return wait_ack();
}

void disable() {
    resolve();
    if (!g_active)
        return;
    // Best effort: any failure here is fine -- the consequence is at most some
    // extra trace, so we deliberately ignore the result of both steps.
    if (send_command("disable"))
        (void)wait_ack();
}

} // namespace pt
