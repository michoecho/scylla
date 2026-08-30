#include "address_decoder/address_decoder.h"

#include <array>
#include <cerrno>
#include <memory>
#include <optional>
#include <string_view>
#include <condition_variable>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <poll.h>
#include <spawn.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "address_decoder/json.h"

extern char** environ;

namespace addrdec {

std::string decoded_address::to_string() const {
    if (frames.empty()) {
        return {};
    }
    std::string out;
    for (std::size_t i = 0; i < frames.size(); ++i) {
        const source_frame& f = frames[i];
        if (i != 0) {
            out += "\n                       (inlined by) ";
        }
        out += f.function.empty() ? std::string("??") : f.function;
        if (!f.file.empty()) {
            out += " at " + f.file;
            if (f.line != 0) {
                out += ":" + std::to_string(f.line);
                if (f.column != 0) {
                    out += ":" + std::to_string(f.column);
                }
            }
        }
    }
    return out;
}

namespace {

std::string symbolizer_binary(const std::string& configured) {
    if (!configured.empty()) {
        return configured;
    }
    if (const char* const from_env = std::getenv("TRACE_SYMBOLIZER")) {
        return from_env;
    }
    return "llvm-symbolizer";
}

// One llvm-symbolizer, bound to one object, alive for as long as the worker is.
//
// The protocol is one address per line in, one JSON object per line out, and it
// is strictly in lockstep -- the symbolizer answers *every* line, including a
// blank one and an unparseable one, so a reply can never be attributed to the
// wrong request. That is what makes a persistent process safe here; a format
// that dropped malformed input would desynchronise the stream permanently.
class symbolizer_process {
public:
    // Starts the process. `ok()` is false if it could not be started; a caller
    // holding a dead one answers everything as unresolved.
    // `cancel_fd` is the read end of a pipe belonging to the caller; a byte
    // written to its other end unblocks whatever read is in progress. It is
    // never read from, only polled.
    symbolizer_process(const std::string& binary, const std::string& object_path, int cancel_fd)
        : cancel_fd_(cancel_fd) {
        int to_child[2] = {-1, -1};
        int from_child[2] = {-1, -1};
        if (::pipe(to_child) != 0) {
            return;
        }
        if (::pipe(from_child) != 0) {
            ::close(to_child[0]);
            ::close(to_child[1]);
            return;
        }

        // posix_spawn rather than fork: this runs on a worker thread of a
        // process that is several hundred megabytes of GUI, and fork from a
        // thread in a program with other threads holding locks is a trap even
        // when the child only execs.
        posix_spawn_file_actions_t actions;
        posix_spawn_file_actions_init(&actions);
        posix_spawn_file_actions_adddup2(&actions, to_child[0], STDIN_FILENO);
        posix_spawn_file_actions_adddup2(&actions, from_child[1], STDOUT_FILENO);
        posix_spawn_file_actions_addclose(&actions, to_child[1]);
        posix_spawn_file_actions_addclose(&actions, from_child[0]);
        // The symbolizer's own complaints go nowhere: a missing debug section
        // is a normal thing to hit and is already visible as an unresolved
        // frame, and stderr here is the viewer's terminal.
        posix_spawn_file_actions_addopen(&actions, STDERR_FILENO, "/dev/null", O_WRONLY, 0);

        const std::string obj = "--obj=" + object_path;
        // --output-style=JSON is the whole reason this can be a persistent
        // pipe: it is one self-delimiting record per line, where the textual
        // output is a variable number of lines whose end is only known when the
        // *next* address begins.
        std::vector<std::string> argv_storage = {
            binary, obj, "--output-style=JSON", "--demangle", "--inlines",
            // --functions=linkage rather than =short: only the linkage form
            // falls back to the ELF symbol table when there is no debug info
            // for an address, and half of a real backtrace is libc and libstdc++
            // frames that have nothing else. With =short those come back blank.
            "--functions=linkage",
        };
        std::vector<char*> argv;
        argv.reserve(argv_storage.size() + 1);
        for (std::string& s : argv_storage) {
            argv.push_back(s.data());
        }
        argv.push_back(nullptr);

        const int rc =
            ::posix_spawnp(&pid_, binary.c_str(), &actions, nullptr, argv.data(), environ);
        posix_spawn_file_actions_destroy(&actions);
        ::close(to_child[0]);
        ::close(from_child[1]);
        if (rc != 0) {
            ::close(to_child[1]);
            ::close(from_child[0]);
            pid_ = -1;
            return;
        }

        in_ = ::fdopen(to_child[1], "w");
        out_fd_ = from_child[0];
        if (in_ == nullptr) {
            shutdown();
        }
    }

    ~symbolizer_process() { shutdown(); }

    symbolizer_process(const symbolizer_process&) = delete;
    symbolizer_process& operator=(const symbolizer_process&) = delete;

    bool ok() const { return in_ != nullptr && out_fd_ >= 0; }

    ::pid_t pid() const { return pid_; }

    // One round trip. Returns the frames, innermost first; empty for anything
    // the symbolizer could not place. A write or read failure kills the process
    // for good -- ok() goes false and the worker stops trying.
    std::vector<source_frame> lookup(std::uint64_t file_offset) {
        if (!ok()) {
            return {};
        }
        if (std::fprintf(in_, "0x%llx\n", static_cast<unsigned long long>(file_offset)) < 0 ||
            std::fflush(in_) != 0) {
            shutdown();
            return {};
        }
        std::string line;
        if (!read_line(line)) {
            shutdown();
            return {};
        }
        return parse_symbolizer_reply(line);
    }

private:
    // One line, or false if the stream ended or the read was cancelled.
    //
    // poll() rather than a blocking read, and a cancel fd alongside the pipe,
    // because shutting down has to interrupt a read in progress rather than
    // wait for it -- and it cannot get there by killing the child. A symbolizer
    // dying does close its stdout, but only if nothing else holds the write
    // end; anything the child spawned that inherited it keeps the pipe open and
    // the reader blocked forever. Waiting on a fd the *caller* controls does
    // not depend on the child's behaviour at all.
    bool read_line(std::string& out) {
        out.clear();
        for (;;) {
            if (const std::size_t nl = pending_.find('\n'); nl != std::string::npos) {
                out.assign(pending_, 0, nl);
                pending_.erase(0, nl + 1);
                return true;
            }
            struct pollfd fds[2];
            fds[0] = {out_fd_, POLLIN, 0};
            fds[1] = {cancel_fd_, POLLIN, 0};
            const int ready = ::poll(fds, cancel_fd_ >= 0 ? 2 : 1, -1);
            if (ready < 0) {
                if (errno == EINTR) {
                    continue;
                }
                return false;
            }
            if (cancel_fd_ >= 0 && (fds[1].revents & (POLLIN | POLLHUP)) != 0) {
                return false;
            }
            std::array<char, 8192> buffer;
            const ssize_t n = ::read(out_fd_, buffer.data(), buffer.size());
            if (n < 0 && errno == EINTR) {
                continue;
            }
            if (n <= 0) {
                return false;
            }
            pending_.append(buffer.data(), static_cast<std::size_t>(n));
        }
    }

    void shutdown() {
        // Closing stdin is how llvm-symbolizer is asked to stop: it reads to
        // EOF and exits. Reaping it matters -- a viewer that opens a dozen
        // objects over a session would otherwise leave a dozen zombies.
        if (in_ != nullptr) {
            std::fclose(in_);
            in_ = nullptr;
        }
        if (out_fd_ >= 0) {
            ::close(out_fd_);
            out_fd_ = -1;
        }
        if (pid_ > 0) {
            // Killed rather than merely waited for: stdin is closed above, so a
            // symbolizer between answers exits on its own, but one in the
            // middle of indexing an object would carry on for minutes reading
            // debug info nobody is going to ask about.
            ::kill(pid_, SIGKILL);
            int status = 0;
            ::waitpid(pid_, &status, 0);
            pid_ = -1;
        }
    }

    ::pid_t pid_ = -1;
    std::FILE* in_ = nullptr;
    int out_fd_ = -1;
    const int cancel_fd_ = -1;
    std::string pending_;
};

}  // namespace

std::vector<source_frame> parse_symbolizer_reply(std::string_view line) {
    const std::optional<json::value> doc = json::parse(line);
    if (!doc) {
        return {};
    }
    const json::array* const symbols = (*doc)["Symbol"].as_array();
    if (symbols == nullptr) {
        return {};
    }
    std::vector<source_frame> out;
    out.reserve(symbols->size());
    for (const json::value& sym : *symbols) {
        source_frame frame;
        frame.function = sym.string_or("FunctionName");
        frame.file = sym.string_or("FileName");
        frame.line = static_cast<std::uint32_t>(sym.int_or("Line"));
        frame.column = static_cast<std::uint32_t>(sym.int_or("Column"));
        // An address that hit nothing still gets a record, with every field
        // blank or zero. Reporting that as a frame would print "?? at :0" under
        // every unresolvable address; reporting nothing lets the caller fall
        // back to the bare address.
        if (frame.function.empty() && frame.file.empty()) {
            continue;
        }
        out.push_back(std::move(frame));
    }
    return out;
}

// --- the worker ---------------------------------------------------------------

namespace {

// One object file's worker: a thread, a queue into it, and the symbolizer it
// owns. The symbolizer is spawned by the thread on its first request rather
// than in the constructor, so that the cost of indexing an object is paid by
// the object that is actually asked about.
class worker {
public:
    worker(std::string binary, std::string object_path,
           std::mutex& results_lock, std::vector<decoded_address>& results)
        : binary_(std::move(binary)),
          object_path_(std::move(object_path)),
          results_lock_(results_lock),
          results_(results) {
        // The cancel pipe outlives every symbolizer this worker spawns, so the
        // destructor can always reach it without racing the worker thread over
        // the process object's lifetime. A worker that could not make one still
        // runs; it just cannot interrupt a read, which is the behaviour this
        // module had before the pipe existed.
        int fds[2] = {-1, -1};
        if (::pipe(fds) == 0) {
            cancel_read_ = fds[0];
            cancel_write_ = fds[1];
        }
        thread_ = std::thread([this] { run(); });
    }

    // Stopping has to interrupt a lookup in progress, not wait for it.
    //
    // A worker blocked reading its symbolizer's answer is blocked for as long
    // as that answer takes, and the first answer out of a 500 MB object is
    // minutes -- so a destructor that only sets a flag and joins would hang the
    // program on exit for exactly the object that made this module necessary.
    // Killing the child makes the blocked read return EOF, which the worker
    // already treats as the process being gone.
    ~worker() {
        ::pid_t victim = -1;
        {
            const std::lock_guard<std::mutex> guard(lock_);
            stopping_ = true;
            victim = pid_;
        }
        // Two independent nudges, because neither alone covers both states the
        // worker can be in. The byte unblocks a read in progress; the signal
        // stops a symbolizer that is grinding through an object's debug info
        // and would otherwise keep a core busy until it finished. A spawn
        // racing this destructor is caught on the other side: the worker
        // re-checks stopping_ once it has a pid.
        if (cancel_write_ >= 0) {
            const char byte = 'x';
            [[maybe_unused]] const ssize_t written = ::write(cancel_write_, &byte, 1);
        }
        if (victim > 0) {
            ::kill(victim, SIGKILL);
        }
        wake_.notify_all();
        thread_.join();
        if (cancel_read_ >= 0) {
            ::close(cancel_read_);
        }
        if (cancel_write_ >= 0) {
            ::close(cancel_write_);
        }
    }

    void push(std::uint64_t address, std::uint64_t file_offset) {
        {
            const std::lock_guard<std::mutex> guard(lock_);
            queue_.push_back({address, file_offset});
        }
        wake_.notify_one();
    }

    bool failed() const {
        const std::lock_guard<std::mutex> guard(lock_);
        return failed_;
    }

private:
    struct item {
        std::uint64_t address;
        std::uint64_t file_offset;
    };

    void run() {
        std::unique_lock<std::mutex> guard(lock_);
        for (;;) {
            wake_.wait(guard, [this] { return stopping_ || !queue_.empty(); });
            if (stopping_) {
                return;
            }
            const item next = queue_.front();
            queue_.pop_front();

            // The worker's own cache. The decoder above dedupes too, so this is
            // rarely hit in the viewer -- it is here so that the worker is
            // correct on its own terms, and so that a caller which does not
            // dedupe (a headless dump, a test) still pays for each address once.
            if (const auto found = cache_.find(next.file_offset); found != cache_.end()) {
                std::vector<source_frame> frames = found->second;
                guard.unlock();
                publish(next.address, std::move(frames));
                guard.lock();
                continue;
            }

            // Everything below talks to the subprocess, which is slow and must
            // not hold the queue: a request arriving mid-lookup is queued
            // rather than blocking the caller.
            guard.unlock();
            if (process_ == nullptr) {
                process_ = std::make_unique<symbolizer_process>(binary_, object_path_, cancel_read_);
                {
                    const std::lock_guard<std::mutex> mark(lock_);
                    failed_ = !process_->ok();
                    pid_ = process_->pid();
                    // The destructor may have run while posix_spawn was in
                    // flight, in which case it saw no pid to kill and is now
                    // waiting on the join. Killing here is what makes that
                    // wait finite.
                    if (stopping_ && pid_ > 0) {
                        ::kill(pid_, SIGKILL);
                    }
                }
            }
            std::vector<source_frame> frames = process_->lookup(next.file_offset);
            publish(next.address, frames);
            guard.lock();
            cache_.emplace(next.file_offset, std::move(frames));
        }
    }

    void publish(std::uint64_t address, std::vector<source_frame> frames) {
        const std::lock_guard<std::mutex> guard(results_lock_);
        results_.push_back(decoded_address{address, std::move(frames)});
    }

    const std::string binary_;
    const std::string object_path_;

    mutable std::mutex lock_;
    std::condition_variable wake_;
    std::deque<item> queue_;
    bool stopping_ = false;
    bool failed_ = false;
    // The symbolizer's pid, republished here so the destructor can reach it;
    // the process object itself belongs to the worker thread.
    ::pid_t pid_ = -1;
    // Written by the destructor, polled by the read in progress. Both ends are
    // set once in the constructor and closed after the join, so neither needs
    // the lock.
    int cancel_read_ = -1;
    int cancel_write_ = -1;
    // Keyed by file offset rather than by the caller's address: two processes
    // mapping the same object at different bases are the same lookup.
    std::unordered_map<std::uint64_t, std::vector<source_frame>> cache_;

    // Touched only by the worker thread.
    std::unique_ptr<symbolizer_process> process_;

    std::mutex& results_lock_;
    std::vector<decoded_address>& results_;

    std::thread thread_;
};

}  // namespace

// --- the decoder --------------------------------------------------------------

struct address_decoder::impl {
    std::string binary;

    // Workers are created on demand and never destroyed before the decoder is,
    // so a reference to one stays valid; the map is only ever grown.
    std::map<std::string, std::unique_ptr<worker>> workers;

    // The shared inbox. Workers append under this lock, reap() swaps it empty.
    std::mutex results_lock;
    std::vector<decoded_address> results;

    // The caller-side record: what has been asked and what has come back. Both
    // are touched only from the thread that calls request()/reap().
    std::unordered_set<std::uint64_t> in_flight;
    std::unordered_map<std::uint64_t, decoded_address> known;
};

address_decoder::address_decoder(std::string symbolizer) : impl_(std::make_unique<impl>()) {
    impl_->binary = symbolizer_binary(symbolizer);
}

address_decoder::~address_decoder() = default;

void address_decoder::request(const decode_request& what) {
    if (impl_->known.contains(what.address) || impl_->in_flight.contains(what.address)) {
        return;
    }
    // An address with nowhere to look it up is answered here rather than
    // queued: there is no object, so there is no worker to answer it, and the
    // caller still needs the state machine to leave "sent".
    if (what.object_path.empty()) {
        const std::lock_guard<std::mutex> guard(impl_->results_lock);
        impl_->results.push_back(decoded_address{what.address, {}});
        impl_->in_flight.insert(what.address);
        return;
    }
    std::unique_ptr<worker>& w = impl_->workers[what.object_path];
    if (w == nullptr) {
        w = std::make_unique<worker>(impl_->binary, what.object_path, impl_->results_lock,
                                     impl_->results);
    }
    impl_->in_flight.insert(what.address);
    w->push(what.address, what.file_offset);
}

std::vector<decoded_address> address_decoder::reap() {
    std::vector<decoded_address> out;
    {
        const std::lock_guard<std::mutex> guard(impl_->results_lock);
        out.swap(impl_->results);
    }
    for (const decoded_address& d : out) {
        impl_->in_flight.erase(d.address);
        impl_->known.insert_or_assign(d.address, d);
    }
    return out;
}

const decoded_address* address_decoder::lookup(std::uint64_t address) const {
    const auto found = impl_->known.find(address);
    return found == impl_->known.end() ? nullptr : &found->second;
}

std::size_t address_decoder::outstanding() const {
    return impl_->in_flight.size();
}

bool address_decoder::any_worker_failed() const {
    for (const auto& [path, w] : impl_->workers) {
        if (w != nullptr && w->failed()) {
            return true;
        }
    }
    return false;
}

}  // namespace addrdec
