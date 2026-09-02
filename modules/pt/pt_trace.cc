#include "pt/pt_trace.h"

#include "pt/pt_control.h"

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

namespace pt {
namespace {

constexpr const char* enable_env = "PT_TRACE";
constexpr const char* perf_env = "PT_TRACE_PERF";
constexpr const char* dlfilter_env = "PT_TRACE_DLFILTER";
constexpr const char* browser_env = "PT_TRACE_BROWSER";
constexpr const char* event_env = "PT_TRACE_EVENT";
constexpr const char* aux_pages_env = "PT_TRACE_AUX_PAGES";

const char* env_or(const char* name, const char* fallback) {
    const char* value = std::getenv(name);
    return value && *value ? value : fallback;
}

void report(const std::string& message) {
    std::fprintf(stderr, "PT_TRACE: %s\n", message.c_str());
}

bool wait_for_child(pid_t pid, int& status) {
    while (::waitpid(pid, &status, 0) < 0) {
        if (errno == EINTR) {
            continue;
        }
        return false;
    }
    return true;
}

void stop_child(pid_t pid, int signal) {
    if (pid <= 0) {
        return;
    }
    if (::kill(pid, signal) < 0 && errno != ESRCH) {
        return;
    }
    int status = 0;
    (void)wait_for_child(pid, status);
}

std::vector<char*> make_argv(std::vector<std::string>& args) {
    std::vector<char*> argv;
    argv.reserve(args.size() + 1);
    for (auto& arg : args) {
        argv.push_back(arg.data());
    }
    argv.push_back(nullptr);
    return argv;
}

pid_t exec_child(const std::vector<std::string>& command) {
    pid_t pid = ::fork();
    if (pid != 0) {
        return pid;
    }

    // Seastar blocks signals while it owns the test process.  Do not inherit
    // that mask into perf: perf must be able to receive SIGINT when recording
    // is stopped.
    sigset_t signal_mask;
    ::sigemptyset(&signal_mask);
    ::sigprocmask(SIG_SETMASK, &signal_mask, nullptr);

    auto args = command;
    auto argv = make_argv(args);
    ::execvp(argv[0], argv.data());
    std::fprintf(stderr, "PT_TRACE: cannot execute %s: %s\n", argv[0], std::strerror(errno));
    _exit(127);
}

bool process_is_running(pid_t pid) {
    if (::kill(pid, 0) == 0) {
        return true;
    }
    return errno == EPERM;
}

bool wait_for_perf_ready(pid_t pid, const std::string& ctl_fifo) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (std::chrono::steady_clock::now() < deadline) {
        int status = 0;
        const pid_t result = ::waitpid(pid, &status, WNOHANG);
        if (result == pid) {
            report("perf exited before opening its control fifo");
            return false;
        }
        if (result < 0 && errno != EINTR) {
            return false;
        }

        const int fd = ::open(ctl_fifo.c_str(), O_WRONLY | O_NONBLOCK | O_CLOEXEC);
        if (fd >= 0) {
            ::close(fd);
            return true;
        }
        if (errno != ENXIO && errno != EINTR) {
            report("cannot probe the perf control fifo: " + std::string(std::strerror(errno)));
            return false;
        }
        (void)::poll(nullptr, 0, 10);
    }
    report("timed out waiting for perf to open its control fifo");
    return false;
}

bool run_and_wait(const std::vector<std::string>& command) {
    const pid_t pid = exec_child(command);
    if (pid < 0) {
        report("fork failed: " + std::string(std::strerror(errno)));
        return false;
    }
    int status = 0;
    if (!wait_for_child(pid, status)) {
        report("waitpid failed: " + std::string(std::strerror(errno)));
        return false;
    }
    return WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

bool send_all(int fd, const void* data, size_t size) {
    const auto* bytes = static_cast<const char*>(data);
    size_t sent = 0;
    while (sent != size) {
        const ssize_t result = ::send(fd, bytes + sent, size - sent, MSG_NOSIGNAL);
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (result <= 0) {
            return false;
        }
        sent += static_cast<size_t>(result);
    }
    return true;
}

void serve_once(int listen_fd, const std::string& ftf_path, const std::string& filename) {
    const int client_fd = ::accept(listen_fd, nullptr, nullptr);
    if (client_fd < 0) {
        _exit(1);
    }

    char request[4096];
    const ssize_t request_size = ::read(client_fd, request, sizeof(request) - 1);
    if (request_size <= 0) {
        ::close(client_fd);
        _exit(1);
    }
    request[request_size] = '\0';

    const std::string get_prefix = "GET /" + filename + " ";
    if (std::strncmp(request, get_prefix.c_str(), get_prefix.size()) != 0) {
        static constexpr char response[] = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
        (void)send_all(client_fd, response, sizeof(response) - 1);
        ::close(client_fd);
        _exit(1);
    }

    const int input = ::open(ftf_path.c_str(), O_RDONLY | O_CLOEXEC);
    if (input < 0) {
        static constexpr char response[] = "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n";
        (void)send_all(client_fd, response, sizeof(response) - 1);
        ::close(client_fd);
        _exit(1);
    }

    struct stat st {};
    if (::fstat(input, &st) < 0) {
        ::close(input);
        ::close(client_fd);
        _exit(1);
    }
    const std::string headers =
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: application/octet-stream\r\n"
        "Access-Control-Allow-Origin: https://ui.perfetto.dev\r\n"
        "Cache-Control: no-cache\r\n"
        "Content-Length: " + std::to_string(st.st_size) + "\r\n\r\n";
    if (!send_all(client_fd, headers.data(), headers.size())) {
        ::close(input);
        ::close(client_fd);
        _exit(1);
    }

    char buffer[64 * 1024];
    for (;;) {
        const ssize_t count = ::read(input, buffer, sizeof(buffer));
        if (count == 0) {
            break;
        }
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count < 0 || !send_all(client_fd, buffer, static_cast<size_t>(count))) {
            break;
        }
    }
    ::close(input);
    ::close(client_fd);
    ::close(listen_fd);
    _exit(0);
}

void publish_trace(const std::string& ftf_path) {
    const std::string filename = ftf_path.substr(ftf_path.find_last_of('/') + 1);

    const int listen_fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (listen_fd < 0) {
        report("cannot create HTTP socket: " + std::string(std::strerror(errno)));
        return;
    }
    int reuse = 1;
    (void)::setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    sockaddr_in address {};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (::bind(listen_fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0 ||
        ::listen(listen_fd, 1) < 0) {
        report("cannot start HTTP server: " + std::string(std::strerror(errno)));
        ::close(listen_fd);
        return;
    }
    socklen_t address_size = sizeof(address);
    (void)::getsockname(listen_fd, reinterpret_cast<sockaddr*>(&address), &address_size);

    const std::string url = "https://ui.perfetto.dev/#!/?url=http://127.0.0.1:" +
        std::to_string(ntohs(address.sin_port)) + "/" + filename +
        "&referrer=scylla-pt-trace";
    const pid_t server_pid = ::fork();
    if (server_pid == 0) {
        serve_once(listen_fd, ftf_path, filename);
    }
    if (server_pid < 0) {
        report("cannot fork HTTP server: " + std::string(std::strerror(errno)));
        ::close(listen_fd);
        return;
    }
    ::close(listen_fd);

    report("serving " + ftf_path + " at " + url);
    const char* browser = std::getenv(browser_env);
    if (!browser || !*browser) {
        browser = "xdg-open";
    }
    if (std::strcmp(browser, "none") == 0) {
        return;
    }

    // The command is intentionally a shell command so a browser can be
    // configured with flags.  The URL is passed as quoted $1, not interpolated
    // into the command string.
    const pid_t browser_pid = ::fork();
    if (browser_pid == 0) {
        const std::string command = std::string(browser) + " \"$1\"";
        ::execl("/bin/sh", "sh", "-c", command.c_str(), "pt-trace-browser", url.c_str(), nullptr);
        _exit(127);
    }
    if (browser_pid < 0) {
        report("cannot fork browser: " + std::string(std::strerror(errno)));
    }
    (void)server_pid;
}

} // namespace

struct perf_trace::impl {
    pid_t perf_pid = -1;
    std::string directory;
    std::string ctl_fifo;
    std::string ack_fifo;
    std::string perf_data;
    std::string ftf;
    bool stopped = false;
};

std::unique_ptr<perf_trace> perf_trace::start_if_requested() {
    if (std::getenv(enable_env) == nullptr) {
        return nullptr;
    }

    auto result = std::unique_ptr<perf_trace>(new perf_trace);
    result->_impl = std::make_unique<impl>();
    auto& state = *result->_impl;

    const char* tmpdir = env_or("TMPDIR", "/tmp");
    std::string pattern = std::string(tmpdir) + "/scylla-pt-trace.XXXXXX";
    std::vector<char> mutable_pattern(pattern.begin(), pattern.end());
    mutable_pattern.push_back('\0');
    if (!::mkdtemp(mutable_pattern.data())) {
        report("cannot create temporary directory: " + std::string(std::strerror(errno)));
        return nullptr;
    }
    state.directory = mutable_pattern.data();
    state.ctl_fifo = state.directory + "/control.fifo";
    state.ack_fifo = state.directory + "/ack.fifo";
    state.perf_data = state.directory + "/perf.data";
    state.ftf = state.directory + "/perf.ftf";

    if (::mkfifo(state.ctl_fifo.c_str(), 0600) < 0 || ::mkfifo(state.ack_fifo.c_str(), 0600) < 0) {
        report("cannot create control fifos: " + std::string(std::strerror(errno)));
        ::unlink(state.ctl_fifo.c_str());
        ::unlink(state.ack_fifo.c_str());
        ::rmdir(state.directory.c_str());
        return nullptr;
    }

    (void)::setenv("PERF_CTL_FIFO", state.ctl_fifo.c_str(), 1);
    (void)::setenv("PERF_ACK_FIFO", state.ack_fifo.c_str(), 1);
    pt::resolve();

    const std::string perf = env_or(perf_env, "perf");
    const std::string event = env_or(event_env, "intel_pt/cyc=1/u");
    const std::string aux_pages = env_or(aux_pages_env, "8M");
    const std::string control = "fifo:" + state.ctl_fifo + "," + state.ack_fifo;
    const std::vector<std::string> command = {
        perf, "record", "--mmap-pages=," + aux_pages, "-e", event,
        "-o", state.perf_data, "--delay=-1", "--control", control,
        "-p", std::to_string(::getpid()),
    };
    state.perf_pid = exec_child(command);
    if (state.perf_pid < 0 || !wait_for_perf_ready(state.perf_pid, state.ctl_fifo)) {
        if (state.perf_pid > 0) {
            stop_child(state.perf_pid, SIGTERM);
        }
        ::unlink(state.ctl_fifo.c_str());
        ::unlink(state.ack_fifo.c_str());
        ::rmdir(state.directory.c_str());
        return nullptr;
    }

    report("recording Boost test run; perf.data will be " + state.perf_data);
    return result;
}

perf_trace::~perf_trace() {
    stop();
}

void perf_trace::stop() {
    if (!_impl || _impl->stopped) {
        return;
    }
    auto& state = *_impl;
    state.stopped = true;

    if (process_is_running(state.perf_pid)) {
        pt::disable();
        stop_child(state.perf_pid, SIGINT);
    } else {
        int status = 0;
        (void)::waitpid(state.perf_pid, &status, WNOHANG);
    }
    state.perf_pid = -1;
    ::unlink(state.ctl_fifo.c_str());
    ::unlink(state.ack_fifo.c_str());

    report("perf.data: " + state.perf_data);
    if (::access(state.perf_data.c_str(), R_OK) < 0) {
        report("perf.data was not produced");
        return;
    }

    const char* dlfilter = std::getenv(dlfilter_env);
    if (!dlfilter || !*dlfilter) {
        report("PT_TRACE_DLFILTER is unset; skipping perf script decode");
        return;
    }
    if (::access(dlfilter, R_OK) < 0) {
        report("dlfilter is not readable at " + std::string(dlfilter));
        return;
    }

    const std::string perf = env_or(perf_env, "perf");
    const std::vector<std::string> script = {
        perf, "script", "-i", state.perf_data, "--itrace=bei0ns",
        "--dlfilter", dlfilter, "--dlarg", state.ftf, "--dlarg", "c",
    };
    if (!run_and_wait(script)) {
        report("perf script decode failed");
        return;
    }
    if (::access(state.ftf.c_str(), R_OK) < 0) {
        report("perf script did not produce " + state.ftf);
        return;
    }
    report("ftf: " + state.ftf);
    publish_trace(state.ftf);
}

} // namespace pt
