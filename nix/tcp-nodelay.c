/*
 * Force TCP_NODELAY on sockets created by a wrapped process.
 *
 * This is intentionally a small LD_PRELOAD shim. It is useful for local
 * Buck2/NativeLink development until both applications enable TCP_NODELAY
 * themselves. It leaves non-TCP socket options unchanged.
 */

#define _GNU_SOURCE

#include <dlfcn.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stddef.h>
#include <sys/socket.h>

typedef int (*setsockopt_fn)(int, int, int, const void *, socklen_t);
typedef int (*socket_fn)(int, int, int);
typedef int (*accept_fn)(int, struct sockaddr *, socklen_t *);
typedef int (*accept4_fn)(int, struct sockaddr *, socklen_t *, int);

static setsockopt_fn next_setsockopt(void) {
    static setsockopt_fn function;

    if (function == NULL) {
        function = (setsockopt_fn)dlsym(RTLD_NEXT, "setsockopt");
    }
    return function;
}

static int is_tcp_socket(int domain, int type, int protocol) {
    return (domain == AF_INET || domain == AF_INET6) &&
           (type & 0xf) == SOCK_STREAM &&
           (protocol == 0 || protocol == IPPROTO_TCP);
}

static void enable_tcp_nodelay(int fd) {
    static const int enabled = 1;
    int saved_errno = errno;

    (void)next_setsockopt()(fd, IPPROTO_TCP, TCP_NODELAY, &enabled,
                            sizeof(enabled));
    errno = saved_errno;
}

int setsockopt(int fd, int level, int option, const void *value, socklen_t length) {
    setsockopt_fn real_setsockopt = next_setsockopt();

    if (level == IPPROTO_TCP && option == TCP_NODELAY) {
        static const int enabled = 1;
        return real_setsockopt(fd, level, option, &enabled, sizeof(enabled));
    }

    return real_setsockopt(fd, level, option, value, length);
}

int socket(int domain, int type, int protocol) {
    static socket_fn real_socket;

    if (real_socket == NULL) {
        real_socket = (socket_fn)dlsym(RTLD_NEXT, "socket");
    }

    int fd = real_socket(domain, type, protocol);
    if (fd >= 0 && is_tcp_socket(domain, type, protocol)) {
        enable_tcp_nodelay(fd);
    }
    return fd;
}

int accept(int fd, struct sockaddr *address, socklen_t *length) {
    static accept_fn real_accept;

    if (real_accept == NULL) {
        real_accept = (accept_fn)dlsym(RTLD_NEXT, "accept");
    }

    int accepted = real_accept(fd, address, length);
    if (accepted >= 0) {
        enable_tcp_nodelay(accepted);
    }
    return accepted;
}

int accept4(int fd, struct sockaddr *address, socklen_t *length, int flags) {
    static accept4_fn real_accept4;

    if (real_accept4 == NULL) {
        real_accept4 = (accept4_fn)dlsym(RTLD_NEXT, "accept4");
    }

    int accepted = real_accept4(fd, address, length, flags);
    if (accepted >= 0) {
        enable_tcp_nodelay(accepted);
    }
    return accepted;
}
