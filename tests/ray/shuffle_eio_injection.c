// Linux-only fault injection for the real Rust shuffle file path. No production hooks.
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <unistd.h>

static int fail_io(const char *path, const char *operation, off_t offset) {
    const char *root = getenv("DAFT_TEST_SHUFFLE_IO_ROOT");
    const char *op = getenv("DAFT_TEST_SHUFFLE_IO_OPERATION");
    const char *sequence = getenv("DAFT_TEST_SHUFFLE_IO_SEQUENCE");
    if (!root || !op || (!sequence && strcmp(op, operation)) || !path || strncmp(path, root, strlen(root))) return 0;
    if (!strstr(path, "/daft_shuffle/") || !strstr(path, ".arrow")) return 0;
    const char *minimum = getenv("DAFT_TEST_SHUFFLE_IO_MIN_OFFSET");
    if (minimum && offset < atoll(minimum)) return 0;
    const char *limit = getenv("DAFT_TEST_SHUFFLE_IO_FAILURES");
    int count = limit ? atoi(limit) : 1;
    char steps[256];
    char *next = NULL;
    if (sequence) {
        snprintf(steps, sizeof(steps), "%s", sequence);
        count = 1;
        for (const char *p = sequence; *p; ++p) if (*p == ',') count++;
    }
    for (int i = 0; i < count; ++i) {
        char marker[4096];
        snprintf(marker, sizeof(marker), "%s/fault-%d", root, i);
        if (sequence) {
            const char *step = strtok_r(i == 0 ? steps : NULL, ",", &next);
            if (!step) return 0;
            if (syscall(SYS_faccessat, AT_FDCWD, marker, F_OK) == 0) continue;
            if (strcmp(step, operation)) return 0;
        }
        int fd = syscall(SYS_openat, AT_FDCWD, marker, O_WRONLY | O_CREAT | O_EXCL, 0600);
        if (fd < 0) continue;
        char detail[8192];
        int n = snprintf(detail, sizeof(detail), "%s %ld %lld %s\n", operation, (long)getpid(), (long long)offset, path);
        syscall(SYS_write, fd, detail, n);
        syscall(SYS_close, fd);
        const char *code = getenv("DAFT_TEST_SHUFFLE_IO_ERRNO");
        errno = code ? atoi(code) : EIO;
        return 1;
    }
    return 0;
}

static void trace_fd(int fd, const char *prefix) {
    const char *root = getenv("DAFT_TEST_SHUFFLE_IO_ROOT");
    if (!root || !getenv("DAFT_TEST_SHUFFLE_IO_TRACE_SYNC")) return;
    char link[64], path[4096], marker[4096];
    snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
    ssize_t n = syscall(SYS_readlink, link, path, sizeof(path) - 1);
    if (n < 0) return;
    path[n] = 0;
    if (strncmp(path, root, strlen(root)) || !strstr(path, "/daft_shuffle/")) return;
    snprintf(marker, sizeof(marker), "%s/%s-%ld-%d", root, prefix, (long)getpid(), fd);
    int out = syscall(SYS_openat, AT_FDCWD, marker, O_WRONLY | O_CREAT | O_EXCL, 0600);
    if (out < 0) return;
    syscall(SYS_write, out, path, n);
    syscall(SYS_close, out);
}

static int fail_fd(int fd, const char *operation, off_t offset) {
    char link[64], path[4096];
    snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
    ssize_t n = syscall(SYS_readlink, link, path, sizeof(path) - 1);
    if (n < 0) return 0;
    path[n] = 0;
    return fail_io(path, operation, offset);
}

int open64(const char *path, int flags, ...) {
    mode_t mode = 0;
    if ((flags & O_CREAT) || (flags & O_TMPFILE) == O_TMPFILE) {
        va_list ap; va_start(ap, flags); mode = va_arg(ap, int); va_end(ap);
    }
    if ((flags & O_ACCMODE) == O_RDONLY && fail_io(path, "open_read", 0)) return -1;
    if (fail_io(path, "open", 0)) return -1;
    return syscall(SYS_openat, AT_FDCWD, path, flags, mode);
}

int open(const char *path, int flags, ...) {
    mode_t mode = 0;
    if ((flags & O_CREAT) || (flags & O_TMPFILE) == O_TMPFILE) {
        va_list ap; va_start(ap, flags); mode = va_arg(ap, int); va_end(ap);
    }
    return open64(path, flags, mode);
}

ssize_t read(int fd, void *buf, size_t n) {
    if (fail_fd(fd, "read", syscall(SYS_lseek, fd, 0, SEEK_CUR))) return -1;
    return syscall(SYS_read, fd, buf, n);
}

ssize_t pread64(int fd, void *buf, size_t n, off64_t offset) {
    if (fail_fd(fd, "read", offset)) return -1;
    return syscall(SYS_pread64, fd, buf, n, offset);
}

ssize_t write(int fd, const void *buf, size_t n) {
    if (fail_fd(fd, "write", syscall(SYS_lseek, fd, 0, SEEK_CUR))) return -1;
    return syscall(SYS_write, fd, buf, n);
}

ssize_t pwrite64(int fd, const void *buf, size_t n, off64_t offset) {
    if (fail_fd(fd, "write", offset)) {
        int saved_errno = errno;
        trace_fd(fd, "write-error-fd");
        if (getenv("DAFT_TEST_SHUFFLE_IO_PARTIAL_EFFECT") && n > 1)
            syscall(SYS_pwrite64, fd, buf, n / 2, offset);
        errno = saved_errno;
        return -1;
    }
    const char *short_write = getenv("DAFT_TEST_SHUFFLE_IO_SHORT_WRITE");
    if (short_write && n > 4093) {
        char link[64], path[4096];
        snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
        ssize_t len = syscall(SYS_readlink, link, path, sizeof(path) - 1);
        const char *root = getenv("DAFT_TEST_SHUFFLE_IO_ROOT");
        if (len >= 0 && root) {
            path[len] = 0;
            if (!strncmp(path, root, strlen(root)) && strstr(path, "/daft_shuffle/")) n = 4093;
        }
    }
    return syscall(SYS_pwrite64, fd, buf, n, offset);
}

int fsync(int fd) {
    if (fail_fd(fd, "fsync", 0)) return -1;
    return syscall(SYS_fsync, fd);
}

int fdatasync(int fd) {
    trace_fd(fd, "recovery-sync-fd");
    if (fail_fd(fd, "fdatasync", 0)) return -1;
    return syscall(SYS_fdatasync, fd);
}
