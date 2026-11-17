/* Linux-only LD_PRELOAD fault injection for the manual publication crash check.
 * Stop the entire disposable writer process at a successful filesystem boundary.
 * No code in the library or its normal executable depends on this shim.
 */
#define _GNU_SOURCE
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <unistd.h>

static int stopped;

static void boundary(const char *stage, const char *path) {
    const char *selected = getenv("TTF_CRASH_STAGE");
    const char *root = getenv("TTF_CRASH_ROOT");
    const char *version = getenv("TTF_CRASH_VERSION");
    if (!selected || !root || !version || strcmp(selected, stage) != 0)
        return;
    char expected[PATH_MAX];
    const char *name = strstr(stage, "log-") == stage ? version :
                       strcmp(stage, "current-synced") == 0 ? "CURRENT.tmp" : "CURRENT";
    int length = snprintf(expected, sizeof(expected), "%s/_timeseries_log/%s", root, name);
    if (length < 0 || length >= (int)sizeof(expected) || strcmp(path, expected) != 0)
        return;
    if (!__sync_bool_compare_and_swap(&stopped, 0, 1))
        return;
    char marker[128];
    length = snprintf(marker, sizeof(marker), "TTF_CRASH_BOUNDARY=%s\n", stage);
    if (length > 0 && length < (int)sizeof(marker))
        syscall(SYS_write, STDERR_FILENO, marker, length);
    kill(getpid(), SIGSTOP);
}

static int opened(int directory, const char *path, int flags, mode_t mode) {
    int fd = syscall(SYS_openat, directory, path, flags, mode);
    if (fd >= 0 && (flags & O_CREAT) && (flags & O_EXCL))
        boundary("log-created", path);
    return fd;
}

#define OPEN_WRAPPER(name) \
    int name(const char *path, int flags, ...) { \
        mode_t mode = 0; \
        if ((flags & O_CREAT) || (flags & O_TMPFILE) == O_TMPFILE) { \
            va_list args; \
            va_start(args, flags); \
            mode = va_arg(args, mode_t); \
            va_end(args); \
        } \
        return opened(AT_FDCWD, path, flags, mode); \
    }
OPEN_WRAPPER(open)
OPEN_WRAPPER(open64)

int fsync(int fd) {
    int result = syscall(SYS_fsync, fd);
    if (result == 0) {
        char descriptor[64], path[PATH_MAX];
        snprintf(descriptor, sizeof(descriptor), "/proc/self/fd/%d", fd);
        ssize_t length = readlink(descriptor, path, sizeof(path) - 1);
        if (length >= 0) {
            path[length] = '\0';
            boundary("log-synced", path);
            boundary("current-synced", path);
        }
    }
    return result;
}

int rename(const char *from, const char *to) {
    boundary("before-current-rename", to);
    int result = syscall(SYS_rename, from, to);
    if (result == 0)
        boundary("after-current-rename", to);
    return result;
}
