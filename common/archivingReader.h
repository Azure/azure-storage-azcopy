#include <archive.h>
#include <archive_entry.h>

struct archive_cookie {
        int err;
        struct archive *a;
        struct archive *disk;
        struct archive_entry *entry;
};

typedef const void* const_void_ptr;

struct archive_cookie archive_init(char* filename, uintptr_t client_data);
int archive_close(struct archive *a, struct archive *disk, struct archive_entry *entry);

extern ssize_t buffer_write(struct archive *a, void *client_data, const void *buff, size_t length);
