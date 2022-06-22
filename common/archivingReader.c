#include "_cgo_export.h"

struct archive_cookie
archive_init(char* filename, uintptr_t client_data) {
	struct archive *a, *disk;
	struct archive_entry *entry;
	struct archive_cookie ret;
	int err = ARCHIVE_OK;

	ret.err = ARCHIVE_OK;

	a = archive_write_new();
	err = archive_write_set_format_pax_restricted(a);
	if (err != ARCHIVE_OK) {
		ret.err = err;
		return ret;
	}

	err = archive_write_open2(a, (void*)client_data, NULL, buffer_write, NULL, NULL);
	if (err != ARCHIVE_OK) {
		ret.err = err;
		return ret;
	}
	
        if (-1 == archive_write_get_bytes_in_last_block(a)) {
		archive_write_set_bytes_in_last_block(a, 1);
	}

	disk = archive_read_disk_new();
	err = archive_read_disk_open(disk, filename);
	if (err != ARCHIVE_OK) {
		ret.err = err;
		return ret;
	}

	entry = archive_entry_new();
	err = archive_read_next_header2(disk, entry);
	if (err != ARCHIVE_OK) {
		ret.err = err;
		return ret;
	}

	ret.a = a;
	ret.disk = disk;
	ret.entry = entry;

	return ret;
}

int
archive_close(struct archive *a, struct archive *disk, struct archive_entry *entry) {
	archive_entry_free(entry);
	archive_read_close(disk);
	archive_read_free(disk);
	archive_write_close(a);
	archive_write_free(a);

	return ARCHIVE_OK;
}

