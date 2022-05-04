// +build linux

package ste

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"syscall"
)

// CreateFile covers the following UNIX properties:
// File Mode, File Type
func (bd *blobDownloader) CreateFile(jptm IJobPartTransferMgr, destination string, size int64, writeThrough bool, t FolderCreationTracker) (file io.WriteCloser, needChunks bool, err error) {
	var sip ISourceInfoProvider
	sip, err = newBlobSourceInfoProvider(jptm)
	unixSIP := sip.(IUNIXPropertyBearingSourceInfoProvider) // Blob may have unix properties.

	err = common.CreateParentDirectoryIfNotExist(destination, t)

	// try to remove the file before we create something else over it
	err = os.Remove(destination)
	if err != nil {
		return
	}

	needChunks = size > 0
	needMakeFile := true
	var mode = uint32(common.DEFAULT_FILE_PERM)
	if unixSIP.HasUNIXProperties() {
		var stat UnixStatAdapter
		stat, err = unixSIP.GetUNIXProperties()

		if stat.Extended() {
			if stat.StatxMask()&STATX_MODE == STATX_MODE {
				mode = stat.FileMode()
			} else {
				mode = 0
			}
		} else {
			mode = stat.FileMode()
		}

		if mode != 0 { // Folders are not necessary to handle
			switch {
			case mode&S_IFBLK == S_IFBLK || mode&S_IFCHR == S_IFCHR:
				// the file is representative of a device and does not need to be written to
				err = unix.Mknod(destination, mode, int(stat.RDevice()))

				needChunks = false
				needMakeFile = false
			case mode&S_IFIFO == S_IFIFO || mode&S_IFSOCK == S_IFSOCK:
				// the file is a pipe and does not need to be written to
				err = unix.Mknod(destination, mode, 0)

				needChunks = false
				needMakeFile = false
			case mode&S_IFLNK == S_IFLNK:
				// the file is a symlink and does not need to be written to
				// TODO: cause symlinks to be uploaded this way
				// TODO: get link path
				err = unix.Link("", destination)

				needChunks = false
				needMakeFile = false
			}
		}
	}

	if needMakeFile {
		flags := os.O_RDWR | os.O_CREATE | os.O_TRUNC
		if writeThrough {
			flags |= os.O_SYNC
		}

		file, err = os.OpenFile(destination, flags, os.FileMode(mode))
		if err != nil {
			return
		}

		if size == 0 {
			return
		}

		err = syscall.Fallocate(int(file.(*os.File).Fd()), 0, 0, size)
		if err == syscall.ENOTSUP {
			err = file.(*os.File).Truncate(size) // err will get returned at the end
		}
	}

	return
}
