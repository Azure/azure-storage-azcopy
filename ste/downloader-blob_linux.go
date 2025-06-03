//go:build linux
// +build linux

package ste

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"syscall"
	"time"
)

// CreateFile covers the following UNIX properties:
// File Mode, File Type
func (bd *blobDownloader) CreateFile(jptm IJobPartTransferMgr, destination string, size int64, writeThrough bool, t FolderCreationTracker) (file io.WriteCloser, needChunks bool, err error) {
	var sip ISourceInfoProvider
	sip, err = newBlobSourceInfoProvider(jptm)
	if err != nil {
		return
	}

	unixSIP := sip.(IUNIXPropertyBearingSourceInfoProvider) // Blob may have unix properties.

	err = common.CreateParentDirectoryIfNotExist(destination, t)
	if err != nil {
		return
	}

	// try to remove the file before we create something else over it
	_ = os.Remove(destination)

	needChunks = (size > 0 || jptm.ShouldDecompress())
	needMakeFile := true
	var mode = uint32(common.DEFAULT_FILE_PERM)
	if jptm.Info().PreservePOSIXProperties && unixSIP.HasUNIXProperties() {
		var stat common.UnixStatAdapter
		stat, err = unixSIP.GetUNIXProperties()

		if stat.Extended() {
			if stat.StatxMask()&common.STATX_MODE == common.STATX_MODE { // We need to retain access to the file until we're well & done with it
				mode = stat.FileMode() | common.DEFAULT_FILE_PERM
			}
		} else {
			mode = stat.FileMode() | common.DEFAULT_FILE_PERM
		}

		if mode != 0 { // Folders & Symlinks are not necessary to handle
			switch {
			case mode&common.S_IFBLK == common.S_IFBLK || mode&common.S_IFCHR == common.S_IFCHR:
				// the file is representative of a device and does not need to be written to
				err = unix.Mknod(destination, mode, int(stat.RDevice()))

				needChunks = false
				needMakeFile = false
			case mode&common.S_IFIFO == common.S_IFIFO || mode&common.S_IFSOCK == common.S_IFSOCK:
				// the file is a pipe and does not need to be written to
				err = unix.Mknod(destination, mode, 0)

				needChunks = false
				needMakeFile = false
			}
		}
	}

	if !needMakeFile {
		return
	}

	flags := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	if writeThrough {
		flags |= os.O_SYNC
	}

	file, err = os.OpenFile(destination, flags, os.FileMode(mode)) // os.FileMode is uint32 on Linux.
	if err != nil {
		return
	}

	if size == 0 {
		return
	}

	for i := 0; i < common.EINTR_RETRY_COUNT; i++ {
		err = syscall.Fallocate(int(file.(*os.File).Fd()), 0, 0, size)
		if err == nil || err != syscall.EINTR {
			break
		}
	}

	if err == syscall.ENOTSUP {
		err = file.(*os.File).Truncate(size) // err will get returned at the end
	}

	return
}

func (bd *blobDownloader) ApplyUnixProperties(adapter common.UnixStatAdapter) (stage string, err error) {
	// At this point, mode has already been applied. Let's work out what we need to apply, and apply the rest.
	destination := bd.txInfo.Destination

	// First, grab our file descriptor and such.
	fi, err := os.Stat(destination)
	if err != nil {
		return "stat", err
	}

	// At this point, mode has already been applied. Let's work out what we need to apply, and apply the rest.
	if adapter.Extended() {
		stat := fi.Sys().(*syscall.Stat_t)
		mask := adapter.StatxMask()

		// stx_attributes is not persisted.

		mode := os.FileMode(common.DEFAULT_FILE_PERM)
		if common.StatXReturned(mask, common.STATX_MODE) {
			mode = os.FileMode(adapter.FileMode())
		}

		err = os.Chmod(destination, mode)
		if err != nil {
			return "chmod", err
		}

		uid := stat.Uid
		if common.StatXReturned(mask, common.STATX_UID) {
			uid = adapter.Owner()
		}

		gid := stat.Gid
		if common.StatXReturned(mask, common.STATX_GID) {
			gid = adapter.Group()
		}
		// set ownership
		err = os.Chown(destination, int(uid), int(gid))
		if err != nil {
			return "chown", err
		}

		atime := time.Unix(stat.Atim.Unix())
		if common.StatXReturned(mask, common.STATX_ATIME) || !adapter.ATime().IsZero() { // workaround for noatime when underlying fs supports atime
			atime = adapter.ATime()
		}

		mtime := time.Unix(stat.Mtim.Unix())
		if common.StatXReturned(mask, common.STATX_MTIME) {
			mtime = adapter.MTime()
		}

		// adapt times
		err = os.Chtimes(destination, atime, mtime)
		if err != nil {
			return "chtimes", err
		}
	} else {
		err = os.Chmod(destination, os.FileMode(adapter.FileMode())) // only write permissions
		if err != nil {
			return "chmod", err
		}
		err = os.Chown(destination, int(adapter.Owner()), int(adapter.Group()))
		if err != nil {
			return "chown", err
		}
		err = os.Chtimes(destination, adapter.ATime(), adapter.MTime())
		if err != nil {
			return "chtimes", err
		}
	}

	return
}

func (bd *blobDownloader) SetFolderProperties(jptm IJobPartTransferMgr) error {
	sip, err := newBlobSourceInfoProvider(jptm)
	if err != nil {
		return err
	}

	bd.txInfo = jptm.Info() // inform our blobDownloader a bit.

	usip := sip.(IUNIXPropertyBearingSourceInfoProvider)
	if usip.HasUNIXProperties() {
		props, err := usip.GetUNIXProperties()
		if err != nil {
			return err
		}
		stage, err := bd.ApplyUnixProperties(props)

		if err != nil {
			return fmt.Errorf("set unix properties: %s; %w", stage, err)
		}
	}

	return nil
}
