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
	_ = os.Remove(destination)

	needChunks = size > 0
	needMakeFile := true
	var mode = uint32(common.DEFAULT_FILE_PERM)
	if jptm.Info().PreservePOSIXProperties.IsTruthy() && unixSIP.HasUNIXProperties() {
		var stat common.UnixStatAdapter
		stat, err = unixSIP.GetUNIXProperties()

		if stat.Extended() {
			if stat.StatxMask()&common.STATX_MODE == common.STATX_MODE { // We need to retain access to the file until we're well & done with it
				mode = stat.FileMode() | common.DEFAULT_FILE_PERM
			}
		} else {
			mode = stat.FileMode() | common.DEFAULT_FILE_PERM
		}

		if mode != 0 { // Folders are not necessary to handle
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
			case mode&common.S_IFLNK == common.S_IFLNK:
				// the file is a symlink and does not need to be written to
				// TODO: cause symlinks to be uploaded this way
				// TODO: get link path
				err = unix.Link("", destination)

				needChunks = false
				needMakeFile = false
			}
		}
	}

	bd.fileMode = mode

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

	err = syscall.Fallocate(int(file.(*os.File).Fd()), 0, 0, size)
	if err == syscall.ENOTSUP {
		err = file.(*os.File).Truncate(size) // err will get returned at the end
	}

	return
}

func (bd *blobDownloader) ApplyUnixProperties(adapter common.UnixStatAdapter) (stage string, err error) {
	// At this point, mode has already been applied. Let's work out what we need to apply, and apply the rest.

	// if adapter.Extended() {
	// 	var stat *syscall.Stat_t
	// 	stat = fi.Sys().(*syscall.Stat_t)
	// 	mask := adapter.StatxMask()
	//
	// 	todo: this is being troublesome.
	// 	attr := adapter.Attribute()
	// 	_, _, err = syscall.Syscall(syscall.SYS_IOCTL, f.Fd(), uintptr(unix.FS_IOC_SETFLAGS), uintptr(attr))
	// 	if err != nil {
	// 		return "ioctl setflags", err
	// 	}
	// }

	err = os.Chmod(bd.txInfo.Destination, os.FileMode(adapter.FileMode())) // only write permissions
	if err != nil {
		return "chmod", err
	}
	err = os.Chown(bd.txInfo.Destination, int(adapter.Owner()), int(adapter.Group()))
	if err != nil {
		return "chown", err
	}
	err = os.Chtimes(bd.txInfo.Destination, adapter.ATime(), adapter.MTime())
	if err != nil {
		return "chtimes", err
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
		_, err = bd.ApplyUnixProperties(props)

		return err
	}

	return nil
}
