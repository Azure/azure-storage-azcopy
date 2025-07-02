// Copyright © 2017 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package common

import (
	"encoding/binary"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/pkg/xattr"
	"golang.org/x/sys/unix"
)

// Extended Attribute (xattr) keys for fetching various information from Linux cifs client.
const (
	CIFS_XATTR_CREATETIME     = "user.cifs.creationtime" // File creation time.
	CIFS_XATTR_ATTRIB         = "user.cifs.dosattrib"    // FileAttributes.
	CIFS_XATTR_CIFS_ACL       = "system.cifs_acl"        // DACL only.
	CIFS_XATTR_CIFS_NTSD      = "system.cifs_ntsd"       // Owner, Group, DACL.
	CIFS_XATTR_CIFS_NTSD_FULL = "system.cifs_ntsd_full"  // Owner, Group, DACL, SACL.
)

// 100-nanosecond intervals from Windows Epoch (January 1, 1601) to Unix Epoch (January 1, 1970).
const (
	TICKS_FROM_WINDOWS_EPOCH_TO_UNIX_EPOCH = 116444736000000000
)

// windows.Filetime.
type Filetime struct {
	LowDateTime  uint32
	HighDateTime uint32
}

// windows.ByHandleFileInformation
type ByHandleFileInformation struct {
	FileAttributes     uint32
	CreationTime       Filetime
	LastAccessTime     Filetime
	LastWriteTime      Filetime
	ChangeTime         Filetime
	VolumeSerialNumber uint32
	FileSizeHigh       uint32
	FileSizeLow        uint32
	NumberOfLinks      uint32
	FileIndexHigh      uint32
	FileIndexLow       uint32
}

// Nanoseconds converts Filetime (as ticks since Windows Epoch) to nanoseconds since Unix Epoch (January 1, 1970).
func (ft *Filetime) Nanoseconds() int64 {
	// 100-nanosecond intervals (ticks) since Windows Epoch (January 1, 1601).
	nsec := int64(ft.HighDateTime)<<32 + int64(ft.LowDateTime)

	// 100-nanosecond intervals since Unix Epoch (January 1, 1970).
	nsec -= TICKS_FROM_WINDOWS_EPOCH_TO_UNIX_EPOCH

	// nanoseconds since Unix Epoch.
	return nsec * 100
}

// Convert nanoseconds since Unix Epoch (January 1, 1970) to Filetime since Windows Epoch (January 1, 1601).
func NsecToFiletime(nsec int64) Filetime {
	// 100-nanosecond intervals since Unix Epoch (January 1, 1970).
	nsec /= 100

	// 100-nanosecond intervals since Windows Epoch (January 1, 1601).
	nsec += TICKS_FROM_WINDOWS_EPOCH_TO_UNIX_EPOCH

	return Filetime{LowDateTime: uint32(nsec & 0xFFFFFFFF), HighDateTime: uint32(nsec >> 32)}
}

// WindowsTicksToUnixNano converts ticks (100-ns intervals) since Windows Epoch to nanoseconds since Unix Epoch.
func WindowsTicksToUnixNano(ticks int64) int64 {
	// 100-nanosecond intervals since Unix Epoch (January 1, 1970).
	ticks -= TICKS_FROM_WINDOWS_EPOCH_TO_UNIX_EPOCH

	// nanoseconds since Unix Epoch (January 1, 1970).
	return ticks * 100
}

// UnixNanoToWindowsTicks converts nanoseconds since Unix Epoch to ticks since Windows Epoch.
func UnixNanoToWindowsTicks(nsec int64) int64 {
	// 100-nanosecond intervals since Unix Epoch (January 1, 1970).
	nsec /= 100

	// 100-nanosecond intervals since Windows Epoch (January 1, 1601).
	nsec += TICKS_FROM_WINDOWS_EPOCH_TO_UNIX_EPOCH

	return nsec
}

// StatxTimestampToFiletime converts the unix StatxTimestamp (sec, nsec) to the Windows' Filetime.
// Note that StatxTimestamp is from Unix Epoch while Filetime holds time from Windows Epoch.
func StatxTimestampToFiletime(ts unix.StatxTimestamp) Filetime {
	return NsecToFiletime(ts.Sec*int64(time.Second) + int64(ts.Nsec))
}

func GetFileInformation(path string) (ByHandleFileInformation, error) {
	var stx unix.Statx_t

	// We want all attributes including Btime (aka creation time).
	// For consistency with Windows implementation we pass flags==0 which causes it to follow symlinks.
	err := unix.Statx(unix.AT_FDCWD, path, 0 /* flags */, unix.STATX_ALL, &stx)
	if err == unix.ENOSYS || err == unix.EPERM {
		panic(fmt.Errorf("statx syscall is not available: %v", err))
	} else if err != nil {
		return ByHandleFileInformation{}, fmt.Errorf("statx(%s) failed: %v", path, err)
	}

	// For getting FileAttributes we need to query the CIFS_XATTR_ATTRIB extended attribute.
	// Note: This doesn't necessarily cause a new QUERY_PATH_INFO call to the SMB server, instead
	//       the value cached in the inode (likely as a result of the above Statx call) will be
	//       returned.
	xattrbuf, err := xattr.Get(path, CIFS_XATTR_ATTRIB)
	if err != nil {
		return ByHandleFileInformation{},
			fmt.Errorf("xattr.Get(%s, %s) failed: %v", path, CIFS_XATTR_ATTRIB, err)
	}

	var info ByHandleFileInformation

	info.FileAttributes = binary.LittleEndian.Uint32(xattrbuf)

	info.CreationTime = StatxTimestampToFiletime(stx.Btime)
	info.LastAccessTime = StatxTimestampToFiletime(stx.Atime)
	info.LastWriteTime = StatxTimestampToFiletime(stx.Mtime)
	info.ChangeTime = StatxTimestampToFiletime(stx.Ctime)

	// TODO: Do we need this?
	info.VolumeSerialNumber = 0

	info.FileSizeHigh = uint32(stx.Size >> 32)
	info.FileSizeLow = uint32(stx.Size & 0xFFFFFFFF)

	info.NumberOfLinks = stx.Nlink

	info.FileIndexHigh = uint32(stx.Ino >> 32)
	info.FileIndexLow = uint32(stx.Ino & 0xFFFFFFFF)

	return info, nil
}

func CreateFileOfSizeWithWriteThroughOption(destinationPath string, fileSize int64, writeThrough bool, t FolderCreationTracker, forceIfReadOnly bool) (*os.File, error) {
	// forceIfReadOnly is not used on this OS

	err := CreateParentDirectoryIfNotExist(destinationPath, t)
	if err != nil {
		return nil, err
	}

	flags := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	if writeThrough {
		// TODO: conduct further testing of this code path, on Linux
		flags = flags | os.O_SYNC // technically, O_DSYNC may be very slightly faster, but its not exposed in the os package
	}
	f, err := os.OpenFile(destinationPath, flags, DEFAULT_FILE_PERM)
	if err != nil {
		return nil, err
	}

	if fileSize == 0 {
		return f, err
	}

	err = syscall.Fallocate(int(f.Fd()), 0, 0, fileSize)
	if err != nil {
		// To solve the case that Fallocate cannot work well with cifs/smb3.
		if err == syscall.ENOTSUP {
			if truncateError := f.Truncate(fileSize); truncateError != nil {
				return nil, truncateError
			}
		} else {
			return nil, err
		}
	}
	return f, nil
}

func SetBackupMode(enable bool, fromTo FromTo) error {
	// n/a on this platform
	return nil
}

func GetExtendedProperties(path string, entityType EntityType) (ExtendedProperties, error) {
	{ // attempt to call statx, if ENOSYS is returned, statx is unavailable
		var stat unix.Statx_t

		statxFlags := unix.AT_STATX_SYNC_AS_STAT
		if entityType == EEntityType.Symlink() {
			statxFlags |= unix.AT_SYMLINK_NOFOLLOW
		}
		// dirfd is a null pointer, because we should only ever be passing relative paths here, and directories will be passed via transferInfo.Source.
		// AT_SYMLINK_NOFOLLOW is not used, because we automagically resolve symlinks. TODO: Add option to not follow symlinks, and use AT_SYMLINK_NOFOLLOW when resolving is disabled.
		err := unix.Statx(0, path, statxFlags, unix.STATX_ALL, &stat)

		if err != nil && err != unix.ENOSYS {
			return nil, err
		} else if err == nil {
			return &UnixStatxProperties{info: stat}, nil
		}
	}

	var stat unix.Stat_t
	err := unix.Stat(path, &stat)
	if err != nil {
		return nil, err
	}

	return &UnixStatProperties{info: stat}, nil
}

// UnixStatxProperties wraps unix.Statx_t to implement the ExtendedProperties interface
type UnixStatxProperties struct {
	info unix.Statx_t
}

// GetChangeTime returns the change time
func (p *UnixStatxProperties) GetChangeTime() time.Time {
	return time.Unix(p.info.Ctime.Sec, int64(p.info.Ctime.Nsec))
}

// GetLastWriteTime returns the last write time
func (p *UnixStatxProperties) GetLastWriteTime() time.Time {
	return time.Unix(p.info.Mtime.Sec, int64(p.info.Mtime.Nsec))
}

// GetLastAccessTime returns the last access time
func (p *UnixStatxProperties) GetLastAccessTime() time.Time {
	return time.Unix(p.info.Atime.Sec, int64(p.info.Atime.Nsec))
}

// UnixStatProperties wraps unix.Stat_t to implement the ExtendedProperties interface
type UnixStatProperties struct {
	info unix.Stat_t
}

// GetChangeTime returns the change time
func (p *UnixStatProperties) GetChangeTime() time.Time {
	return time.Unix(p.info.Ctim.Sec, p.info.Ctim.Nsec)
}

// GetLastWriteTime returns the last write time
func (p *UnixStatProperties) GetLastWriteTime() time.Time {
	return time.Unix(p.info.Mtim.Sec, p.info.Mtim.Nsec)
}

// GetLastAccessTime returns the last access time
func (p *UnixStatProperties) GetLastAccessTime() time.Time {
	return time.Unix(p.info.Atim.Sec, p.info.Atim.Nsec)
}
