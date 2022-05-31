// +build linux

package ste

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"golang.org/x/sys/unix"
	"time"
)

func (f localFileSourceInfoProvider) HasUNIXProperties() bool {
	return true
}

func (f localFileSourceInfoProvider) GetUNIXProperties() (common.UnixStatAdapter, error) {
	{ // attempt to call statx, if ENOSYS is returned, statx is unavailable
		var stat unix.Statx_t
		// dirfd is a null pointer, because we should only ever be passing relative paths here, and directories will be passed via transferInfo.Source.
		// AT_SYMLINK_NOFOLLOW is not used, because we automagically resolve symlinks. TODO: Add option to not follow symlinks, and use AT_SYMLINK_NOFOLLOW when resolving is disabled.
		err := unix.Statx(0, f.transferInfo.Source,
			unix.AT_STATX_SYNC_AS_STAT,
			unix.STATX_ALL,
			&stat)

		if err != nil && err != unix.ENOSYS {
			return nil, err
		} else if err == nil {
			return statxTAdapter(stat), nil
		}
	}

	var stat unix.Stat_t
	err := unix.Stat(f.transferInfo.Source, &stat)
	if err != nil {
		return nil, err
	}

	return statTAdapter(stat), nil
}

type statxTAdapter unix.Statx_t

func (s statxTAdapter) Extended() bool {
	return true
}

func (s statxTAdapter) StatxMask() uint32 {
	return s.Mask
}

func (s statxTAdapter) Attribute() uint64 {
	return s.Attributes
}

func (s statxTAdapter) AttributeMask() uint64 {
	return s.Attributes_mask
}

func (s statxTAdapter) BTime() time.Time {
	return time.Unix(s.Btime.Sec, int64(s.Btime.Nsec))
}

func (s statxTAdapter) NLink() uint64 {
	return uint64(s.Nlink)
}

func (s statxTAdapter) Owner() uint32 {
	return s.Uid
}

func (s statxTAdapter) Group() uint32 {
	return s.Gid
}

func (s statxTAdapter) FileMode() uint32 {
	return uint32(s.Mode)
}

func (s statxTAdapter) INode() uint64 {
	return s.Ino
}

func (s statxTAdapter) Device() uint64 {
	return unix.Mkdev(s.Dev_major, s.Dev_minor)
}

func (s statxTAdapter) RDevice() uint64 {
	return unix.Mkdev(s.Rdev_major, s.Rdev_minor)
}

func (s statxTAdapter) ATime() time.Time {
	return time.Unix(s.Atime.Sec, int64(s.Atime.Nsec))
}

func (s statxTAdapter) MTime() time.Time {
	return time.Unix(s.Mtime.Sec, int64(s.Mtime.Nsec))
}

func (s statxTAdapter) CTime() time.Time {
	return time.Unix(s.Btime.Sec, int64(s.Ctime.Nsec))
}

type statTAdapter unix.Stat_t

func (s statTAdapter) Extended() bool {
	return false
}

func (s statTAdapter) StatxMask() uint32 {
	return 0
}

func (s statTAdapter) Attribute() uint64 {
	return 0
}

func (s statTAdapter) AttributeMask() uint64 {
	return 0
}

func (s statTAdapter) BTime() time.Time {
	return time.Time{}
}

func (s statTAdapter) NLink() uint64 {
	return s.Nlink
}

func (s statTAdapter) Owner() uint32 {
	return s.Uid
}

func (s statTAdapter) Group() uint32 {
	return s.Gid
}

func (s statTAdapter) FileMode() uint32 {
	return s.Mode
}

func (s statTAdapter) INode() uint64 {
	return s.Ino
}

func (s statTAdapter) Device() uint64 {
	return s.Dev
}

func (s statTAdapter) RDevice() uint64 {
	return s.Rdev
}

func (s statTAdapter) ATime() time.Time {
	return time.Unix(s.Atim.Unix())
}

func (s statTAdapter) MTime() time.Time {
	return time.Unix(s.Mtim.Unix())
}

func (s statTAdapter) CTime() time.Time {
	return time.Unix(s.Ctim.Unix())
}
