// +build linux

package ste

import (
	"errors"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"golang.org/x/sys/unix"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func (f localFileSourceInfoProvider) HasUNIXProperties() bool {
	return true
}

func (f localFileSourceInfoProvider) GetUNIXProperties() (common.UnixStatAdapter, error) {
	// Can we use statx?
	var uname unix.Utsname
	err := unix.Uname(&uname)
	if err != nil {
		return nil, err
	}

	linuxVersion := regexp.MustCompile("(\\d+\\.\\d+\\.\\d+)").FindString(string(uname.Release[:]))
	splits := strings.Split(linuxVersion, ".")

	if len(splits) != 3 {
		return nil, errors.New("Failed to parse linux version " + linuxVersion)
	}

	versions := make([]int64, 3)
	for k, v := range splits {
		versions[k], _ = strconv.ParseInt(v, 10, 64)
	}

	var resp common.UnixStatAdapter

	/*
		Why shouldn't we just do a single call?

		For some odd reason during testing, Statx had strange behaviour, in that statx returned atime properly, but the stx_mask didn't contain the atime flag.
		Furthermore, Statx is linux-exclusive. It may be best to obtain all posix-standard properties through a posix-standard interface, for the sake of future accuracy.
	*/

	var statx *unix.Statx_t
	if versions[0] > 4 || (versions[0] == 4 && versions[1] > 11) { // Can we statx?
		var stat unix.Statx_t
		// dirfd is a null pointer, because we should only ever be passing relative paths here, and directories will be passed via transferInfo.Source.
		// AT_SYMLINK_NOFOLLOW is not used, because we automagically resolve symlinks. TODO: Add option to not follow symlinks, and use AT_SYMLINK_NOFOLLOW when resolving is disabled.
		err = unix.Statx(0, f.transferInfo.Source,
			unix.AT_STATX_SYNC_AS_STAT, // We want to sync attributes to ensure correctness.
			unix.STATX_BTIME,           // Let's pull only the special statx properties, and yank the rest from a standard stat_t call.
			&stat)
		if err != nil {
			return nil, err
		}

		statx = &stat
	}

	var stat unix.Stat_t
	err = unix.Stat(f.transferInfo.Source, &stat)
	if err != nil {
		return nil, err
	}

	resp = comboStatAdapter{&stat, statx}

	return resp, nil
}

type comboStatAdapter struct {
	base      *unix.Stat_t
	extension *unix.Statx_t
}

func (c comboStatAdapter) Extended() bool {
	return c.extension != nil
}

func (c comboStatAdapter) StatxMask() uint32 {
	return c.extension.Mask
}

func (c comboStatAdapter) Attribute() uint64 {
	return c.extension.Attributes
}

func (c comboStatAdapter) AttributeMask() uint64 {
	return c.extension.Attributes_mask
}

func (c comboStatAdapter) BTime() time.Time {
	return time.Unix(c.extension.Btime.Sec, int64(c.extension.Btime.Nsec))
}

func (c comboStatAdapter) NLink() uint64 {
	return c.base.Nlink
}

func (c comboStatAdapter) Owner() uint32 {
	return c.base.Uid
}

func (c comboStatAdapter) Group() uint32 {
	return c.base.Gid
}

func (c comboStatAdapter) FileMode() uint32 {
	return c.base.Mode
}

func (c comboStatAdapter) INode() uint64 {
	return c.base.Ino
}

func (c comboStatAdapter) Device() uint64 {
	return c.base.Dev
}

func (c comboStatAdapter) RDevice() uint64 {
	return c.base.Rdev
}

func (c comboStatAdapter) ATime() time.Time {
	return time.Unix(c.base.Atim.Unix())
}

func (c comboStatAdapter) MTime() time.Time {
	return time.Unix(c.base.Mtim.Unix())
}

func (c comboStatAdapter) CTime() time.Time {
	return time.Unix(c.base.Ctim.Unix())
}
