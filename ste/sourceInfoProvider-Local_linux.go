// +build linux

package ste

import (
	"errors"
	"golang.org/x/sys/unix"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func (f localFileSourceInfoProvider) GetUNIXProperties() (*UnixStatAdapter, error) {
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

	resp := UnixStatAdapter{}

	if versions[0] > 4 || (versions[0] == 4 && versions[1] > 11) { // Can we statx?
		var stat unix.Statx_t
		// dirfd is a null pointer, because we should only ever be passing relative paths here, and directories will be passed via transferInfo.Source.
		// AT_SYMLINK_NOFOLLOW is not used, because we automagically resolve symlinks. TODO: Add option to not follow symlinks, and use AT_SYMLINK_NOFOLLOW when resolving is disabled.
		err = unix.Statx(0, f.transferInfo.Source,
			unix.AT_STATX_FORCE_SYNC, // We want to sync attributes to ensure correctness.
			unix.STATX_ALL,           // We want EVERY available statx field, since this is full POSIX preservation.
			&stat)
		if err != nil {
			return nil, err
		}

		getUnixTime := func(ts unix.StatxTimestamp) time.Time {
			return time.Unix(ts.Sec, int64(ts.Nsec))
		}

		resp = UnixStatAdapter{
			statx:          true,
			Mask:           int64(stat.Mask),    // Some data types were incongruent between Statx_t and Stat_t.
			BlockSize:      int64(stat.Blksize), // As a result, compromises were made to best suit both structs.
			Attributes:     stat.Attributes,
			NumLinks:       uint64(stat.Nlink),
			OwnerUID:       stat.Uid,
			GroupGID:       stat.Gid,
			Mode:           uint32(stat.Mode),
			INode:          stat.Ino,
			Size:           stat.Size,
			Blocks:         stat.Blocks,
			AttributesMask: stat.Attributes_mask,
			AccessTime:     getUnixTime(stat.Atime),
			BirthTime:      getUnixTime(stat.Btime),
			ChangeTime:     getUnixTime(stat.Ctime),
			ModTime:        getUnixTime(stat.Mtime),
			RepDevID:       unix.Mkdev(stat.Rdev_major, stat.Rdev_minor),
			DevID:          unix.Mkdev(stat.Dev_major, stat.Dev_minor),
		}

	} else { // We must stat, because statx is for sure unavailable.
		var stat unix.Stat_t
		err = unix.Stat(f.transferInfo.Source, &stat)
		if err != nil {
			return nil, err
		}

		getUnixTime := func(ts unix.Timespec) time.Time {
			return time.Unix(ts.Sec, ts.Nsec)
		}

		resp = UnixStatAdapter{
			statx:      false, // A fair number of properties should not be persisted if stat was used, as persisting empty values may have unknown consequences.
			BlockSize:  stat.Blksize,
			NumLinks:   stat.Nlink,
			OwnerUID:   stat.Uid,
			GroupGID:   stat.Gid,
			Mode:       stat.Mode,
			INode:      stat.Ino,
			Size:       uint64(stat.Size),
			Blocks:     uint64(stat.Blocks),
			DevID:      stat.Dev,
			RepDevID:   stat.Rdev,
			AccessTime: getUnixTime(stat.Atim),
			ModTime:    getUnixTime(stat.Mtim),
			ChangeTime: getUnixTime(stat.Ctim),
		}
	}

	return &resp, nil
}
