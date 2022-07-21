//go:build linux || darwin
// +build linux darwin

package common

import (
	"bufio"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// GetMemAvailable returns the MemAvailable on linux system.
// MemAvailable field added after 3.14+ kernel only. So for kernel version before that we need to give rough estimate.
//
// TODO: Estimating MemAvailable on kernel before 3.14.
func GetMemAvailable() (int64, error) {

	// command to get the Available Memory
	cmdStr := `cat /proc/meminfo`
	cmd := exec.Command("sh", "-c", cmdStr)

	stdOut, err := cmd.StdoutPipe()
	if err != nil {
		err := fmt.Errorf("GetMemAvailable failed with error: %v", err)
		return 0, err
	}

	// Flow will be like this, set the scanner to stdOut and start the cmd.
	scanner := bufio.NewScanner(stdOut)
	err = cmd.Start()
	if err != nil {
		err := fmt.Errorf("GetMemAvailable failed with error: %v", err)
		return 0, err
	}

	// Set the split function for the scanning operation.
	scanner.Split(bufio.ScanLines)

	// Scan the stdOutput.
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "MemAvailable") {
			var multiplier int64
			var result int
			tokens := strings.Fields(scanner.Text())
			if len(tokens) != 3 {
				err := fmt.Errorf("GetMemAvailable invalid ouput[%s] of /proc/meminfo", scanner.Text())
				return 0, err
			}

			value := tokens[1]
			multiplerStr := tokens[2]

			if multiplerStr == "kB" {
				multiplier = 1024
			} else {
				// "/proc/meminfo" output always in kB only. If we are getting different string, something wrong.
				err := fmt.Errorf("MemAvailable value is not in kB, output[%s]", scanner.Text())
				return 0, err
			}

			if result, err = strconv.Atoi(value); err != nil {
				return 0, err
			}
			return int64(result) * int64(multiplier), nil
		}
	}

	if err = cmd.Wait(); err != nil {
		err := fmt.Errorf("GetMemAvailable failed with error: %v", err)
		return 0, err
	}

	// If we reached here, means MemAvailable entry not found in cat /proc/meminfo.
	var kernelVersion unix.Utsname
	_ = unix.Uname(&kernelVersion)

	err = fmt.Errorf(fmt.Sprintf("MemAvailable entry not found, kernel version: %+v", kernelVersion))
	return 0, err
}

// parseStat parse the output of stat and return common structure for stat and statx
func parseStat(in unix.Stat_t) UnixStatContainer {
	var out UnixStatContainer

	// Fileds not present in stat call
	out.statx = false
	out.mask = 0
	out.attributes = 0
	out.attributesMask = 0
	out.birthTime = time.Time{}

	// Fields present in stat call
	out.numLinks = in.Nlink
	out.groupGID = in.Gid
	out.ownerUID = in.Uid
	out.accessTime = time.Unix(in.Atim.Sec, in.Atim.Nsec)
	out.changeTime = time.Unix(in.Ctim.Sec, in.Ctim.Nsec)
	out.devID = in.Dev
	out.iNode = in.Ino
	out.modTime = time.Unix(in.Mtim.Sec, in.Mtim.Nsec)
	out.mode = in.Mode
	out.size = uint64(in.Size)
	out.repDevID = in.Rdev
	return out
}

// parseStatx parse the output of statx and return common structure for stat and statx
func parseStatx(in unix.Statx_t) UnixStatContainer {
	var out UnixStatContainer

	out.statx = true
	out.mask = in.Mask
	out.attributes = in.Attributes
	out.attributesMask = in.Attributes_mask
	out.birthTime = time.Unix(in.Btime.Sec, int64(in.Btime.Nsec))

	// Fields present in stat call
	out.numLinks = uint64(in.Nlink)
	out.groupGID = in.Gid
	out.ownerUID = in.Uid
	out.accessTime = time.Unix(in.Atime.Sec, int64(in.Atime.Nsec))
	out.changeTime = time.Unix(in.Ctime.Sec, int64(in.Ctime.Nsec))
	out.devID = unix.Mkdev(in.Dev_major, in.Dev_minor)
	out.iNode = in.Ino
	out.modTime = time.Unix(in.Mtime.Sec, int64(in.Mtime.Nsec))
	out.mode = uint32(in.Mode)
	out.size = in.Size
	out.repDevID = unix.Mkdev(in.Rdev_major, in.Rdev_minor)

	return out
}

// GetExtendedProperties return stat/statx properties of file/folder.
func GetExtendedProperties(fileName string) (UnixStatAdapter, error) {
	{ // attempt to call statx, if ENOSYS is returned, statx is unavailable
		var stat unix.Statx_t
		// dirfd is a null pointer, because we should only ever be passing relative paths here, and directories will be passed via transferInfo.Source.
		// AT_SYMLINK_NOFOLLOW is not used, because we automagically resolve symlinks. TODO: Add option to not follow symlinks, and use AT_SYMLINK_NOFOLLOW when resolving is disabled.
		err := unix.Statx(0, fileName,
			unix.AT_SYMLINK_NOFOLLOW|unix.AT_STATX_SYNC_AS_STAT,
			unix.STATX_ALL,
			&stat)

		if err != nil && err != unix.ENOSYS {
			return UnixStatContainer{}, err
		} else if err == nil {
			return parseStatx(stat), nil
		}
	}

	var stat unix.Stat_t
	err := unix.Stat(fileName, &stat)
	if err != nil {
		return UnixStatContainer{}, err
	}

	return parseStat(stat), nil
}
