//go:build !windows
// +build !windows

package traverser

import (
	"os"
	"strconv"
	"syscall"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func WrapFolder(fullpath string, stat os.FileInfo) (os.FileInfo, error) {
	return stat, nil
}

// IsHardlink returns true if the given os.FileInfo represents a hard link.
// It checks if the file has more than one link and is not a directory.
// This function only works on Unix-like systems where FileInfo.Sys() returns *syscall.Stat_t.
func IsHardlink(fileInfo os.FileInfo) bool {
	stat, ok := fileInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return false // gracefully skip if not the expected type
	}
	return stat.Nlink > 1 && !fileInfo.IsDir()
}

// IsRegularFile checks if the given os.FileInfo represents a regular file.
// Returns true if the file is regular (not a directory, symlink, or special file).
func IsRegularFile(info os.FileInfo) bool {
	return info.Mode().IsRegular()
}

func IsSymbolicLink(fileInfo os.FileInfo) bool {
	return fileInfo.Mode()&os.ModeSymlink == os.ModeSymlink
}

func getInodeString(fileInfo os.FileInfo) string {
	stat, ok := fileInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return ""
	}
	return strconv.FormatUint(uint64(stat.Dev), 10) + ":" + strconv.FormatUint(stat.Ino, 10)
}

func LogHardLinkIfDefaultPolicy(fileInfo os.FileInfo, handling common.HardlinkHandlingType) {
	if IsHardlink(fileInfo) && handling == common.DefaultHardlinkHandlingType {
		logNFSLinkWarning(fileInfo.Name(), getInodeString(fileInfo), false, handling)
	}
}
