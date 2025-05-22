//go:build !windows
// +build !windows

package cmd

import (
	"fmt"
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

// CheckHardLink logs a warning if the given file is a hard link and the specified
// hardlink handling policy is set to default hard links behaviour(follow).
func CheckHardLink(fileInfo os.FileInfo, hardlinkHandling common.PreserveHardlinksOption) {
	if !IsHardlink(fileInfo) || hardlinkHandling != common.DefaultPreserveHardlinksOption {
		return
	}

	stat := fileInfo.Sys().(*syscall.Stat_t) // safe to cast again since IsHardlink succeeded
	inodeStr := strconv.FormatUint(stat.Ino, 10)
	logHardlinkWarning(fileInfo.Name(), inodeStr)
}

func logHardlinkWarning(currentFile, inodeNo string) {
	if common.AzcopyCurrentJobLogger == nil {
		return
	}
	common.AzcopyCurrentJobLogger.Log(
		common.LogWarning,
		fmt.Sprintf("File '%s' with inode '%s' at the source is a hard link, but is copied as a full file", currentFile, inodeNo),
	)
}
