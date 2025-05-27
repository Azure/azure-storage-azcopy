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

// IsRegularFile checks if the given os.FileInfo represents a regular file.
// Returns true if the file is regular (not a directory, symlink, or special file).
func IsRegularFile(info os.FileInfo) bool {
	return info.Mode().IsRegular()
}

func IsSymbolicLink(fileInfo os.FileInfo) bool {
	return fileInfo.Mode()&os.ModeSymlink == os.ModeSymlink
}

// LogHardLinkIfDefaultPolicy logs a warning if the given file is a hard link and the specified
// hardlink handling policy is set to default hard links behaviour(follow).
func LogHardLinkIfDefaultPolicy(fileInfo os.FileInfo, hardlinkHandling common.PreserveHardlinksOption) {
	if !IsHardlink(fileInfo) || hardlinkHandling != common.DefaultPreserveHardlinksOption {
		return
	}

	stat := fileInfo.Sys().(*syscall.Stat_t) // safe to cast again since IsHardlink succeeded
	inodeStr := strconv.FormatUint(stat.Ino, 10)
	logNFSLinkWarning(fileInfo.Name(), inodeStr, false)
}

// logNFSLinkWarning logs a warning for either a symbolic link or a hard link in an NFS share.
// - For symlinks: inodeNo should be empty.
// - For hard links: inodeNo should be the file's inode number.
func logNFSLinkWarning(fileName, inodeNo string, isSymlink bool) {
	if common.AzcopyCurrentJobLogger == nil {
		return
	}

	var message string
	if isSymlink {
		message = fmt.Sprintf("File '%s' at the source is a symbolic link and will be skipped and not copied", fileName)
	} else {
		message = fmt.Sprintf("File '%s' with inode '%s' at the source is a hard link, but is copied as a full file", fileName, inodeNo)
	}

	common.AzcopyCurrentJobLogger.Log(common.LogWarning, message)
}

func logSpecialFileWarning(fileName string) {
	if common.AzcopyCurrentJobLogger == nil {
		return
	}

	message := fmt.Sprintf("File '%s' at the source is a special file and will be skipped and not copied", fileName)
	common.AzcopyCurrentJobLogger.Log(common.LogWarning, message)
}
