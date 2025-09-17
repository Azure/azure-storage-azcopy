//go:build !windows
// +build !windows

package cmd

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

// LogHardLinkIfDefaultPolicy logs a warning if the given file is a hard link and the specified
// hardlink handling policy is set to default hard links behaviour(follow).
func LogHardLinkIfDefaultPolicy(fileInfo os.FileInfo, hardlinkHandling common.HardlinkHandlingType) {
	if !IsHardlink(fileInfo) || hardlinkHandling != common.DefaultHardlinkHandlingType {
		return
	}

	stat := fileInfo.Sys().(*syscall.Stat_t) // safe to cast again since IsHardlink succeeded
	inodeStr := strconv.FormatUint(stat.Ino, 10)
	logNFSLinkWarning(fileInfo.Name(), inodeStr, false, hardlinkHandling)
}

// HandleSymlinkForNFS processes a symbolic link based on the specified handling type.
// It either logs a warning or preserves the symlink based on the symlink handling type.
func HandleSymlinkForNFS(fileName string,
	symlinkHandlingType common.SymlinkHandlingType,
	incrementEnumerationCounter enumerationCounterFunc) bool {

	if symlinkHandlingType.None() {
		// Log a warning if symlink handling is disabled
		logNFSLinkWarning(fileName, "", true, common.DefaultHardlinkHandlingType)
		if incrementEnumerationCounter != nil {
			incrementEnumerationCounter(common.EEntityType.Symlink(),
				symlinkHandlingType, common.DefaultHardlinkHandlingType)
		}
		return true
	} else if symlinkHandlingType.Preserve() {
		//TODO: no action need as of now
		return false
	} else {
		if incrementEnumerationCounter != nil {
			incrementEnumerationCounter(common.EEntityType.Symlink(),
				symlinkHandlingType, common.DefaultHardlinkHandlingType)
		}
		return false
	}
}
