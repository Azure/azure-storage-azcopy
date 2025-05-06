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

func CheckHardLink(fileInfo os.FileInfo, hardlinkHandling common.PreserveHardlinksOption) {
	// Cast system-specific stat info
	stat, ok := fileInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return // gracefully skip if not the expected type
	}

	// Skip if not a hard link or not a file
	if stat.Nlink <= 1 || fileInfo.IsDir() || hardlinkHandling != common.DefaultPreserveHardlinksOption {
		return
	}

	inodeStr := strconv.FormatUint(stat.Ino, 10)
	if existingPath, found := Get(inodeStr); found {
		logHardlinkWarning(fileInfo.Name(), existingPath)
	} else {
		Set(inodeStr, fileInfo.Name())
		logHardlinkWarning(fileInfo.Name(), "")
	}
}

func logHardlinkWarning(currentFile, linkedTo string) {
	if common.AzcopyCurrentJobLogger == nil {
		return
	}

	if linkedTo == "" {
		common.AzcopyCurrentJobLogger.Log(
			common.LogWarning,
			fmt.Sprintf("File '%s' at the source is a hard link, but is copied as a full file", currentFile),
		)
	} else {
		common.AzcopyCurrentJobLogger.Log(
			common.LogWarning,
			fmt.Sprintf("File '%s' at the source is a hard link to '%s', but is copied as a full file", currentFile, linkedTo),
		)
	}
}
