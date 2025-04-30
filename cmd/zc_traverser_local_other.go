//go:build !windows
// +build !windows

package cmd

import (
	"fmt"
	"os"
	"syscall"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func WrapFolder(fullpath string, stat os.FileInfo) (os.FileInfo, error) {
	return stat, nil
}

func CheckHardlink(fileInfo os.FileInfo, hardlinkHandling common.PreserveHardlinksOption, filePath string) {
	stat := fileInfo.Sys().(*syscall.Stat_t)
	if stat.Nlink > 1 && hardlinkHandling == common.DefaultPreserveHardlinksOption && !fileInfo.IsDir() {
		if azcopyScanningLogger != nil {
			azcopyScanningLogger.Log(common.LogInfo, fmt.Sprintf("Found a hardlink to '%s'. It will be copied as a regular file at the destination.", filePath))
		}
	}
}
