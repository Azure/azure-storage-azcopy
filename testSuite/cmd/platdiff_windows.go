package cmd

import (
	"os"
	"path"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// GetAzCopyAppPath returns the path of Azcopy in local appdata.
func GetAzCopyAppPath() string {
	userProfile := common.GetEnvironmentVariable(common.EEnvironmentVariable.UserDir())
	azcopyAppDataFolder := path.Join(userProfile, ".azcopy")
	if err := os.Mkdir(azcopyAppDataFolder, os.ModeDir); err != nil && !os.IsExist(err) {
		return ""
	}
	return azcopyAppDataFolder
}
