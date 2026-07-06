package cmd

import (
	"os"
	"path"

	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
)

// GetAzCopyAppPath returns the path of Azcopy in local appdata.
func GetAzCopyAppPath() string {
	userProfile := enum.EEnvironmentVariable.UserDir().Get()
	azcopyAppDataFolder := path.Join(userProfile, ".azcopy")
	if err := os.Mkdir(azcopyAppDataFolder, os.ModeDir); err != nil && !os.IsExist(err) {
		return ""
	}
	return azcopyAppDataFolder
}
