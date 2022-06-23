package cmd

import (
	"os"
	"path"

	"github.com/shubham808/azure-storage-azcopy/v10/common"
)

// GetAzCopyAppPath returns the path of Azcopy in local appdata.
func GetAzCopyAppPath() string {
	lcm := common.GetLifecycleMgr()
	userProfile := lcm.GetEnvironmentVariable(common.EEnvironmentVariable.UserDir())
	azcopyAppDataFolder := path.Join(userProfile, ".azcopy")
	if err := os.Mkdir(azcopyAppDataFolder, os.ModeDir); err != nil && !os.IsExist(err) {
		return ""
	}
	return azcopyAppDataFolder
}
