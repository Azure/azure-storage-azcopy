package cmd

import (
	"os"
	"path"
)

// GetAzCopyAppPath returns the path of Azcopy in local appdata.
func GetAzCopyAppPath() string {
	userProfile := os.Getenv("USERPROFILE")
	azcopyAppDataFolder := path.Join(userProfile, ".azcopy")
	if err := os.Mkdir(azcopyAppDataFolder, os.ModeDir); err != nil && !os.IsExist(err) {
		return ""
	}
	return azcopyAppDataFolder
}
