package cmd

import (
	"fmt"
	"os"
)

// GetAzCopyAppPath returns the path of Azcopy in local appdata.
func GetAzCopyAppPath() string {
	localAppData := os.Getenv("LOCALAPPDATA")
	azcopyAppDataFolder := fmt.Sprintf("%s%s%s", localAppData, string(os.PathSeparator), "Azcopy")
	if err := os.Mkdir(azcopyAppDataFolder, os.ModeDir); err != nil && !os.IsExist(err) {
		return ""
	}
	return azcopyAppDataFolder
}
