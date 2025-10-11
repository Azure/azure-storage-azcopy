//go:build linux || darwin
// +build linux darwin

package cmd

import (
	"os"
	"path"
)

// GetAzCopyAppPath returns the path of Azcopy folder in local appdata.
// Azcopy folder in local appdata contains all the files created by azcopy locally.
func GetAzCopyAppPath() string {
	localAppData := os.Getenv("HOME")
	azcopyAppDataFolder := path.Join(localAppData, "/.azcopy")
	if err := os.Mkdir(azcopyAppDataFolder, os.ModeDir|os.ModePerm); err != nil && !os.IsExist(err) {
		return ""
	}
	return azcopyAppDataFolder
}
