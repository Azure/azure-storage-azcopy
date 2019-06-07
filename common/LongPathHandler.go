package common

import (
	"os"
	"path/filepath"
	"strings"
)

//https://docs.microsoft.com/en-us/windows/desktop/FileIO/naming-a-file#maximum-path-length-limitation
//Because windows doesn't (by default) support strings above 260 characters,
//we need to provide a special prefix to tell it to support paths up to 32,767 characters.

//Furthermore, we don't use the built-in long path support in the Go SDK commit 231aa9d6d7
//because it fails to support long UNC paths. As a result, we opt to wrap things such as filepath.Glob()
//to safely use them with long UNC paths.

//ToExtendedPath converts short paths to an extended path.
func ToExtendedPath(short string) string {
	if os.PathSeparator == '\\' {
		if strings.HasPrefix(short, EXTENDED_PATH_PREFIX) {
			return strings.Replace(short, `/`, `\`, -1)
		} else if strings.HasPrefix(short, `\\`) {
			return strings.Replace(EXTENDED_UNC_PATH_PREFIX+short[1:], `/`, `\`, -1)
		} else {
			return strings.Replace(EXTENDED_PATH_PREFIX+short, `/`, `\`, -1)
		}
	}

	return short
}

//ToShortPath converts an extended path to a short path.
func ToShortPath(long string) string {
	if os.PathSeparator == '\\' {
		if strings.HasPrefix(long, EXTENDED_UNC_PATH_PREFIX) {
			return `\` + long[7:] //Return what we stole from it
		} else if strings.HasPrefix(long, EXTENDED_PATH_PREFIX) {
			return long[4:]
		}
	}

	return long
}

//PreparePath prepares a path by getting its absolute path and then converting it to an extended path
func PreparePath(path string) (string, error) {
	var err error
	path, err = filepath.Abs(path)
	if err != nil {
		return path, err
	}

	if OS_PATH_SEPARATOR == `\` {
		path = ToExtendedPath(path)
		AZCOPY_PATH_SEPARATOR_STRING, AZCOPY_PATH_SEPARATOR_CHAR = `\`, '\\'
		if fi, err := os.Stat(path); err == nil {
			if fi.IsDir() && !strings.HasSuffix(path, `\`) {
				path += `\`
			}
		}
	}

	return path, nil
}
