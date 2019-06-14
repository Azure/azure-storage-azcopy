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
	short, err := filepath.Abs(short)
	PanicIfErr(err) //TODO: Handle errors better?

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
