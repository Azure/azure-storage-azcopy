package common

import (
	"os"
	"path/filepath"
	"strings"
)

//ToExtendedPath converts short paths to an extended path.
func ToExtendedPath(short string) string {
	if os.PathSeparator == '\\' {
		if strings.HasPrefix(short, `\\?\`) {
			return strings.Replace(short, `/`, `\`, -1)
		} else if strings.HasPrefix(short, `\\`) {
			return strings.Replace(`\\?\UNC`+short[1:], `/`, `\`, -1)
		} else {
			return strings.Replace(`\\?\`+short, `/`, `\`, -1)
		}
	}

	return short
}

//ToShortPath converts an extended path to a short path.
func ToShortPath(long string) string {
	if os.PathSeparator == '\\' {
		if strings.HasPrefix(long, `\\?\UNC`) {
			return strings.Replace(`\`+long[7:], `\`, `/`, -1)
		} else if strings.HasPrefix(long, `\\?\`) {
			return strings.Replace(long[4:], `\`, `/`, -1)
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
