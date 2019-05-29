package common

import (
	"os"
	"strings"
)

func ToLongPath(short string) string {
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
