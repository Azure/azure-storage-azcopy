package common

import "strings"

func ToLongPath(short string) string {
	if strings.HasPrefix(short, `\\?\`) {
		return short
	} else if strings.HasPrefix(short, `\\`) {
		return `\\?\UNC` + short[1:]
	} else {
		return `\\?\` + short
	}
}

func ToShortPath(long string) string {
	if strings.HasPrefix(long, `\\?\UNC`) {
		return `\` + long[7:]
	} else if strings.HasPrefix(long, `\\?\`) {
		return long[4:]
	}

	return long
}
