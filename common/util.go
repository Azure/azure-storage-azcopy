package common

import (
	"path"
	"strings"
)

var AzcopyJobPlanFolder string
var AzcopyCurrentJobLogger ILoggerResetable

func CleanLocalPath(localPath string) string {
	localPathSeparator := DeterminePathSeparator(localPath)
	// path.Clean only likes /, and will only handle /. So, we consolidate it to /.
	// it will do absolutely nothing with \.
	normalizedPath := path.Clean(strings.ReplaceAll(localPath, localPathSeparator, AZCOPY_PATH_SEPARATOR_STRING))
	// return normalizedPath path separator.
	normalizedPath = strings.ReplaceAll(normalizedPath, AZCOPY_PATH_SEPARATOR_STRING, localPathSeparator)

	// path.Clean steals the first / from the // or \\ prefix.
	if strings.HasPrefix(localPath, `\\`) || strings.HasPrefix(localPath, `//`) {
		// return the \ we stole from the UNC/extended path.
		normalizedPath = localPathSeparator + normalizedPath
	}

	// path.Clean steals the last / from C:\, C:/, and does not add one for C:
	if RootDriveRegex.MatchString(strings.ReplaceAll(ToShortPath(normalizedPath), OS_PATH_SEPARATOR, AZCOPY_PATH_SEPARATOR_STRING)) {
		normalizedPath += OS_PATH_SEPARATOR
	}

	return normalizedPath
}

func RelativePath(filePath, root string) string {
	return strings.TrimPrefix(strings.TrimPrefix(CleanLocalPath(filePath), CleanLocalPath(root)), DeterminePathSeparator(root))
}
