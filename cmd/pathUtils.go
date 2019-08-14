package cmd

import (
	"strings"
)

func getPathBeforeFirstWildcard(path string) string {
	if strings.Index(path, "*") == -1 {
		return path
	}

	firstWCIndex := strings.Index(path, "*")
	result := replacePathSeparators(path[:firstWCIndex])
	lastSepIndex := strings.LastIndex(result, "/")
	result = result[:lastSepIndex+1]

	return result
}

// In local cases, many wildcards may be used, hence string.Contains
// In non-local cases, only a trailing wildcard may be used: ex. https://myAccount.blob.core.windows.net/container/*
// In both cases, we want to copy the contents of the matches to the exact path specified on the destination.
// Without this, a directory is created at the destination, and everything is placed under it.
func pathPointsToContents(path string) bool {
	return strings.Contains(path, "*")
}
