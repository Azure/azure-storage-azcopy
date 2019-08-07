package cmd

import (
	"context"
	"net/url"
	"os"
	"strings"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/azure-storage-file-go/azfile"

	"github.com/Azure/azure-storage-azcopy/azbfs"
	"github.com/Azure/azure-storage-azcopy/common"
)

func isPathDirectory(path string, location common.Location, ctx *context.Context, p *pipeline.Pipeline) bool {
	if location == common.ELocation.Local() {
		if strings.HasSuffix(path, "/") {
			return true
		}

		props, err := os.Stat(path)

		if err != nil {
			return false
		}

		return props.IsDir()
	} else {
		objURL, err := url.Parse(path)

		if err != nil {
			return false
		}

		result := false

		if azblob.NewBlobURLParts(*objURL).IPEndpointStyleInfo.AccountName == "" {
			// Typical endpoint style
			// If there's no slashes after the first, it's a container.
			// If there's a slash on the end, it's a virtual directory/container.
			// Otherwise, it's just a blob.
			result = strings.HasSuffix(objURL.Path, "/") || strings.Count(objURL.Path[1:], "/") == 0
		} else {
			// IP endpoint style: https://IP:port/accountname/container
			// If there's 2 or less slashes after the first, it's a container.
			// OR If there's a slash on the end, it's a virtual directory/container.
			// Otherwise, it's just a blob.
			result = strings.HasSuffix(objURL.Path, "/") || strings.Count(objURL.Path[1:], "/") <= 1
		}

		if result || p == nil {
			return result
		}

		switch location {
		case common.ELocation.File():
			// Need make request to ensure if it's directory
			directoryURL := azfile.NewDirectoryURL(*objURL, *p)
			_, err := directoryURL.GetProperties(*ctx)
			if err != nil {
				result = false
				break
			}

			result = true
		case common.ELocation.BlobFS():
			// Need to get the resource properties and verify if it is a file or directory
			dirURL := azbfs.NewDirectoryURL(*objURL, *p)
			result = dirURL.IsDirectory(*ctx)
		}

		return result
	}
}

// In local cases, many wildcards may be used, hence string.Contains
// In non-local cases, only a trailing wildcard may be used: ex. https://myAccount.blob.core.windows.net/container/*
// In both cases, we want to copy the contents of the matches to the exact path specified on the destination.
// Without this, a directory is created at the destination, and everything is placed under it.
func pathPointsToContents(path string) bool {
	return strings.Contains(path, "*")
}

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
