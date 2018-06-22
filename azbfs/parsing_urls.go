package azbfs

import (
	"net/url"
	"strings"
)

// A BfsURLParts object represents the components that make up an Azure Storage FileSystem/Directory/File URL. You parse an
// existing URL into its parts by calling NewBfsURLParts(). You construct a URL from parts by calling URL().
type BfsURLParts struct {
	Scheme              string // Ex: "https://"
	Host                string // Ex: "account.dfs.core.windows.net"
	FileSystemName      string // File System name, Ex: "myfilesystem"
	DirectoryOrFilePath string // Path of directory or file, Ex: "mydirectory/myfile"
	UnparsedParams      string
}

// NewBfsURLParts parses a URL initializing BfsURLParts' fields. Any other
// query parameters remain in the UnparsedParams field. This method overwrites all fields in the BfsURLParts object.
func NewBfsURLParts(u url.URL) BfsURLParts {
	up := BfsURLParts{
		Scheme: u.Scheme,
		Host:   u.Host,
	}

	if u.Path != "" {
		path := u.Path

		if path[0] == '/' {
			path = path[1:]
		}

		// Find the next slash (if it exists)
		filesystemEndIndex := strings.Index(path, "/")
		if filesystemEndIndex == -1 { // Slash not found; path has filesystem name & no path of directory or file
			up.FileSystemName = path
		} else { // Slash found; path has filesystem name & path of directory or file
			up.FileSystemName = path[:filesystemEndIndex]
			up.DirectoryOrFilePath = path[filesystemEndIndex+1:]
		}
	}

	// Convert the query parameters to a case-sensitive map & trim whitespace
	paramsMap := u.Query()
	up.UnparsedParams = paramsMap.Encode()
	return up
}

// URL returns a URL object whose fields are initialized from the BfsURLParts fields.
func (up BfsURLParts) URL() url.URL {
	path := ""
	// Concatenate filesystem & path of directory or file (if they exist)
	if up.FileSystemName != "" {
		path += "/" + up.FileSystemName
		if up.DirectoryOrFilePath != "" {
			path += "/" + up.DirectoryOrFilePath
		}
	}

	rawQuery := up.UnparsedParams
	u := url.URL{
		Scheme:   up.Scheme,
		Host:     up.Host,
		Path:     path,
		RawQuery: rawQuery,
	}
	return u
}
