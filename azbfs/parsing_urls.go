package azbfs

import (
	"net"
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
	SAS                 SASQueryParameters

	accountName       string // "" if not using IP endpoint style
	isIPEndpointStyle bool   // Ex: "https://ip/accountname/filesystem"
}

// isIPEndpointStyle checkes if URL's host is IP, in this case the storage account endpoint will be composed as:
// http(s)://IP(:port)/storageaccount/share(||container||etc)/...
func isIPEndpointStyle(url url.URL) bool {
	return net.ParseIP(url.Host) != nil
}

// NewBfsURLParts parses a URL initializing BfsURLParts' fields. Any other
// query parameters remain in the UnparsedParams field. This method overwrites all fields in the BfsURLParts object.
func NewBfsURLParts(u url.URL) BfsURLParts {
	isIPEndpointStyle := isIPEndpointStyle(u)

	up := BfsURLParts{
		Scheme:            u.Scheme,
		Host:              u.Host,
		isIPEndpointStyle: isIPEndpointStyle,
	}

	if u.Path != "" {
		path := u.Path

		if path[0] == '/' {
			path = path[1:]
		}

		if isIPEndpointStyle {
			accountEndIndex := strings.Index(path, "/")
			if accountEndIndex == -1 { // Slash not found; path has account name & no file system, path of directory or file
				up.accountName = path
			} else {
				up.accountName = path[:accountEndIndex] // The account name is the part between the slashes

				path = path[accountEndIndex+1:]
				// Find the next slash (if it exists)
				fsEndIndex := strings.Index(path, "/")
				if fsEndIndex == -1 { // Slash not found; path has file system name & no path of directory or file
					up.FileSystemName = path
				} else { // Slash found; path has file system name & path of directory or file
					up.FileSystemName = path[:fsEndIndex]
					up.DirectoryOrFilePath = path[fsEndIndex+1:]
				}
			}
		} else {
			// Find the next slash (if it exists)
			fsEndIndex := strings.Index(path, "/")
			if fsEndIndex == -1 { // Slash not found; path has share name & no path of directory or file
				up.FileSystemName = path
			} else { // Slash found; path has share name & path of directory or file
				up.FileSystemName = path[:fsEndIndex]
				up.DirectoryOrFilePath = path[fsEndIndex+1:]
			}
		}
	}

	// Convert the query parameters to a case-sensitive map & trim whitespace
	paramsMap := u.Query()
	up.SAS = newSASQueryParameters(paramsMap, true)
	up.UnparsedParams = paramsMap.Encode()
	return up
}

// URL returns a URL object whose fields are initialized from the BfsURLParts fields.
func (up BfsURLParts) URL() url.URL {
	path := ""
	// Concatenate account name for IP endpoint style URL
	if up.isIPEndpointStyle && up.accountName != "" {
		path += "/" + up.accountName
	}
	// Concatenate filesystem & path of directory or file (if they exist)
	if up.FileSystemName != "" {
		path += "/" + up.FileSystemName
		if up.DirectoryOrFilePath != "" {
			path += "/" + up.DirectoryOrFilePath
		}
	}

	rawQuery := up.UnparsedParams

	sas := up.SAS.Encode()
	if sas != "" {
		if len(rawQuery) > 0 {
			rawQuery += "&"
		}
		rawQuery += sas
	}
	u := url.URL{
		Scheme:   up.Scheme,
		Host:     up.Host,
		Path:     path,
		RawQuery: rawQuery,
	}
	return u
}
