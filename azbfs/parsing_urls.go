package azbfs

import (
	"net"
	"net/url"
	"strings"
)

const (
	shareSnapshot = "sharesnapshot"
)

// A FileURLParts object represents the components that make up an Azure Storage Share/Directory/File URL. You parse an
// existing URL into its parts by calling NewFileURLParts(). You construct a URL from parts by calling URL().
// NOTE: Changing any SAS-related field requires computing a new SAS signature.
type FileURLParts struct {
	Scheme              string // Ex: "https://"
	Host                string // Ex: "account.share.core.windows.net"
	FileSystemName      string // Share name, Ex: "myshare"
	DirectoryOrFilePath string // Path of directory or file, Ex: "mydirectory/myfile"
	UnparsedParams      string

	accountName       string // "" if not using IP endpoint style
	isIPEndpointStyle bool   // Ex: "https://ip/accountname/filesystem"
}

// isIPEndpointStyle checkes if URL's host is IP, in this case the storage account endpoint will be composed as:
// http(s)://IP(:port)/storageaccount/share(||container||etc)/...
func isIPEndpointStyle(url url.URL) bool {
	if net.ParseIP(url.Host) != nil {
		return true
	}

	return false
}

// NewFileURLParts parses a URL initializing FileURLParts' fields including any SAS-related & sharesnapshot query parameters. Any other
// query parameters remain in the UnparsedParams field. This method overwrites all fields in the FileURLParts object.
func NewFileURLParts(u url.URL) FileURLParts {
	isIPEndpointStyle := isIPEndpointStyle(u)

	up := FileURLParts{
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
	up.UnparsedParams = paramsMap.Encode()
	return up
}

// URL returns a URL object whose fields are initialized from the FileURLParts fields. The URL's RawQuery
// field contains the SAS, snapshot, and unparsed query parameters.
func (up FileURLParts) URL() url.URL {
	path := ""
	// Concatenate account name for IP endpoint style URL
	if up.isIPEndpointStyle && up.accountName != "" {
		path += "/" + up.accountName
	}

	// Concatenate share & path of directory or file (if they exist)
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
