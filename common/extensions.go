package common

import (
	"bytes"
	"github.com/Azure/azure-storage-azcopy/azbfs"
	"net/http"
	"net/url"
	"runtime"
	"strings"

	"github.com/Azure/azure-storage-file-go/azfile"
)

/////////////////////////////////////////////////////////////////////////////////////////////////
type URLStringExtension string

func (s URLStringExtension) RedactSecretQueryParamForLogging() string {
	u, err := url.Parse(string(s))
	if err != nil {
		return string(s)
	}
	return URLExtension{*u}.RedactSecretQueryParamForLogging()
}

/////////////////////////////////////////////////////////////////////////////////////////////////
type URLExtension struct {
	url.URL
}

// URLWithPlusDecodedInPath returns a URL with '+' in path decoded as ' '(space).
// This is useful for the cases, e.g: S3 management console encode ' '(space) as '+', which is not supported by Azure resources.
func (u URLExtension) URLWithPlusDecodedInPath() url.URL {
	if u.Path != "" && strings.Contains(u.Path, "+") {
		u.Path = strings.Replace(u.Path, "+", " ", -1)
	}
	return u.URL
}

func (u URLExtension) RedactSecretQueryParamForLogging() string {
	// redact sig= in Azure
	if ok, rawQuery := RedactSecretQueryParam(u.RawQuery, SigAzure); ok {
		u.RawQuery = rawQuery
	}

	// rediact x-amx-signature in S3
	if ok, rawQuery := RedactSecretQueryParam(u.RawQuery, SigXAmzForAws); ok {
		u.RawQuery = rawQuery
	}

	return u.String()
}

const SigAzure = azbfs.SigAzure
const SigXAmzForAws = azbfs.SigXAmzForAws

func RedactSecretQueryParam(rawQuery, queryKeyNeedRedact string) (bool, string) {
	rawQuery = strings.ToLower(rawQuery) // lowercase the string so we can look for ?[queryKeyNeedRedact] and &[queryKeyNeedRedact]=
	sigFound := strings.Contains(rawQuery, "?"+queryKeyNeedRedact+"=")
	if !sigFound {
		sigFound = strings.Contains(rawQuery, "&"+queryKeyNeedRedact+"=")
		if !sigFound {
			return sigFound, rawQuery // [?|&][queryKeyNeedRedact]= not found; return same rawQuery passed in (no memory allocation)
		}
	}
	// [?|&][queryKeyNeedRedact]= found, redact its value
	values, _ := url.ParseQuery(rawQuery)
	for name := range values {
		if strings.EqualFold(name, queryKeyNeedRedact) {
			values[name] = []string{"REDACTED"}
		}
	}
	return sigFound, values.Encode()
}

/////////////////////////////////////////////////////////////////////////////////////////////////
type FileURLPartsExtension struct {
	azfile.FileURLParts
}

func (parts FileURLPartsExtension) GetShareURL() url.URL {
	parts.DirectoryOrFilePath = ""
	return parts.URL()
}

func (parts FileURLPartsExtension) GetServiceURL() url.URL {
	parts.ShareName = ""
	parts.DirectoryOrFilePath = ""
	return parts.URL()
}

/////////////////////////////////////////////////////////////////////////////////////////////////
type HTTPResponseExtension struct {
	*http.Response
}

// IsSuccessStatusCode checks if response's status code is contained in specified success status codes.
func (r HTTPResponseExtension) IsSuccessStatusCode(successStatusCodes ...int) bool {
	if r.Response == nil {
		return false
	}
	for _, i := range successStatusCodes {
		if i == r.StatusCode {
			return true
		}
	}
	return false
}

/////////////////////////////////////////////////////////////////////////////////////////////////
type ByteSlice []byte
type ByteSliceExtension struct {
	ByteSlice
}

// RemoveBOM removes any BOM from the byte slice
func (bs ByteSliceExtension) RemoveBOM() []byte {
	if bs.ByteSlice == nil {
		return nil
	}
	// UTF8
	return bytes.TrimPrefix(bs.ByteSlice, []byte("\xef\xbb\xbf"))
}

/////////////////////////////////////////////////////////////////////////////////////////////////

func DeterminePathSeparator(path string) string {
	// Just use forward-slash everywhere that isn't windows.
	if runtime.GOOS == "windows" && strings.Contains(path, `\`) {
		return `\` // Not using OS_PATH_SEPARATOR here explicitly
	} else {
		return AZCOPY_PATH_SEPARATOR_STRING
	}
}

// it's possible that enumerators didn't form rootPath and childPath correctly for them to be combined plainly
// so we must behave defensively and make sure the full path is correct
func GenerateFullPath(rootPath, childPath string) string {
	// align both paths to the root separator and trim the prefixes and suffixes
	rootSeparator := DeterminePathSeparator(rootPath)
	rootPath = strings.TrimSuffix(rootPath, rootSeparator)
	childPath = strings.ReplaceAll(childPath, DeterminePathSeparator(childPath), rootSeparator)
	childPath = strings.TrimPrefix(childPath, rootSeparator)

	if rootPath == "" {
		return childPath
	}

	// if the childPath is empty, it means the rootPath already points to the desired entity
	if childPath == "" {
		return rootPath
	}

	// otherwise, make sure a path separator is inserted between the rootPath if necessary
	return rootPath + rootSeparator + childPath
}
