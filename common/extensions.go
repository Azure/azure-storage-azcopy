package common

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"runtime"
	"strings"
)

// ///////////////////////////////////////////////////////////////////////////////////////////////
type URLStringExtension string

func (s URLStringExtension) RedactSecretQueryParamForLogging() string {
	u, err := url.Parse(string(s))

	// no need to redact if it's a local path
	if err != nil || u.Host == "" {
		return string(s)
	}
	return URLExtension{*u}.RedactSecretQueryParamForLogging()
}

// ///////////////////////////////////////////////////////////////////////////////////////////////
type URLExtension struct {
	url.URL
}

// URLWithPlusDecodedInPath returns a URL with '+' in path decoded as ' '(space).
// This is useful for the cases, e.g: S3 management console encode ' '(space) as '+', which is not supported by Azure resources.
func (u URLExtension) URLWithPlusDecodedInPath() url.URL {
	// url.RawPath is not always present. Which is likely, if we're _just_ using +.
	if u.RawPath != "" {
		if u.RawPath != u.EscapedPath() {
			panic("sanity check: lost user input meaning on URL")
		}

		var err error
		u.RawPath = strings.ReplaceAll(u.RawPath, "+", "%20")
		u.Path, err = url.PathUnescape(u.RawPath)

		PanicIfErr(err)
	} else if u.Path != "" {
		// If we're working with no encoded characters, just replace the pluses in the path and move on.
		u.Path = strings.ReplaceAll(u.Path, "+", " ")
	}

	return u.URL
}

func (u URLExtension) RedactSecretQueryParamForLogging() string {
	// redact sig= in Azure
	if ok, rawQuery := RedactSecretQueryParam(u.RawQuery, SigAzure); ok {
		u.RawQuery = rawQuery
	}

	// redact x-amx-signature in S3
	if ok, rawQuery := RedactSecretQueryParam(u.RawQuery, SigXAmzForAws); ok {
		u.RawQuery = rawQuery
	}

	// redact x-amx-credential in S3
	if ok, rawquery := RedactSecretQueryParam(u.RawQuery, CredXAmzForAws); ok {
		u.RawQuery = rawquery
	}

	return u.String()
}

const SigAzure = "sig"
const SigXAmzForAws = "x-amz-signature"
const CredXAmzForAws = "x-amz-credential"

func RedactSecretQueryParam(rawQuery, queryKeyNeedRedact string) (bool, string) {
	values, _ := url.ParseQuery(rawQuery)
	sigFound := false
	for param := range values {
		if strings.EqualFold(strings.ToLower(param), queryKeyNeedRedact) {
			sigFound = true
			values[param] = []string{"REDACTED"}
		}
	}

	return sigFound, values.Encode()
}

// ///////////////////////////////////////////////////////////////////////////////////////////////
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

// ///////////////////////////////////////////////////////////////////////////////////////////////
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
		if strings.Contains(path, `/`) {
			panic("inconsistent path separators. Some are forward, some are back. This is not supported.")
		}

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
	} else if childPath == "\x00" { // The enumerator has asked us to target with a / at the end of our root path. This is a massive hack. When the footgun happens later, ping Adele!
		return rootPath + rootSeparator
	}

	// otherwise, make sure a path separator is inserted between the rootPath if necessary
	return rootPath + rootSeparator + childPath
}

func GenerateFullPathWithQuery(rootPath, childPath, extraQuery string) string {
	p := GenerateFullPath(rootPath, childPath)

	extraQuery = strings.TrimLeft(extraQuery, "?")
	if extraQuery == "" {
		return p
	} else {
		return p + "?" + extraQuery
	}
}

// Current size of block names in AzCopy is 48B. To be consistent with this,
// we have to generate a 36B string and then base64-encode this to retain the
// same size.
// Block Names of blobs are of format noted below.
// <5B empty placeholder> <16B GUID of AzCopy re-interpreted as string><5B PartNum><5B Index in the jobPart><5B blockNum>
const AZCOPY_BLOCKNAME_LENGTH = 48

func GenerateBlockBlobBlockID(blockNamePrefix string, index int32) string {
	blockID := []byte(fmt.Sprintf("%s%05d", blockNamePrefix, index))
	return base64.StdEncoding.EncodeToString(blockID)
}
