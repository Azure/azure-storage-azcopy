package common

import (
	"bytes"
	"net/http"
	"net/url"
	"runtime"
	"strings"

	"github.com/Azure/azure-storage-azcopy/azbfs"

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
	// url.RawPath is not always present. Which is likely, if we're _just_ using +.
	if u.RawPath != "" {
		// If we're working with a raw path, then we need to be extra super careful about what we change.
		// Thus, we'll follow both paths and account for encoding.
		// rawIndex is our location in the rawPath, index is our location in the path.
		rawIndex, index := 0, 0

		// We convert to rune arrays because strings aren't mutable in Go.
		path, rawPath := []rune(u.Path), []rune(u.RawPath)

		for rawIndex < len(rawPath) && index < len(path) {
			rawChar := rawPath[rawIndex]
			char := path[index]

			// Check that the characters are the exact same.
			// If rawIndex is encoded (%XX), increment by an additional two before we loop again.
			isRawEncoded := rawChar != char

			// This check wouldn't trigger in a string like aaaa%aaaaa but it'd be a fine indicator for something more complex.
			if isRawEncoded && rawChar != '%' {
				panic("safe encoding swap sanity check hit-- indexes are not equivalent.")
			}

			// We want to ignore encoded characters-- They're usually meant literally, which'd produce a bug if we replaced them.
			if isRawEncoded {
				rawIndex += 2
			} else {
				// Replace pluses with spaces, as this function does.
				if char == '+' {
					path[index] = ' '
					// We must change the raw path to using %20 otherwise, when go stringifies, it can't properly calculate the original, and encodes the literal path.
					rawPath = append(
						append(
							// To insert extra characters
							// first, reallocate to a new array.
							append(make([]rune, 0), rawPath[:rawIndex]...),
							// append the extra characters
							[]rune("%20")...),
						// then append the remaining characters
						rawPath[rawIndex+1:]...)
					rawIndex += 2 // since we encoded the character, let's bump up two.
				}
			}

			// Increment our indexes to move to the next character.
			rawIndex++
			index++
		}

		if rawIndex != len(rawPath) || index != len(path) {
			panic("sanity check, both indexes are not at the end of the rawpath and path strings")
		}

		// return path and rawpath back to the url struct
		u.Path, u.RawPath = string(path), string(rawPath)
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
