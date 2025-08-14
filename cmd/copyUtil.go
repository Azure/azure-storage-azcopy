// Copyright © 2017 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cmd

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type boolDefaultTrue struct {
	value         bool
	isManuallySet bool // whether the variable was manually set by the user
}

func (b boolDefaultTrue) Value() bool {
	return b.value
}

func (b boolDefaultTrue) ValueToValidate() bool {
	if b.isManuallySet {
		return b.value
	} else {
		return false
	}
}

func parsePatterns(pattern string) (cookedPatterns []string) {
	cookedPatterns = make([]string, 0)
	rawPatterns := strings.Split(pattern, ";")
	for _, pattern := range rawPatterns {

		// skip the empty patterns
		if len(pattern) != 0 {
			cookedPatterns = append(cookedPatterns, pattern)
		}
	}

	return
}

// returns result of stripping and if striptopdir is enabled
// if nothing happens, the original source is returned
func stripTrailingWildcardOnRemoteSource(source string, location common.Location) (result string, stripTopDir bool, err error) {
	result = source
	resourceURL, err := url.Parse(result)
	gURLParts := common.NewGenericResourceURLParts(*resourceURL, location)

	if err != nil {
		err = fmt.Errorf("failed to parse url %s; %w", result, err)
		return
	}

	if strings.Contains(gURLParts.GetContainerName(), "*") {
		// Disallow container name search and object specifics
		if gURLParts.GetObjectName() != "" {
			err = errors.New("cannot combine a specific object name with an account-level search")
			return
		}

		// Return immediately here because we know this will be safe.
		return
	}

	// Trim the trailing /*.
	if strings.HasSuffix(resourceURL.RawPath, "/*") {
		resourceURL.RawPath = strings.TrimSuffix(resourceURL.RawPath, "/*")
		resourceURL.Path = strings.TrimSuffix(resourceURL.Path, "/*")
		stripTopDir = true
	}

	// Ensure there aren't any extra *s floating around.
	if strings.Contains(resourceURL.RawPath, "*") {
		err = errors.New("cannot use wildcards in the path section of the URL except in trailing \"/*\". If you wish to use * in your URL, manually encode it to %2A")
		return
	}

	result = resourceURL.String()

	return
}

func warnIfAnyHasWildcard(oncer *sync.Once, paramName string, value []string) {
	for _, v := range value {
		warnIfHasWildcard(oncer, paramName, v)
	}
}

func warnIfHasWildcard(oncer *sync.Once, paramName string, value string) {
	if strings.Contains(value, "*") || strings.Contains(value, "?") {
		oncer.Do(func() {
			glcm.OnWarning(fmt.Sprintf("*** Warning *** The %s parameter does not support wildcards. The wildcard "+
				"character provided will be interpreted literally and will not have any wildcard effect. To use wildcards "+
				"(in filenames only, not paths) use include-pattern or exclude-pattern", paramName))
		})
	}
}

type copyHandlerUtil struct{}

// TODO: Need be replaced with anonymous embedded field technique.
var gCopyUtil = copyHandlerUtil{}

// checks if a given url points to a container or virtual directory, as opposed to a blob or prefix match
func (util copyHandlerUtil) urlIsContainerOrVirtualDirectory(rawURL string) bool {
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	blobURLParts, err := blob.ParseURL(rawURL)
	if err != nil {
		return false
	}
	if blobURLParts.IPEndpointStyleInfo.AccountName == "" {
		// Typical endpoint style
		// If there's no slashes after the first, it's a container.
		// If there's a slash on the end, it's a virtual directory/container.
		// Otherwise, it's just a blob.
		if len(parsedURL.Path) == 0 {
			return true // We know for SURE that it's a account level URL
		}

		return strings.HasSuffix(parsedURL.Path, "/") || strings.Count(parsedURL.Path[1:], "/") == 0
	} else {
		// IP endpoint style: https://IP:port/accountname/container
		// If there's 2 or less slashes after the first, it's a container.
		// OR If there's a slash on the end, it's a virtual directory/container.
		// Otherwise, it's just a blob.
		return strings.HasSuffix(parsedURL.Path, "/") || strings.Count(parsedURL.Path[1:], "/") <= 1
	}
}

// redactSigQueryParam checks for the signature in the given rawquery part of the url
// If the signature exists, it replaces the value of the signature with "REDACTED"
// This api is used when SAS is written to log file to avoid exposing the user given SAS
// TODO: remove this, redactSigQueryParam could be added in SDK
func (util copyHandlerUtil) redactSigQueryParam(rawQuery string) (bool, string) {
	rawQuery = strings.ToLower(rawQuery) // lowercase the string so we can look for ?sig= and &sig=
	sigFound := strings.Contains(rawQuery, "?"+common.SigAzure+"=")
	if !sigFound {
		sigFound = strings.Contains(rawQuery, "&"+common.SigAzure+"=")
		if !sigFound {
			return sigFound, rawQuery // [?|&]sig= not found; return same rawQuery passed in (no memory allocation)
		}
	}
	// [?|&]sig= found, redact its value
	values, _ := url.ParseQuery(rawQuery)
	for name := range values {
		if strings.EqualFold(name, common.SigAzure) {
			values[name] = []string{"REDACTED"}
		}
	}
	return sigFound, values.Encode()
}

// ConstructCommandStringFromArgs creates the user given commandString from the os Arguments
// If any argument passed is an http Url and contains the signature, then the signature is redacted
func (util copyHandlerUtil) ConstructCommandStringFromArgs() string {
	// Get the os Args and strip away the first argument since it will be the path of Azcopy executable
	args := os.Args[1:]
	if len(args) == 0 {
		return ""
	}
	s := strings.Builder{}
	for _, arg := range args {
		// If the argument starts with http, it is either the remote source or remote destination
		// If there exists a signature in the argument string it needs to be redacted
		if startsWith(arg, "http") {
			// parse the url
			argUrl, err := url.Parse(arg)
			// If there is an error parsing the url, then throw the error
			if err != nil {
				panic(fmt.Errorf("error parsing the url %s. Failed with error %s", arg, err.Error()))
			}
			// Check for the signature query parameter
			_, rawQuery := util.redactSigQueryParam(argUrl.RawQuery)
			argUrl.RawQuery = rawQuery
			s.WriteString(argUrl.String())
		} else {
			s.WriteString(arg)
		}
		s.WriteString(" ")
	}
	return s.String()
}

// doesBlobRepresentAFolder verifies whether blob is valid or not.
// Used to handle special scenarios or conditions.
func (util copyHandlerUtil) doesBlobRepresentAFolder(metadata map[string]*string) bool {
	// this condition is to handle the WASB V1 directory structure.
	// HDFS driver creates a blob for the empty directories (let’s call it ‘myfolder’)
	// and names all the blobs under ‘myfolder’ as such: ‘myfolder/myblob’
	// The empty directory has meta-data 'hdi_isfolder = true'
	// Note: GoLang sometimes sets metadata keys with the first letter capitalized
	v, ok := common.TryReadMetadata(metadata, common.POSIXFolderMeta)
	return ok && v != nil && strings.ToLower(*v) == "true"
}

func startsWith(s string, t string) bool {
	return len(s) >= len(t) && strings.EqualFold(s[0:len(t)], t)
}

// ///////////////////////////////////////////////////////////////////////////////////////////////
type s3URLPartsExtension struct {
	common.S3URLParts
}
