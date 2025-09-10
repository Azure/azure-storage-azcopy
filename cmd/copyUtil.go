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
	"strings"
	"sync"

	"github.com/Azure/azure-storage-azcopy/v10/traverser"

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
			glcm.Warn(fmt.Sprintf("*** Warning *** The %s parameter does not support wildcards. The wildcard "+
				"character provided will be interpreted literally and will not have any wildcard effect. To use wildcards "+
				"(in filenames only, not paths) use include-pattern or exclude-pattern", paramName))
		})
	}
}

// TODO: Need be replaced with anonymous embedded field technique.
var gCopyUtil = traverser.CopyHandlerUtil{}

// ///////////////////////////////////////////////////////////////////////////////////////////////
