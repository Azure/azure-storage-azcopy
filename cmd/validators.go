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
	"github.com/Azure/azure-storage-azcopy/common"
	"net/url"
	"os"
)

type validator struct {}

func (validator validator) determineLocationType(stringToParse string) common.LocationType {
	if validator.isLocalPath(stringToParse) {
		return common.Local
	} else if validator.isUrl(stringToParse) {
		return common.Blob
	} else {
		return common.Unknown
	}
}

// verify if path is a valid local path
func (validator validator) isLocalPath(path string) bool {
	// attempting to get stats from the OS validates whether a given path is a valid local path
	_, err := os.Stat(path)
	// in case the path does not exist yet, an err is returned
	// we need to make sure that it is indeed just a local path that does not exist yet, and not a url
	if err == nil || (!validator.isUrl(path) && os.IsNotExist(err)) {
		return true
	}
	return false
}

// verify if givenUrl is a valid url
func (validator) isUrl(givenUrl string) bool {
	u, err := url.Parse(givenUrl)
	// attempting to parse the url validates whether a given string is a valid url
	if err != nil {
		return false
	}
	// a local path can also be parsed as a url sometimes, so in this case we make sure it is not a local path
	// as Host, Scheme, and Path would be absent if it were a local path
	if u.Host == "" || u.Scheme == "" || u.Path == "" {
		return false
	}
	return true
}
