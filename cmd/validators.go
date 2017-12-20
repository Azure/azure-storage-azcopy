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
	"os"
	"net/url"
	"github.com/Azure/azure-storage-azcopy/common"
)


// TODO use bit shift/or for case switching (pattern with &&)
func isSourceAndDestinationPairValid(copySource, copyDestination string) common.CopyCmdType {

	// source is local file system
	if IsLocalPath(copySource) {

		// local => local is not supported
		if IsLocalPath(copyDestination) {
			return common.Invalid
		}

		// local => Azure is valid
		if IsUrl(copyDestination) {
			return common.UploadFromLocalToWastore
		}

	}

	// source is Azure
	if IsUrl(copySource) {

		// Azure => Azure is not supported yet
		if IsUrl(copyDestination) {
			return common.Invalid
		}

		// Azure => local is valid
		if IsLocalPath(copyDestination) {
			return common.DownloadFromWastoreToLocal
		}
	}

	return common.Invalid
}

// verify if path is a valid local path
func IsLocalPath(path string) bool {
	// TODO comment
	_, err := os.Stat(path)
	// TODO comment
	if err == nil || (!IsUrl(path) && os.IsNotExist(err)){
		return true
	}
	return false
}

// verify if givenUrl is a valid url
func IsUrl(givenUrl string) bool{
	u, err := url.Parse(givenUrl)
	// TODO comment
	if err != nil {
		return false
	}
	// TODO comment
	if u.Host == "" || u.Scheme == "" || u.Path == "" {
		return false
	}
	return true
}