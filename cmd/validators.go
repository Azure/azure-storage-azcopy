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

	"github.com/Azure/azure-storage-azcopy/common"
)

func validateFromTo(src, dst string, userSpecifiedFromTo string) (common.FromTo, error) {
	inferredFromTo := inferFromTo(src, dst)
	if userSpecifiedFromTo == "" {
		// If user didn't explicitly specify FromTo, use what was inferred (if possible)
		if inferredFromTo == common.EFromTo.Unknown() {
			return common.EFromTo.Unknown(), fmt.Errorf("the inferred source/destination combination is currently not supported. Please post an issue on Github if support for this scenario is desired")
		}
		return inferredFromTo, nil
	}

	// User explicitly specified FromTo, make sure it matches what we infer or accept it if we can't infer
	var userFromTo common.FromTo
	err := userFromTo.Parse(userSpecifiedFromTo)
	if err != nil {
		return common.EFromTo.Unknown(), fmt.Errorf("invalid --from-to value specified: %q", userSpecifiedFromTo)
	}
	if inferredFromTo == common.EFromTo.Unknown() || inferredFromTo == userFromTo ||
		userFromTo == common.EFromTo.BlobTrash() || userFromTo == common.EFromTo.FileTrash() || userFromTo == common.EFromTo.BlobFSTrash() {
		// We couldn't infer the FromTo or what we inferred matches what the user specified
		// We'll accept what the user specified
		return userFromTo, nil
	}
	// inferredFromTo != raw.fromTo: What we inferred doesn't match what the user specified
	return common.EFromTo.Unknown(), errors.New("the specified --from-to switch is inconsistent with the specified source/destination combination")
}

func inferFromTo(src, dst string) common.FromTo {
	// Try to infer the 1st argument
	srcLocation := inferArgumentLocation(src)
	if srcLocation == srcLocation.Unknown() {
		glcm.Info("Cannot infer source location of " +
			common.URLStringExtension(src).RedactSecretQueryParamForLogging() +
			". Please specify the --from-to switch")
		return common.EFromTo.Unknown()
	}

	dstLocation := inferArgumentLocation(dst)
	if dstLocation == dstLocation.Unknown() {
		glcm.Info("Cannot infer destination location of " +
			common.URLStringExtension(dst).RedactSecretQueryParamForLogging() +
			". Please specify the --from-to switch")
		return common.EFromTo.Unknown()
	}

	switch {
	case srcLocation == common.ELocation.Local() && dstLocation == common.ELocation.Blob():
		return common.EFromTo.LocalBlob()
	case srcLocation == common.ELocation.Blob() && dstLocation == common.ELocation.Local():
		return common.EFromTo.BlobLocal()
	case srcLocation == common.ELocation.Local() && dstLocation == common.ELocation.File():
		return common.EFromTo.LocalFile()
	case srcLocation == common.ELocation.File() && dstLocation == common.ELocation.Local():
		return common.EFromTo.FileLocal()
	case srcLocation == common.ELocation.Pipe() && dstLocation == common.ELocation.Blob():
		return common.EFromTo.PipeBlob()
	case srcLocation == common.ELocation.Blob() && dstLocation == common.ELocation.Pipe():
		return common.EFromTo.BlobPipe()
	case srcLocation == common.ELocation.Local() && dstLocation == common.ELocation.BlobFS():
		return common.EFromTo.LocalBlobFS()
	case srcLocation == common.ELocation.BlobFS() && dstLocation == common.ELocation.Local():
		return common.EFromTo.BlobFSLocal()
	case srcLocation == common.ELocation.Blob() && dstLocation == common.ELocation.Blob():
		return common.EFromTo.BlobBlob()
	case srcLocation == common.ELocation.File() && dstLocation == common.ELocation.Blob():
		return common.EFromTo.FileBlob()
	case srcLocation == common.ELocation.Blob() && dstLocation == common.ELocation.File():
		return common.EFromTo.BlobFile()
	case srcLocation == common.ELocation.File() && dstLocation == common.ELocation.File():
		return common.EFromTo.FileFile()
	case srcLocation == common.ELocation.S3() && dstLocation == common.ELocation.Blob():
		return common.EFromTo.S3Blob()
	case srcLocation == common.ELocation.Benchmark() && dstLocation == common.ELocation.Blob():
		return common.EFromTo.BenchmarkBlob()
	case srcLocation == common.ELocation.Benchmark() && dstLocation == common.ELocation.File():
		return common.EFromTo.BenchmarkFile()
	case srcLocation == common.ELocation.Benchmark() && dstLocation == common.ELocation.BlobFS():
		return common.EFromTo.BenchmarkBlobFS()
	}
	return common.EFromTo.Unknown()
}

func inferArgumentLocation(arg string) common.Location {
	if arg == pipeLocation {
		return common.ELocation.Pipe()
	}
	if startsWith(arg, "http") {
		// Let's try to parse the argument as a URL
		u, err := url.Parse(arg)
		// NOTE: sometimes, a local path can also be parsed as a url. To avoid thinking it's a URL, check Scheme, Host, and Path
		if err == nil && u.Scheme != "" && u.Host != "" {
			// Is the argument a URL to blob storage?
			switch host := strings.ToLower(u.Host); true {
			// Azure Stack does not have the core.windows.net
			case strings.Contains(host, ".blob"):
				return common.ELocation.Blob()
			case strings.Contains(host, ".file"):
				return common.ELocation.File()
			case strings.Contains(host, ".dfs"):
				return common.ELocation.BlobFS()
			case strings.Contains(host, benchmarkSourceHost):
				return common.ELocation.Benchmark()
			}

			if common.IsS3URL(*u) {
				return common.ELocation.S3()
			}
		}
	}

	return common.ELocation.Local()
}
