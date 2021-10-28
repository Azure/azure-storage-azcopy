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
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func ValidateFromTo(src, dst string, userSpecifiedFromTo string) (common.FromTo, error) {
	if userSpecifiedFromTo == "" {
		inferredFromTo := inferFromTo(src, dst)

		// If user didn't explicitly specify FromTo, use what was inferred (if possible)
		if inferredFromTo == common.EFromTo.Unknown() {
			return common.EFromTo.Unknown(), fmt.Errorf("the inferred source/destination combination could not be identified, or is currently not supported")
		}
		return inferredFromTo, nil
	}

	// User explicitly specified FromTo, therefore, we should respect what they specified.
	var userFromTo common.FromTo
	err := userFromTo.Parse(userSpecifiedFromTo)
	if err != nil {
		return common.EFromTo.Unknown(), fmt.Errorf("invalid --from-to value specified: %q. "+fromToHelpText, userSpecifiedFromTo)

	}

	return userFromTo, nil
}

const fromToHelpText = "Valid values are two-word phases of the form BlobLocal, LocalBlob etc.  Use the word 'Blob' for Blob Storage, " +
	"'Local' for the local file system, 'File' for Azure Files, and 'BlobFS' for ADLS Gen2. " +
	"If you need a combination that is not supported yet, please log an issue on the AzCopy GitHub issues list."

func inferFromTo(src, dst string) common.FromTo {
	// Try to infer the 1st argument
	srcLocation := InferArgumentLocation(src)
	if srcLocation == srcLocation.Unknown() {
		glcm.Info("Cannot infer source location of " +
			common.URLStringExtension(src).RedactSecretQueryParamForLogging() +
			". Please specify the --from-to switch. " + fromToHelpText)
		return common.EFromTo.Unknown()
	}

	dstLocation := InferArgumentLocation(dst)
	if dstLocation == dstLocation.Unknown() {
		glcm.Info("Cannot infer destination location of " +
			common.URLStringExtension(dst).RedactSecretQueryParamForLogging() +
			". Please specify the --from-to switch. " + fromToHelpText)
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
	case srcLocation == common.ELocation.GCP() && dstLocation == common.ELocation.Blob():
		return common.EFromTo.GCPBlob()
	}

	glcm.Info("The parameters you supplied were " +
		"Source: '" + common.URLStringExtension(src).RedactSecretQueryParamForLogging() + "' of type " + srcLocation.String() +
		", and Destination: '" + common.URLStringExtension(dst).RedactSecretQueryParamForLogging() + "' of type " + dstLocation.String())
	glcm.Info("Based on the parameters supplied, a valid source-destination combination could not " +
		"automatically be found. Please check the parameters you supplied.  If they are correct, please " +
		"specify an exact source and destination type using the --from-to switch. " + fromToHelpText)

	return common.EFromTo.Unknown()
}

var IPv4Regex = regexp.MustCompile(`\d+\.\d+\.\d+\.\d+`) // simple regex

func InferArgumentLocation(arg string) common.Location {
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
				// enable targeting an emulator/stack
			case IPv4Regex.MatchString(host):
				return common.ELocation.Unknown()
			}

			if common.IsS3URL(*u) {
				return common.ELocation.S3()
			}

			if common.IsGCPURL(*u) {
				return common.ELocation.GCP()
			}
		}
	}

	return common.ELocation.Local()
}
