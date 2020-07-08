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
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/azbfs"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/azure-storage-file-go/azfile"
)

const (
	NumOfFilesPerDispatchJobPart = 10000
)

type copyHandlerUtil struct{}

// TODO: Need be replaced with anonymous embedded field technique.
var gCopyUtil = copyHandlerUtil{}

const wildCard = "*"

// checks if a given url points to a container or virtual directory, as opposed to a blob or prefix match
func (util copyHandlerUtil) urlIsContainerOrVirtualDirectory(url *url.URL) bool {
	if azblob.NewBlobURLParts(*url).IPEndpointStyleInfo.AccountName == "" {
		// Typical endpoint style
		// If there's no slashes after the first, it's a container.
		// If there's a slash on the end, it's a virtual directory/container.
		// Otherwise, it's just a blob.
		if len(url.Path) == 0 {
			return true // We know for SURE that it's a account level URL
		}

		return strings.HasSuffix(url.Path, "/") || strings.Count(url.Path[1:], "/") == 0
	} else {
		// IP endpoint style: https://IP:port/accountname/container
		// If there's 2 or less slashes after the first, it's a container.
		// OR If there's a slash on the end, it's a virtual directory/container.
		// Otherwise, it's just a blob.
		return strings.HasSuffix(url.Path, "/") || strings.Count(url.Path[1:], "/") <= 1
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
				panic(fmt.Errorf("error parsing the url %s. Failed with error %s", argUrl.String(), err.Error()))
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

func (util copyHandlerUtil) urlIsBFSFileSystemOrDirectory(ctx context.Context, url *url.URL, p pipeline.Pipeline) bool {
	if util.urlIsContainerOrVirtualDirectory(url) {

		return true
	}
	// Need to get the resource properties and verify if it is a file or directory
	dirURL := azbfs.NewDirectoryURL(*url, p)
	isDir, err := dirURL.IsDirectory(context.Background())

	if err != nil {
		if ste.JobsAdmin != nil {
			ste.JobsAdmin.LogToJobLog(fmt.Sprintf("Failed to check if destination is a folder or a file (ADLSg2). Assuming the destination is a file: %s", err), pipeline.LogWarning)
		}
	}

	return isDir
}

func (util copyHandlerUtil) urlIsAzureFileDirectory(ctx context.Context, url *url.URL, p pipeline.Pipeline) bool {
	// Azure file share case
	if util.urlIsContainerOrVirtualDirectory(url) {
		return true
	}

	// Need make request to ensure if it's directory
	directoryURL := azfile.NewDirectoryURL(*url, p)
	_, err := directoryURL.GetProperties(ctx)
	if err != nil {
		if ste.JobsAdmin != nil {
			ste.JobsAdmin.LogToJobLog(fmt.Sprintf("Failed to check if the destination is a folder or a file (Azure Files). Assuming the destination is a file: %s", err), pipeline.LogWarning)
		}

		return false
	}

	return true
}

// append a file name to the container path to generate a blob path
func (copyHandlerUtil) generateObjectPath(destinationPath, fileName string) string {
	if strings.LastIndex(destinationPath, "/") == len(destinationPath)-1 {
		return fmt.Sprintf("%s%s", destinationPath, fileName)
	}
	return fmt.Sprintf("%s/%s", destinationPath, fileName)
}

func (util copyHandlerUtil) getBlobNameFromURL(path string) string {
	// return everything after the second /
	return strings.SplitAfterN(path[1:], common.AZCOPY_PATH_SEPARATOR_STRING, 2)[1]
}

func (util copyHandlerUtil) firstIndexOfWildCard(name string) int {
	return strings.Index(name, wildCard)
}
func (util copyHandlerUtil) getContainerURLFromString(url url.URL) url.URL {
	blobParts := azblob.NewBlobURLParts(url)
	blobParts.BlobName = ""
	return blobParts.URL()
	//containerName := strings.SplitAfterN(url.Path[1:], "/", 2)[0]
	//url.Path = "/" + containerName
	//return url
}

func (util copyHandlerUtil) getContainerUrl(blobParts azblob.BlobURLParts) url.URL {
	blobParts.BlobName = ""
	return blobParts.URL()
}

// doesBlobRepresentAFolder verifies whether blob is valid or not.
// Used to handle special scenarios or conditions.
func (util copyHandlerUtil) doesBlobRepresentAFolder(metadata azblob.Metadata) bool {
	// this condition is to handle the WASB V1 directory structure.
	// HDFS driver creates a blob for the empty directories (let’s call it ‘myfolder’)
	// and names all the blobs under ‘myfolder’ as such: ‘myfolder/myblob’
	// The empty directory has meta-data 'hdi_isfolder = true'
	return metadata["hdi_isfolder"] == "true"
}

func startsWith(s string, t string) bool {
	return len(s) >= len(t) && strings.EqualFold(s[0:len(t)], t)
}

/////////////////////////////////////////////////////////////////////////////////////////////////
type s3URLPartsExtension struct {
	common.S3URLParts
}
