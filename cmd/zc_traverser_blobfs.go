// Copyright © Microsoft <wastore@microsoft.com>
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
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"

	"github.com/Azure/azure-storage-azcopy/azbfs"
	"github.com/Azure/azure-storage-azcopy/common"
)

type blobFSTraverser struct {
	rawURL    *url.URL
	p         pipeline.Pipeline
	ctx       context.Context
	recursive bool

	// Generic function to indicate that a new stored object has been enumerated
	incrementEnumerationCounter func()
}

func newBlobFSTraverser(rawURL *url.URL, p pipeline.Pipeline, ctx context.Context, recursive bool, incrementEnumerationCounter func()) (t *blobFSTraverser) {
	t = &blobFSTraverser{
		rawURL:                      rawURL,
		p:                           p,
		ctx:                         ctx,
		recursive:                   recursive,
		incrementEnumerationCounter: incrementEnumerationCounter,
	}
	return
}

func (t *blobFSTraverser) isDirectory(bool) bool {
	return copyHandlerUtil{}.urlIsBFSFileSystemOrDirectory(t.ctx, t.rawURL, t.p) // This gets all the fanciness done for us.
}

func (t *blobFSTraverser) getPropertiesIfSingleFile() (*azbfs.PathGetPropertiesResponse, bool, error) {
	pathURL := azbfs.NewFileURL(*t.rawURL, t.p)
	pgr, err := pathURL.GetProperties(t.ctx)

	if err != nil {
		return nil, false, err
	}

	if pgr.XMsResourceType() == "directory" {
		return pgr, false, nil
	}

	return pgr, true, nil
}

func (_ *blobFSTraverser) parseLMT(t string) time.Time {
	out, err := time.Parse(time.RFC1123, t)

	if err != nil {
		return time.Time{}
	}

	return out
}

func (t *blobFSTraverser) traverse(preprocessor objectMorpher, processor objectProcessor, filters []objectFilter) (err error) {
	bfsURLParts := azbfs.NewBfsURLParts(*t.rawURL)

	pathProperties, isFile, _ := t.getPropertiesIfSingleFile()
	if isFile {
		storedObject := newStoredObject(
			preprocessor,
			getObjectNameOnly(bfsURLParts.DirectoryOrFilePath),
			"",
			t.parseLMT(pathProperties.LastModified()),
			pathProperties.ContentLength(),
			pathProperties.ContentMD5(),
			blobTypeNA,
			bfsURLParts.FileSystemName,
		)

		/* TODO: Enable this code segment in case we ever do BlobFS->Blob transfers.
		Read below comment for info
		storedObject.contentDisposition = pathProperties.ContentDisposition()
		storedObject.cacheControl = pathProperties.CacheControl()
		storedObject.contentLanguage = pathProperties.ContentLanguage()
		storedObject.contentEncoding = pathProperties.ContentEncoding()
		storedObject.contentType = pathProperties.ContentType()
		storedObject.metadata = .... */

		if t.incrementEnumerationCounter != nil {
			t.incrementEnumerationCounter()
		}

		return processIfPassedFilters(filters, storedObject, processor)
	}

	dirUrl := azbfs.NewDirectoryURL(*t.rawURL, t.p)
	marker := ""
	searchPrefix := bfsURLParts.DirectoryOrFilePath

	if !strings.HasSuffix(searchPrefix, common.AZCOPY_PATH_SEPARATOR_STRING) {
		searchPrefix += common.AZCOPY_PATH_SEPARATOR_STRING
	}

	for {
		dlr, err := dirUrl.ListDirectorySegment(t.ctx, &marker, t.recursive)

		if err != nil {
			return fmt.Errorf("could not list files. Failed with error %s", err.Error())
		}

		for _, v := range dlr.Paths {
			if v.IsDirectory == nil {
				storedObject := newStoredObject(
					preprocessor,
					getObjectNameOnly(*v.Name),
					strings.TrimPrefix(*v.Name, searchPrefix),
					v.LastModifiedTime(),
					*v.ContentLength,
					t.getContentMd5(t.ctx, dirUrl, v),
					blobTypeNA,
					bfsURLParts.FileSystemName,
				)

				/* TODO: Enable this code segment in the case we ever do BlobFS->Blob transfers.

				I leave this here for the sake of feature parity in the future, and because it feels weird letting the other traversers have it but not this one.

				pathProperties, err := dirUrl.NewFileURL(storedObject.relativePath).GetProperties(t.ctx)

				if err == nil {
					storedObject.contentDisposition = pathProperties.ContentDisposition()
					storedObject.cacheControl = pathProperties.CacheControl()
					storedObject.contentLanguage = pathProperties.ContentLanguage()
					storedObject.contentEncoding = pathProperties.ContentEncoding()
					storedObject.contentType = pathProperties.ContentType()
				    storedObject.metadata ...
				}*/

				if t.incrementEnumerationCounter != nil {
					t.incrementEnumerationCounter()
				}

				err := processIfPassedFilters(filters, storedObject, processor)
				if err != nil {
					return err
				}
			}
		}

		marker = dlr.XMsContinuation()
		if marker == "" { // do-while pattern
			break
		}
	}

	return
}

// globalBlobFSMd5ValidationOption is an ugly workaround, to tweak performance of another ugly workaround (namely getContentMd5, below)
var globalBlobFSMd5ValidationOption = common.EHashValidationOption.FailIfDifferentOrMissing() // default to strict, if not set

// getContentMd5 compensates for the fact that ADLS Gen 2 currently does not return MD5s in the PathListResponse (even
// tho the property is there in the swagger and the generated API)
func (t *blobFSTraverser) getContentMd5(ctx context.Context, directoryURL azbfs.DirectoryURL, file azbfs.Path) []byte {
	if globalBlobFSMd5ValidationOption == common.EHashValidationOption.NoCheck() {
		return nil // not gonna check it, so don't need it
	}

	var returnValueForError []byte = nil // If we get an error, we just act like there was no content MD5. If validation is set to fail on error, this will fail the transfer of this file later on (at the time of the MD5 check)

	// convert format of what we have, if we have something in the PathListResponse from Service
	if file.ContentMD5Base64 != nil {
		value, err := base64.StdEncoding.DecodeString(*file.ContentMD5Base64)
		if err != nil {
			return returnValueForError
		}
		return value
	}

	// Fall back to making a new round trip to the server
	// This is an interim measure, so that we can still validate MD5s even before they are being returned in the server's
	// PathList response
	// TODO: remove this in a future release, once we know that Service is always returning the MD5s in the PathListResponse.
	//     Why? Because otherwise, if there's a file with NO MD5, we'll make a round-trip here, but that's pointless if we KNOW that
	//     that Service is always returning them in the PathListResponse which we've already checked above.
	//     As at mid-Feb 2019, we don't KNOW that (in fact it's not returning them in the PathListResponse) so we need this code for now.
	fileURL := directoryURL.FileSystemURL().NewDirectoryURL(*file.Name)
	props, err := fileURL.GetProperties(ctx)
	if err != nil {
		return returnValueForError
	}
	return props.ContentMD5()
}
