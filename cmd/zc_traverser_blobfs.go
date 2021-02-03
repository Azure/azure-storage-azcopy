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
	incrementEnumerationCounter enumerationCounterFunc
}

func newBlobFSTraverser(rawURL *url.URL, p pipeline.Pipeline, ctx context.Context, recursive bool, incrementEnumerationCounter enumerationCounterFunc) (t *blobFSTraverser) {
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

func (t *blobFSTraverser) getFolderProps() (p contentPropsProvider, size int64) {
	return noContentProps, 0
}

func (t *blobFSTraverser) traverse(preprocessor objectMorpher, processor objectProcessor, filters []objectFilter) (err error) {
	bfsURLParts := azbfs.NewBfsURLParts(*t.rawURL)

	pathProperties, isFile, _ := t.getPropertiesIfSingleFile()
	if isFile {
		storedObject := newStoredObject(
			preprocessor,
			getObjectNameOnly(bfsURLParts.DirectoryOrFilePath),
			"",
			common.EEntityType.File(),
			t.parseLMT(pathProperties.LastModified()),
			pathProperties.ContentLength(),
			md5OnlyAdapter{md5: pathProperties.ContentMD5()}, // not supplying full props, since we can't below, and it would be inconsistent to do so here
			noBlobProps,
			noMetdata, // not supplying metadata, since we can't below and it would be inconsistent to do so here
			bfsURLParts.FileSystemName,
		)

		if t.incrementEnumerationCounter != nil {
			t.incrementEnumerationCounter(common.EEntityType.File())
		}

		err := processIfPassedFilters(filters, storedObject, processor)
		_, err = getProcessingError(err)
		return err
	}

	// else, its not just one file

	// Include the root dir in the enumeration results
	// Our rule is that enumerators of folder-aware sources must always include the root folder's properties
	// So include it if its a directory (which exists), or the file system root.
	contentProps, size := t.getFolderProps()
	if pathProperties != nil || bfsURLParts.DirectoryOrFilePath == "" {
		rootLmt := time.Time{} // if root is filesystem (no path) then we won't have any properties to get an LMT from.  Also, we won't actually end up syncing the folder, since its not really a folder, so it's OK to use a zero-like time here
		if pathProperties != nil {
			rootLmt = t.parseLMT(pathProperties.LastModified())
		}

		storedObject := newStoredObject(
			preprocessor,
			"",
			"", // it IS the root, so has no name within the root
			common.EEntityType.Folder(),
			rootLmt,
			size,
			contentProps,
			noBlobProps,
			noMetdata,
			bfsURLParts.FileSystemName)
		if t.incrementEnumerationCounter != nil {
			t.incrementEnumerationCounter(common.EEntityType.Folder())
		}
		err = processIfPassedFilters(filters, storedObject, processor)
		_, err = getProcessingError(err)
		if err != nil {
			return err
		}
	}

	// enumerate everything inside the folder
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
			var entityType common.EntityType
			lmt := v.LastModifiedTime()
			if v.IsDirectory == nil || *v.IsDirectory == false {
				entityType = common.EEntityType.File()
				contentProps = md5OnlyAdapter{md5: t.getContentMd5(t.ctx, dirUrl, v)}
				size = *v.ContentLength
			} else {
				entityType = common.EEntityType.Folder()
				contentProps, size = t.getFolderProps()
			}

			// TODO: if we need to get full properties and metadata, then add call here to
			//     dirUrl.NewFileURL(storedObject.relativePath).GetProperties(t.ctx)
			//     AND consider also supporting alternate mechanism to get the props in the backend
			//     using s2sGetPropertiesInBackend
			storedObject := newStoredObject(
				preprocessor,
				getObjectNameOnly(*v.Name),
				strings.TrimPrefix(*v.Name, searchPrefix),
				entityType,
				lmt,
				size,
				contentProps,
				noBlobProps,
				noMetdata,
				bfsURLParts.FileSystemName,
			)

			if t.incrementEnumerationCounter != nil {
				t.incrementEnumerationCounter(entityType)
			}

			err := processIfPassedFilters(filters, storedObject, processor)
			_, err = getProcessingError(err)
			if err != nil {
				return err
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
