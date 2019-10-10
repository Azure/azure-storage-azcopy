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
	"strings"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-file-go/azfile"

	"github.com/Azure/azure-storage-azcopy/common"
)

// allow us to iterate through a path pointing to the file endpoint
type fileTraverser struct {
	rawURL        *url.URL
	p             pipeline.Pipeline
	ctx           context.Context
	recursive     bool
	getProperties bool

	// a generic function to notify that a new stored object has been enumerated
	incrementEnumerationCounter func()
}

func (t *fileTraverser) isDirectory(bool) bool {
	return copyHandlerUtil{}.urlIsAzureFileDirectory(t.ctx, t.rawURL, t.p) // This handles all of the fanciness for us.
}

func (t *fileTraverser) getPropertiesIfSingleFile() (*azfile.FileGetPropertiesResponse, bool) {
	fileURL := azfile.NewFileURL(*t.rawURL, t.p)
	fileProps, filePropertiesErr := fileURL.GetProperties(t.ctx)

	// if there was no problem getting the properties, it means that we are looking at a single file
	if filePropertiesErr == nil {
		return fileProps, true
	}

	return nil, false
}

func (t *fileTraverser) traverse(preprocessor objectMorpher, processor objectProcessor, filters []objectFilter) (err error) {
	targetURLParts := azfile.NewFileURLParts(*t.rawURL)

	// if not pointing to a share, check if we are pointing to a single file
	if targetURLParts.DirectoryOrFilePath != "" {
		// check if the url points to a single file
		fileProperties, isFile := t.getPropertiesIfSingleFile()
		if isFile {
			storedObject := newStoredObject(
				preprocessor,
				getObjectNameOnly(targetURLParts.DirectoryOrFilePath),
				"",
				fileProperties.LastModified(),
				fileProperties.ContentLength(),
				fileProperties.ContentMD5(),
				blobTypeNA,
				targetURLParts.ShareName,
			)

			storedObject.contentDisposition = fileProperties.ContentDisposition()
			storedObject.cacheControl = fileProperties.CacheControl()
			storedObject.contentLanguage = fileProperties.ContentLanguage()
			storedObject.contentEncoding = fileProperties.ContentEncoding()
			storedObject.contentType = fileProperties.ContentType()

			// .NewMetadata() seems odd to call here, but it does actually obtain the metadata.
			storedObject.Metadata = common.FromAzFileMetadataToCommonMetadata(fileProperties.NewMetadata())

			if t.incrementEnumerationCounter != nil {
				t.incrementEnumerationCounter()
			}

			return processIfPassedFilters(filters, storedObject, processor)
		}
	}

	// get the directory URL so that we can list the files
	directoryURL := azfile.NewDirectoryURL(targetURLParts.URL(), t.p)

	dirStack := &directoryStack{}
	dirStack.Push(directoryURL)
	for currentDirURL, ok := dirStack.Pop(); ok; currentDirURL, ok = dirStack.Pop() {
		// Perform list files and directories.
		for marker := (azfile.Marker{}); marker.NotDone(); {
			lResp, err := currentDirURL.ListFilesAndDirectoriesSegment(t.ctx, marker, azfile.ListFilesAndDirectoriesOptions{})
			if err != nil {
				return fmt.Errorf("cannot list files due to reason %s", err)
			}

			// Process the files returned in this segment.
			for _, fileInfo := range lResp.FileItems {
				f := currentDirURL.NewFileURL(fileInfo.Name)

				// compute the relative path of the file with respect to the target directory
				fileURLParts := azfile.NewFileURLParts(f.URL())
				relativePath := strings.TrimPrefix(fileURLParts.DirectoryOrFilePath, targetURLParts.DirectoryOrFilePath)
				relativePath = strings.TrimPrefix(relativePath, common.AZCOPY_PATH_SEPARATOR_STRING)

				// We need to omit some properties if we don't get properties
				// TODO: make it so we can (and must) call newStoredOBject here.
				storedObject := storedObject{
					name:          getObjectNameOnly(fileInfo.Name),
					relativePath:  relativePath,
					size:          fileInfo.Properties.ContentLength,
					containerName: targetURLParts.ShareName,
				}
				if preprocessor != nil { // TODO ******** REMOVE THIS ONCE USE newStoredOBject, above *******
					preprocessor(&storedObject)
				}

				// Only get the properties if we're told to
				if t.getProperties {
					fileProperties, err := f.GetProperties(t.ctx)
					if err != nil {
						return err
					}

					// Leaving this on because it's free IO wise, and file->* is in the works
					storedObject.contentDisposition = fileProperties.ContentDisposition()
					storedObject.cacheControl = fileProperties.CacheControl()
					storedObject.contentLanguage = fileProperties.ContentLanguage()
					storedObject.contentEncoding = fileProperties.ContentEncoding()
					storedObject.contentType = fileProperties.ContentType()
					storedObject.md5 = fileProperties.ContentMD5()

					// .NewMetadata() seems odd to call here, but it does actually obtain the metadata.
					storedObject.Metadata = common.FromAzFileMetadataToCommonMetadata(fileProperties.NewMetadata())

					storedObject.lastModifiedTime = fileProperties.LastModified()
				}

				if t.incrementEnumerationCounter != nil {
					t.incrementEnumerationCounter()
				}

				processErr := processIfPassedFilters(filters, storedObject, processor)
				if processErr != nil {
					return processErr
				}
			}

			// If recursive is turned on, add sub directories.
			if t.recursive {
				for _, dirInfo := range lResp.DirectoryItems {
					d := currentDirURL.NewDirectoryURL(dirInfo.Name)
					dirStack.Push(d)
				}
			}
			marker = lResp.NextMarker
		}
	}

	return
}

func newFileTraverser(rawURL *url.URL, p pipeline.Pipeline, ctx context.Context, recursive, getProperties bool, incrementEnumerationCounter func()) (t *fileTraverser) {
	t = &fileTraverser{rawURL: rawURL, p: p, ctx: ctx, recursive: recursive, getProperties: getProperties, incrementEnumerationCounter: incrementEnumerationCounter}
	return
}
