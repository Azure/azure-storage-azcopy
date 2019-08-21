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

func (t *blobFSTraverser) traverse(processor objectProcessor, filters []objectFilter) (err error) {
	bfsURLParts := azbfs.NewBfsURLParts(*t.rawURL)

	pathProperties, isFile, _ := t.getPropertiesIfSingleFile()
	if isFile {
		storedObject := newStoredObject(
			getObjectNameOnly(bfsURLParts.DirectoryOrFilePath),
			"", // We already know the exact location of the file. No need for a relative path.
			t.parseLMT(pathProperties.LastModified()),
			pathProperties.ContentLength(),
			pathProperties.ContentMD5(),
			blobTypeNA,
			"") // We already know the container name -- no need.

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
					getObjectNameOnly(*v.Name),
					strings.TrimPrefix(*v.Name, searchPrefix),
					v.LastModifiedTime(),
					*v.ContentLength,
					v.ContentMD5(),
					blobTypeNA,
					"") // We already know the container name -- no need.

				if t.incrementEnumerationCounter != nil {
					t.incrementEnumerationCounter()
				}

				err := processIfPassedFilters(filters, storedObject, processor)
				if err != nil {
					return err
				}
			}
		}

		if marker == "" { // do-while pattern
			break
		}
	}

	return
}
