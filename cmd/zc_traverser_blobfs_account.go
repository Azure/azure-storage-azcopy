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

	"github.com/Azure/azure-pipeline-go/pipeline"

	"github.com/Azure/azure-storage-azcopy/v10/azbfs"
)

// We don't allow S2S from BlobFS, but what this gives us is the ability for users to download entire accounts at once.
// This is just added to create that feature parity.
// Enumerates an entire blobFS account, looking into each matching filesystem as it goes
type BlobFSAccountTraverser struct {
	accountURL        azbfs.ServiceURL
	p                 pipeline.Pipeline
	ctx               context.Context
	fileSystemPattern string
	cachedFileSystems []string

	// a generic function to notify that a new stored object has been enumerated
	incrementEnumerationCounter enumerationCounterFunc
}

func (t *BlobFSAccountTraverser) IsDirectory(isSource bool) bool {
	return true // Returns true as account traversal is inherently folder-oriented and recursive.
}

func (t *BlobFSAccountTraverser) listContainers() ([]string, error) {
	// a nil list also returns 0
	if len(t.cachedFileSystems) == 0 {
		marker := ""
		fsList := make([]string, 0)

		for {
			resp, err := t.accountURL.ListFilesystemsSegment(t.ctx, &marker)

			if err != nil {
				return nil, err
			}

			for _, v := range resp.Filesystems {
				var fsName string

				if v.Name != nil {
					fsName = *v.Name
				} else {
					// realistically this should never ever happen
					// but on the off-chance that it does, should we panic?
					WarnStdoutAndScanningLog("filesystem listing returned nil filesystem name")
					continue
				}

				// match against the filesystem name pattern if present
				if t.fileSystemPattern != "" {
					if ok, err := containerNameMatchesPattern(fsName, t.fileSystemPattern); err != nil {
						return nil, err
					} else if !ok {
						// ignore any filesystems that don't match
						continue
					}
				}

				fsList = append(fsList, fsName)
			}

			marker = resp.XMsContinuation()
			if marker == "" {
				break
			}
		}

		t.cachedFileSystems = fsList
		return fsList, nil
	} else {
		return t.cachedFileSystems, nil
	}
}

func (t *BlobFSAccountTraverser) Traverse(preprocessor objectMorpher, processor objectProcessor, filters []ObjectFilter) error {
	// listContainers will return the cached filesystem list if filesystems have already been listed by this traverser.
	fsList, err := t.listContainers()

	for _, v := range fsList {
		fileSystemURL := t.accountURL.NewFileSystemURL(v).URL()
		fileSystemTraverser := newBlobFSTraverser(&fileSystemURL, t.p, t.ctx, true, t.incrementEnumerationCounter)

		preprocessorForThisChild := preprocessor.FollowedBy(newContainerDecorator(v))

		err = fileSystemTraverser.Traverse(preprocessorForThisChild, processor, filters)

		if err != nil {
			WarnStdoutAndScanningLog(fmt.Sprintf("failed to list files in filesystem %s: %s", v, err))
			continue
		}
	}

	return nil
}

func newBlobFSAccountTraverser(rawURL *url.URL, p pipeline.Pipeline, ctx context.Context, incrementEnumerationCounter enumerationCounterFunc) (t *BlobFSAccountTraverser) {
	bfsURLParts := azbfs.NewBfsURLParts(*rawURL)
	fsPattern := bfsURLParts.FileSystemName

	if bfsURLParts.FileSystemName != "" {
		bfsURLParts.FileSystemName = ""
	}

	t = &BlobFSAccountTraverser{p: p, ctx: ctx, incrementEnumerationCounter: incrementEnumerationCounter, accountURL: azbfs.NewServiceURL(bfsURLParts.URL(), p), fileSystemPattern: fsPattern}

	return
}
