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
	"errors"
	"net/url"

	"github.com/Azure/azure-pipeline-go/pipeline"

	"github.com/Azure/azure-storage-azcopy/azbfs"
)

// We don't allow S2S from BlobFS, but what this gives us is the ability for users to download entire accounts at once.
// This is just added to create that feature parity.
// Enumerates an entire blobFS account, looking into each matching filesystem as it goes
type BlobFSAccountTraverser struct {
	accountURL        azbfs.ServiceURL
	p                 pipeline.Pipeline
	ctx               context.Context
	fileSystemPattern string

	// a generic function to notify that a new stored object has been enumerated
	incrementEnumerationCounter func()
}

func (t *BlobFSAccountTraverser) traverse(processor objectProcessor, filters []objectFilter) error {
	marker := ""
	for {
		resp, err := t.accountURL.ListFilesystemsSegment(t.ctx, &marker)

		if err != nil {
			return err
		}

		for _, v := range resp.Filesystems {
			var fsName string
			if v.Name != nil {
				fsName = *v.Name
			} else {
				return errors.New("filesystem listing returned nil container names")
			}

			if t.fileSystemPattern != "" {
				if ok, err := containerNameMatchesPattern(fsName, t.fileSystemPattern); err != nil {
					return err
				} else if !ok {
					continue
				}
			}

			fileSystemURL := t.accountURL.NewFileSystemURL(fsName).URL()
			fileSystemTraverser := newBlobFSTraverser(&fileSystemURL, t.p, t.ctx, true, t.incrementEnumerationCounter)

			middlemanProcessor := initContainerDecorator(fsName, processor)

			err = fileSystemTraverser.traverse(middlemanProcessor, filters)

			if err != nil {
				return err
			}
		}

		marker = resp.XMsContinuation()
		if marker == "" {
			break
		}
	}

	return nil
}

func newBlobFSAccountTraverser(rawURL *url.URL, p pipeline.Pipeline, ctx context.Context, incrementEnumerationCounter func()) (t *BlobFSAccountTraverser) {
	bfsURLParts := azbfs.NewBfsURLParts(*rawURL)
	fsPattern := bfsURLParts.FileSystemName

	if bfsURLParts.FileSystemName != "" {
		bfsURLParts.FileSystemName = ""
	}

	t = &BlobFSAccountTraverser{p: p, ctx: ctx, incrementEnumerationCounter: incrementEnumerationCounter, accountURL: azbfs.NewServiceURL(bfsURLParts.URL(), p), fileSystemPattern: fsPattern}

	return
}
