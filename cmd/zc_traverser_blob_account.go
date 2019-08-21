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
	"net/url"
	"path/filepath"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

// Enumerates an entire blob account, looking into each matching container as it goes
type blobAccountTraverser struct {
	accountURL       azblob.ServiceURL
	p                pipeline.Pipeline
	ctx              context.Context
	containerPattern string

	// a generic function to notify that a new stored object has been enumerated
	incrementEnumerationCounter func()
}

func (t *blobAccountTraverser) traverse(processor objectProcessor, filters []objectFilter) error {
	marker := azblob.Marker{}
	for marker.NotDone() {
		resp, err := t.accountURL.ListContainersSegment(t.ctx, marker, azblob.ListContainersSegmentOptions{})

		if err != nil {
			return err
		}

		for _, v := range resp.ContainerItems {
			// Match a pattern for the container name and the container name only.
			if t.containerPattern != "" {
				if ok, err := filepath.Match(t.containerPattern, v.Name); err != nil {
					// Break if the pattern is invalid
					return err
				} else if !ok {
					// Ignore the container if it doesn't match the pattern.
					continue
				}
			}

			containerURL := t.accountURL.NewContainerURL(v.Name).URL()
			containerTraverser := newBlobTraverser(&containerURL, t.p, t.ctx, true, t.incrementEnumerationCounter)

			middlemanProcessor := initContainerDecorator(v.Name, processor)

			err = containerTraverser.traverse(middlemanProcessor, filters)

			if err != nil {
				return err
			}
		}

		marker = resp.NextMarker
	}

	return nil
}

func newBlobAccountTraverser(rawURL *url.URL, p pipeline.Pipeline, ctx context.Context, incrementEnumerationCounter func()) (t *blobAccountTraverser) {
	bURLParts := azblob.NewBlobURLParts(*rawURL)
	cPattern := bURLParts.ContainerName

	// Strip the container name away and treat it as a pattern
	if bURLParts.ContainerName != "" {
		bURLParts.ContainerName = ""
	}

	t = &blobAccountTraverser{p: p, ctx: ctx, incrementEnumerationCounter: incrementEnumerationCounter, accountURL: azblob.NewServiceURL(bURLParts.URL(), p), containerPattern: cPattern}

	return
}
