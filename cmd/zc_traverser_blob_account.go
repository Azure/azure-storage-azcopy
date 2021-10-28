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
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"net/url"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

// Enumerates an entire blob account, looking into each matching container as it goes
type blobAccountTraverser struct {
	accountURL            azblob.ServiceURL
	p                     pipeline.Pipeline
	ctx                   context.Context
	containerPattern      string
	cachedContainers      []string
	includeDirectoryStubs bool

	// a generic function to notify that a new stored object has been enumerated
	incrementEnumerationCounter enumerationCounterFunc

	s2sPreserveSourceTags bool

	cpkOptions common.CpkOptions
}

func (t *blobAccountTraverser) IsDirectory(_ bool) bool {
	return true // Returns true as account traversal is inherently folder-oriented and recursive.
}

func (t *blobAccountTraverser) listContainers() ([]string, error) {
	// a nil list also returns 0
	if len(t.cachedContainers) == 0 {
		marker := azblob.Marker{}
		cList := make([]string, 0)

		for marker.NotDone() {
			resp, err := t.accountURL.ListContainersSegment(t.ctx, marker, azblob.ListContainersSegmentOptions{})

			if err != nil {
				return nil, err
			}

			for _, v := range resp.ContainerItems {
				// Match a pattern for the container name and the container name only.
				if t.containerPattern != "" {
					if ok, err := containerNameMatchesPattern(v.Name, t.containerPattern); err != nil {
						// Break if the pattern is invalid
						return nil, err
					} else if !ok {
						// Ignore the container if it doesn't match the pattern.
						continue
					}
				}

				cList = append(cList, v.Name)
			}

			marker = resp.NextMarker
		}

		t.cachedContainers = cList

		return cList, nil
	} else {
		return t.cachedContainers, nil
	}
}

func (t *blobAccountTraverser) Traverse(preprocessor objectMorpher, processor objectProcessor, filters []ObjectFilter) error {
	// listContainers will return the cached container list if containers have already been listed by this traverser.
	cList, err := t.listContainers()

	if err != nil {
		return err
	}

	for _, v := range cList {
		containerURL := t.accountURL.NewContainerURL(v).URL()
		containerTraverser := newBlobTraverser(&containerURL, t.p, t.ctx, true,
			t.includeDirectoryStubs, t.incrementEnumerationCounter, t.s2sPreserveSourceTags, t.cpkOptions)

		preprocessorForThisChild := preprocessor.FollowedBy(newContainerDecorator(v))

		err = containerTraverser.Traverse(preprocessorForThisChild, processor, filters)

		if err != nil {
			WarnStdoutAndScanningLog(fmt.Sprintf("failed to list blobs in container %s: %s", v, err))
			continue
		}
	}

	return nil
}

func newBlobAccountTraverser(rawURL *url.URL, p pipeline.Pipeline, ctx context.Context,
	includeDirectoryStubs bool, incrementEnumerationCounter enumerationCounterFunc,
	s2sPreserveSourceTags bool, cpkOptions common.CpkOptions) (t *blobAccountTraverser) {
	bURLParts := azblob.NewBlobURLParts(*rawURL)
	cPattern := bURLParts.ContainerName

	// Strip the container name away and treat it as a pattern
	if bURLParts.ContainerName != "" {
		bURLParts.ContainerName = ""
	}

	t = &blobAccountTraverser{
		p:                           p,
		ctx:                         ctx,
		incrementEnumerationCounter: incrementEnumerationCounter,
		accountURL:                  azblob.NewServiceURL(bURLParts.URL(), p),
		containerPattern:            cPattern,
		includeDirectoryStubs:       includeDirectoryStubs,
		s2sPreserveSourceTags:       s2sPreserveSourceTags,
		cpkOptions:                  cpkOptions,
	}

	return
}
