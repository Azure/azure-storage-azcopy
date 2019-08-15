package cmd

import (
	"context"
	"net/url"
	"path/filepath"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

type blobAccountTraverser struct {
	rawURL           *url.URL
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

			middlemanProcessor := func(object storedObject) error {
				tmpObject := object
				tmpObject.containerName = v.Name

				return processor(tmpObject)
			}

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

	t = &blobAccountTraverser{rawURL: rawURL, p: p, ctx: ctx, incrementEnumerationCounter: incrementEnumerationCounter, accountURL: azblob.NewServiceURL(bURLParts.URL(), p), containerPattern: cPattern}

	return
}
