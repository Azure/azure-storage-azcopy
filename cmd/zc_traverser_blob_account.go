package cmd

import (
	"context"
	"net/url"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

type blobAccountTraverser struct {
	rawURL     *url.URL
	accountURL azblob.ServiceURL
	p          pipeline.Pipeline
	ctx        context.Context

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
			shareURL := t.accountURL.NewContainerURL(v.Name).URL()
			shareTraverser := newBlobTraverser(&shareURL, t.p, t.ctx, true, t.incrementEnumerationCounter)

			middlemanProcessor := func(object storedObject) error {
				tmpObject := object
				tmpObject.containerName = v.Name

				return processor(tmpObject)
			}

			err = shareTraverser.traverse(middlemanProcessor, filters)

			if err != nil {
				return err
			}
		}

		marker = resp.NextMarker
	}

	return nil
}

func newBlobAccountTraverser(rawURL *url.URL, p pipeline.Pipeline, ctx context.Context, incrementEnumerationCounter func()) (t *blobAccountTraverser) {
	t = &blobAccountTraverser{rawURL: rawURL, p: p, ctx: ctx, incrementEnumerationCounter: incrementEnumerationCounter, accountURL: azblob.NewServiceURL(*rawURL, p)}

	return
}
