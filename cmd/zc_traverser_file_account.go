package cmd

import (
	"context"
	"net/url"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-file-go/azfile"
)

type fileAccountTraverser struct {
	rawURL     *url.URL
	accountURL azfile.ServiceURL
	p          pipeline.Pipeline
	ctx        context.Context

	// a generic function to notify that a new stored object has been enumerated
	incrementEnumerationCounter func()
}

func (t *fileAccountTraverser) traverse(processor objectProcessor, filters []objectFilter) error {
	marker := azfile.Marker{}
	for marker.NotDone() {
		resp, err := t.accountURL.ListSharesSegment(t.ctx, marker, azfile.ListSharesOptions{})

		if err != nil {
			return err
		}

		for _, v := range resp.ShareItems {
			shareURL := t.accountURL.NewShareURL(v.Name).URL()
			shareTraverser := newFileTraverser(&shareURL, t.p, t.ctx, true, t.incrementEnumerationCounter)

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

func newFileAccountTraverser(rawURL *url.URL, p pipeline.Pipeline, ctx context.Context, incrementEnumerationCounter func()) (t *fileAccountTraverser) {
	t = &fileAccountTraverser{rawURL: rawURL, p: p, ctx: ctx, incrementEnumerationCounter: incrementEnumerationCounter, accountURL: azfile.NewServiceURL(*rawURL, p)}

	return
}
