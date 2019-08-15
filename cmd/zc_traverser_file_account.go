package cmd

import (
	"context"
	"net/url"
	"path/filepath"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-file-go/azfile"
)

type fileAccountTraverser struct {
	rawURL       *url.URL
	accountURL   azfile.ServiceURL
	p            pipeline.Pipeline
	ctx          context.Context
	sharePattern string

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
			// Match a pattern for the share name and the share name only
			if t.sharePattern != "" {
				if ok, err := filepath.Match(t.sharePattern, v.Name); err != nil {
					// Break if the pattern is invalid
					return err
				} else if !ok {
					// Ignore the share if it doesn't match the pattern.
					continue
				}
			}

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
	fURLparts := azfile.NewFileURLParts(*rawURL)
	sPattern := fURLparts.ShareName

	if fURLparts.ShareName != "" {
		fURLparts.ShareName = ""
	}

	t = &fileAccountTraverser{rawURL: rawURL, p: p, ctx: ctx, incrementEnumerationCounter: incrementEnumerationCounter, accountURL: azfile.NewServiceURL(fURLparts.URL(), p), sharePattern: sPattern}
	return
}
