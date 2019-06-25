package cmd

import (
	"context"
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/azbfs"
	"net/url"
	"strings"
	"time"
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

func (t *blobFSTraverser) getPropertiesIfSingleFile() (*azbfs.PathGetPropertiesResponse, bool) {
	pathURL := azbfs.NewFileURL(*t.rawURL, t.p)
	pgr, err := pathURL.GetProperties(t.ctx)

	if err != nil {
		return nil, false
	}

	if pgr.XMsResourceType() == "directory" {
		return pgr, false
	}

	return pgr, true
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

	pathProperties, isFile := t.getPropertiesIfSingleFile()
	if isFile {
		storedObject := newStoredObject(
			getObjectNameOnly(bfsURLParts.DirectoryOrFilePath),
			"",
			t.parseLMT(pathProperties.LastModified()),
			pathProperties.ContentLength(),
			pathProperties.ContentMD5(),
			blobTypeNA,
		)

		if t.incrementEnumerationCounter != nil {
			t.incrementEnumerationCounter()
		}

		return processIfPassedFilters(filters, storedObject, processor)
	}

	dirUrl := azbfs.NewDirectoryURL(*t.rawURL, t.p)
	marker := ""
	searchPrefix := bfsURLParts.DirectoryOrFilePath

	if !strings.HasSuffix(searchPrefix, "/") {
		searchPrefix += "/"
	}

	for {
		dlr, err := dirUrl.ListDirectorySegment(t.ctx, &marker, t.recursive)

		if err != nil {
			return fmt.Errorf("could not list files. Failed with error %s", err.Error())
		}

		for _, v := range dlr.Paths {
			if v.IsDirectory == nil {
				// TODO: Index file.
				storedObject := newStoredObject(
					getObjectNameOnly(*v.Name),
					strings.TrimPrefix(*v.Name, searchPrefix),
					v.LastModifiedTime(),
					*v.ContentLength,
					v.ContentMD5(),
					blobTypeNA,
				)

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
