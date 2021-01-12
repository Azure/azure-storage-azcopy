package cmd

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"google.golang.org/api/iterator"
	"net/url"
	"strings"

	"github.com/Azure/azure-storage-azcopy/common"
)

type gcpTraverser struct {
	rawURL        *url.URL
	ctx           context.Context
	recursive     bool
	getProperties bool

	gcpURLParts common.GCPURLParts
	gcpClient   *storage.Client

	incrementEnumerationCounter enumerationCounterFunc
}

func (t *gcpTraverser) isDirectory(isSource bool) bool {
	//Identify whether directory or not syntactically
	isDirDirect := !t.gcpURLParts.IsObjectSyntactically() && (t.gcpURLParts.IsDirectorySyntactically() || t.gcpURLParts.IsBucketSyntactically())
	if !isSource {
		return isDirDirect
	}
	bkt := t.gcpClient.Bucket(t.gcpURLParts.BucketName)
	obj := bkt.Object(t.gcpURLParts.ObjectKey)
	//Directories do not have attributes and hence throw error
	_, err := obj.Attrs(t.ctx)
	if err == storage.ErrObjectNotExist {
		return true
	}
	return false
}

func (t *gcpTraverser) traverse(preprocessor objectMorpher, processor objectProcessor, filters []objectFilter) error {
	//Syntactically ensure whether single object or not
	if t.gcpURLParts.IsObjectSyntactically() && !t.gcpURLParts.IsDirectorySyntactically() && !t.gcpURLParts.IsBucketSyntactically() {
		objectPath := strings.Split(t.gcpURLParts.ObjectKey, "/")
		objectName := objectPath[len(objectPath)-1]

		attrs, err := t.gcpClient.Bucket(t.gcpURLParts.BucketName).Object(t.gcpURLParts.ObjectKey).Attrs(t.ctx)
		if err == nil {
			glcm.Info(fmt.Sprintf("Bucket: %v, Object: %v, Type: %v\n", attrs.Bucket, attrs.Name, attrs.ContentType))
			gie := common.GCPObjectInfoExtension{ObjectInfo: *attrs}
			storedObject := newStoredObject(
				preprocessor,
				objectName,
				"",
				common.EEntityType.File(),
				attrs.Updated,
				attrs.Size,
				&gie,
				noBlobProps,
				gie.NewCommonMetadata(),
				t.gcpURLParts.BucketName)
			err = processIfPassedFilters(filters, storedObject,
				processor)
			if err != nil {
				return err
			}
			return nil
		}
	}

	//Append trailing slash if missing
	if !strings.HasSuffix(t.gcpURLParts.ObjectKey, "/") && t.gcpURLParts.ObjectKey != "" {
		t.gcpURLParts.ObjectKey += "/"
	}
	searchPrefix := t.gcpURLParts.ObjectKey

	bkt := t.gcpClient.Bucket(t.gcpURLParts.BucketName)
	query := &storage.Query{Prefix: searchPrefix}
	it := bkt.Objects(t.ctx, query)

	//If code reaches here then the URL points to a bucket or a virtual directory
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			return nil
		}
		if err == nil {
			//Virtual directories alone have "/" as suffix and size as 0
			if strings.HasSuffix(attrs.Name, "/") {
				continue
			}
			objectPath := strings.Split(attrs.Name, "/")
			objectName := objectPath[len(objectPath)-1]

			relativePath := strings.TrimPrefix(attrs.Name, searchPrefix)

			oie := common.GCPObjectInfoExtension{ObjectInfo: storage.ObjectAttrs{}}

			if t.getProperties {
				oi, err := t.gcpClient.Bucket(t.gcpURLParts.BucketName).Object(attrs.Name).Attrs(t.ctx)
				if err != nil {
					return err
				}
				oie = common.GCPObjectInfoExtension{ObjectInfo: *oi}
			}

			storedObject := newStoredObject(
				preprocessor,
				objectName,
				relativePath,
				common.EEntityType.File(),
				attrs.Updated,
				attrs.Size,
				&oie,
				noBlobProps,
				oie.NewCommonMetadata(),
				t.gcpURLParts.BucketName)

			err = processIfPassedFilters(filters,
				storedObject,
				processor)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func newGCPTraverser(rawURL *url.URL, ctx context.Context, recursive, getProperties bool, incrementEnumerationCounter enumerationCounterFunc) (*gcpTraverser, error) {
	t := &gcpTraverser{
		rawURL:                      rawURL,
		ctx:                         ctx,
		recursive:                   recursive,
		getProperties:               getProperties,
		incrementEnumerationCounter: incrementEnumerationCounter,
	}
	gcpURLParts, err := common.NewGCPURLParts(*rawURL)
	if err != nil {
		return t, err
	} else {
		t.gcpURLParts = gcpURLParts
	}

	t.gcpClient, err = common.CreateGCPClient(t.ctx)

	return t, err
}
