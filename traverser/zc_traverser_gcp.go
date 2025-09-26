package traverser

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	gcpUtils "cloud.google.com/go/storage"
	"google.golang.org/api/iterator"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type gcpTraverser struct {
	rawURL        *url.URL
	ctx           context.Context
	recursive     bool
	getProperties bool

	gcpURLParts common.GCPURLParts
	gcpClient   *gcpUtils.Client

	incrementEnumerationCounter enumerationCounterFunc
}

func (t *gcpTraverser) IsDirectory(isSource bool) (bool, error) {
	//Identify whether directory or not syntactically
	isDirDirect := !t.gcpURLParts.IsObjectSyntactically() && (t.gcpURLParts.IsDirectorySyntactically() || t.gcpURLParts.IsBucketSyntactically())
	if !isSource {
		return isDirDirect, nil
	}
	bkt := t.gcpClient.Bucket(t.gcpURLParts.BucketName)
	obj := bkt.Object(t.gcpURLParts.ObjectKey)
	//Directories do not have attributes and hence throw error
	_, err := obj.Attrs(t.ctx)
	if err == gcpUtils.ErrObjectNotExist {
		return true, err
	}
	return false, nil
}

func (t *gcpTraverser) Traverse(preprocessor objectMorpher, processor ObjectProcessor, filters []ObjectFilter) error {
	p := processor
	processor = func(storedObject StoredObject) error {
		if t.incrementEnumerationCounter != nil {
			t.incrementEnumerationCounter(storedObject.EntityType)
		}

		return p(storedObject)
	}

	//Syntactically ensure whether single object or not
	if t.gcpURLParts.IsObjectSyntactically() && !t.gcpURLParts.IsDirectorySyntactically() && !t.gcpURLParts.IsBucketSyntactically() {
		objectPath := strings.Split(t.gcpURLParts.ObjectKey, "/")
		objectName := objectPath[len(objectPath)-1]

		attrs, err := t.gcpClient.Bucket(t.gcpURLParts.BucketName).Object(t.gcpURLParts.ObjectKey).Attrs(t.ctx)
		if err == nil {
			common.GetLifecycleMgr().Info(fmt.Sprintf("Bucket: %v, Object: %v, Type: %v\n", attrs.Bucket, attrs.Name, attrs.ContentType))
			gie := common.GCPObjectInfoExtension{ObjectInfo: *attrs}
			storedObject := NewStoredObject(
				preprocessor,
				objectName,
				"",
				common.EEntityType.File(),
				attrs.Updated,
				attrs.Size,
				&gie,
				NoBlobProps,
				gie.NewCommonMetadata(),
				t.gcpURLParts.BucketName)
			err = ProcessIfPassedFilters(filters, storedObject,
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
	query := &gcpUtils.Query{Prefix: searchPrefix}
	if !t.recursive {
		query.Delimiter = "/"
	}
	it := bkt.Objects(t.ctx, query)

	//If code reaches here then the URL points to a bucket or a virtual directory
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			return nil
		}
		if err == nil {
			//Virtual directories alone have "/" as suffix and size as 0
			if strings.HasSuffix(attrs.Name, "/") || attrs.Name == "" {
				continue
			}
			objectPath := strings.Split(attrs.Name, "/")
			objectName := objectPath[len(objectPath)-1]

			relativePath := strings.TrimPrefix(attrs.Name, searchPrefix)

			oie := common.GCPObjectInfoExtension{ObjectInfo: gcpUtils.ObjectAttrs{}}

			if t.getProperties {
				oi, err := t.gcpClient.Bucket(t.gcpURLParts.BucketName).Object(attrs.Name).Attrs(t.ctx)
				if err != nil {
					return err
				}
				oie = common.GCPObjectInfoExtension{ObjectInfo: *oi}
			}

			storedObject := NewStoredObject(
				preprocessor,
				objectName,
				relativePath,
				common.EEntityType.File(),
				attrs.Updated,
				attrs.Size,
				&oie,
				NoBlobProps,
				oie.NewCommonMetadata(),
				t.gcpURLParts.BucketName)

			err = ProcessIfPassedFilters(filters,
				storedObject,
				processor)
			_, err = GetProcessingError(err)
			if err != nil {
				return err
			}
		}
	}
}

func NewGCPTraverser(rawURL *url.URL, ctx context.Context, opts InitResourceTraverserOptions) (*gcpTraverser, error) {
	t := &gcpTraverser{
		rawURL:                      rawURL,
		ctx:                         ctx,
		recursive:                   opts.Recursive,
		getProperties:               opts.GetPropertiesInFrontend,
		incrementEnumerationCounter: opts.IncrementEnumeration,
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
