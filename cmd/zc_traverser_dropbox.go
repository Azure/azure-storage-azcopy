package cmd

import (
	"context"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/dropbox/dropbox-sdk-go-unofficial/dropbox/files"
	"net/url"
	"strings"
	"time"
)

type dropboxTraverser struct {
	rawURL        *url.URL
	ctx           context.Context
	recursive     bool
	getProperties bool

	dropboxURLParts             common.DropboxURLParts
	client                      files.Client
	incrementEnumerationCounter enumerationCounterFunc
}

func (t *dropboxTraverser) isDirectory(isSource bool) bool {
	return t.dropboxURLParts.IsDir
}

func (t *dropboxTraverser) traverse(preprocessor objectMorpher, processor objectProcessor, filters []objectFilter) error {
	if !t.dropboxURLParts.IsDir {
		objectPath := strings.Split(t.dropboxURLParts.ObjectKey, "/")
		objectName := objectPath[len(objectPath)-1]

		dbx := t.client
		res, err := dbx.GetMetadata(files.NewGetMetadataArg("/" + t.dropboxURLParts.ObjectKey))
		if err != nil {
			return err
		}
		metadata, ok := res.(*files.FileMetadata)
		if !ok {
			return fmt.Errorf("Could not get FileMetadata please verify the source path or retry again")
		}
		var lmt time.Time
		if metadata.ClientModified.After(metadata.ServerModified) {
			lmt = metadata.ClientModified
		} else {
			lmt = metadata.ServerModified
		}
		oie := common.DropboxObjectInfoExtension{Metadata: *metadata}
		storedObject := newStoredObject(
			preprocessor,
			objectName,
			"",
			common.EEntityType.File(),
			lmt,
			int64(metadata.Size),
			&oie,
			noBlobProps,
			oie.NewCommonMetadata(),
			t.dropboxURLParts.BucketName)
		err = processIfPassedFilters(
			filters,
			storedObject,
			processor)
		if err != nil {
			return err
		}
		return nil
	}
	objects, err := t.ListObjects(t.dropboxURLParts.ObjectKey)
	if err != nil {
		return err
	}
	for i := 0; i < len(objects); i++ {
		relativePath := strings.TrimPrefix(objects[i].ObjectPath(), t.dropboxURLParts.ObjectKey+"/")
		storedObject := newStoredObject(
			preprocessor,
			objects[i].ObjectName(),
			relativePath,
			common.EEntityType.File(),
			objects[i].LMT(),
			objects[i].Size(),
			objects[i],
			noBlobProps,
			objects[i].NewCommonMetadata(),
			t.dropboxURLParts.BucketName)
		err := processIfPassedFilters(filters,
			storedObject,
			processor)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *dropboxTraverser) ListObjects(path string) ([]*common.DropboxObjectInfoExtension, error) {
	objectInfos := make([]*common.DropboxObjectInfoExtension, 0)
	arg := files.NewListFolderArg(common.IffString(path == "" || path == "/" || path == "/*", "", "/"+path))
	arg.Recursive = t.recursive
	arg.IncludeNonDownloadableFiles = false
	res, err := t.client.ListFolder(arg)
	if err != nil {
		return objectInfos, err
	}
	entries := res.Entries
	for res.HasMore {
		arg := files.NewListFolderContinueArg(res.Cursor)
		res, err = t.client.ListFolderContinue(arg)
		if err != nil {
			return objectInfos, err
		}
		entries = append(entries, res.Entries...)
	}

	for _, entry := range entries {
		f, ok := entry.(*files.FileMetadata)
		if ok {
			objectInfos = append(objectInfos, &common.DropboxObjectInfoExtension{Metadata: *f})
		}
	}
	return objectInfos, nil
}

func newDropboxTraverser(rawURL *url.URL, ctx context.Context, recursive, getProperties bool, incrementEnumerationCounter enumerationCounterFunc) (*dropboxTraverser, error) {
	t := &dropboxTraverser{rawURL: rawURL,
		ctx:                         ctx,
		recursive:                   recursive,
		getProperties:               getProperties,
		incrementEnumerationCounter: incrementEnumerationCounter,
	}
	dropboxURLParts, err := common.NewDropboxURLParts(*rawURL)
	if err != nil {
		return t, err
	} else {
		t.dropboxURLParts = dropboxURLParts
	}

	t.client, err = common.CreateDropboxClient()
	if err != nil {
		return t, err
	}

	return t, nil
}
