package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/minio/minio-go"

	"github.com/Azure/azure-storage-azcopy/common"
)

type s3Traverser struct {
	rawURL    *url.URL // No pipeline needed for S3
	ctx       context.Context
	recursive bool

	s3URLParts s3URLPartsExtension
	s3Client   *minio.Client

	// A generic function to notify that a new stored object has been enumerated
	incrementEnumerationCounter func()
}

func (t *s3Traverser) traverse(processor objectProcessor, filters []objectFilter) (err error) {
	// Check if resource is a single object.
	if t.s3URLParts.IsObjectSyntactically() && !t.s3URLParts.IsDirectorySyntactically() && !t.s3URLParts.IsBucketSyntactically() {
		objectPath := strings.Split(t.s3URLParts.ObjectKey, "/")
		objectName := objectPath[len(objectPath)-1]

		oi, err := t.s3Client.StatObject(t.s3URLParts.BucketName, t.s3URLParts.ObjectKey, minio.StatObjectOptions{})

		if err != nil {
			return err
		}

		err = processIfPassedFilters(filters, newStoredObject(
			objectName,
			"", // No need for relative path when we already know the object location
			oi.LastModified,
			oi.Size,
			nil,
			blobTypeNA,
		), processor)

		if err != nil {
			return err
		}

		return nil
	}

	// Check if the resource is a service level URL
	if t.s3URLParts.IsServiceSyntactically() {
		// TODO: Let's just figure this one out later.
		return errors.New("service-level enumeration is not allowed")
	}

	searchPrefix, _, wildcard := t.s3URLParts.searchObjectPrefixAndPatternFromS3URL()

	if wildcard {
		return fmt.Errorf("cannot traverse s3 with wildcard")
	}

	// It's a bucket or virtual directory.
	for objectInfo := range t.s3Client.ListObjectsV2(t.s3URLParts.BucketName, searchPrefix, t.recursive, t.ctx.Done()) {
		if objectInfo.Err != nil {
			return fmt.Errorf("cannot list objects, %v", objectInfo.Err)
		}

		if objectInfo.StorageClass == "" {
			// Directories are the only objects without storage classes.
			continue
		}

		objectPath := strings.Split(objectInfo.Key, "/")
		objectName := objectPath[len(objectPath)-1]

		relativePath := strings.TrimPrefix(objectInfo.Key, searchPrefix)

		err = processIfPassedFilters(filters,
			newStoredObject(
				objectName,
				relativePath,
				objectInfo.LastModified,
				objectInfo.Size,
				nil,
				blobTypeNA,
			),
			processor)

		if err != nil {
			return
		}
	}
	return
}

func newS3Traverser(rawURL *url.URL, ctx context.Context, recursive bool, incrementEnumerationCounter func()) (t *s3Traverser, err error) {
	t = &s3Traverser{rawURL: rawURL, ctx: ctx, recursive: recursive, incrementEnumerationCounter: incrementEnumerationCounter}

	// initialize S3 client and URL parts
	var s3URLParts common.S3URLParts
	s3URLParts, err = common.NewS3URLParts(*t.rawURL)

	if err != nil {
		return
	} else {
		t.s3URLParts = s3URLPartsExtension{s3URLParts}
	}

	t.s3Client, err = common.CreateS3Client(
		t.ctx,
		common.CredentialInfo{
			CredentialType: common.ECredentialType.S3AccessKey(),
			S3CredentialInfo: common.S3CredentialInfo{
				Endpoint: t.s3URLParts.Endpoint,
				Region:   t.s3URLParts.Region,
			},
		},
		common.CredentialOpOptions{
			LogError: glcm.Error,
		})

	return
}
