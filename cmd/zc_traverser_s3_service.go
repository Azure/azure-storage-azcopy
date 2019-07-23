package cmd

import (
	"context"
	"errors"
	"net/url"

	"github.com/minio/minio-go"

	"github.com/Azure/azure-storage-azcopy/common"
)

// As we discussed, the general architecture is that this is going to search a list of buckets and spawn s3Traversers for each bucket.
// This will modify the storedObject format a slight bit to add a "container" parameter.

type s3ServiceTraverser struct {
	rawURL *url.URL // No pipeline needed for S3
	ctx    context.Context

	s3URL    s3URLPartsExtension
	s3Client *minio.Client

	// a generic function to notify that a new stored object has been enumerated
	incrementEnumerationCounter func()
}

func (t *s3ServiceTraverser) traverse(processor objectProcessor, filters []objectFilter) error {
	if bucketInfo, err := t.s3Client.ListBuckets(); err == nil {
		for _, v := range bucketInfo {
			tmpS3URL := t.s3URL
			tmpS3URL.BucketName = v.Name
			urlResult := tmpS3URL.URL()
			bucketTraverser, err := newS3Traverser(&urlResult, t.ctx, true, t.incrementEnumerationCounter)

			if err != nil {
				return err
			}

			middlemanProcessor := func(object storedObject) error {
				tmpObject := object
				tmpObject.containerName = v.Name

				return processor(tmpObject)
			}

			err = bucketTraverser.traverse(middlemanProcessor, filters)

			if err != nil {
				return err
			}
		}
	} else {
		return err
	}

	return nil
}

func newS3ServiceTraverser(rawURL *url.URL, ctx context.Context, incrementEnumerationCounter func()) (t *s3ServiceTraverser, err error) {
	t = &s3ServiceTraverser{rawURL: rawURL, ctx: ctx, incrementEnumerationCounter: incrementEnumerationCounter}

	var s3URLParts common.S3URLParts
	s3URLParts, err = common.NewS3URLParts(*t.rawURL)

	if err != nil {
		return
	} else if !s3URLParts.IsServiceSyntactically() {
		return nil, errors.New("rawURL supplied to S3ServiceTraverser should be service URL")
	} else {
		t.s3URL = s3URLPartsExtension{s3URLParts}
	}

	t.s3Client, err = common.CreateS3Client(
		t.ctx,
		common.CredentialInfo{
			CredentialType: common.ECredentialType.S3AccessKey(),
			S3CredentialInfo: common.S3CredentialInfo{
				Endpoint: t.s3URL.Endpoint,
				Region:   t.s3URL.Region,
			},
		},
		common.CredentialOpOptions{
			LogError: glcm.Error,
		})

	return
}
