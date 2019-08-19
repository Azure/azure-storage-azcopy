// Copyright © 2019 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cmd

import (
	"context"
	"net/url"
	"path/filepath"

	"github.com/minio/minio-go"

	"github.com/Azure/azure-storage-azcopy/common"
)

// As we discussed, the general architecture is that this is going to search a list of buckets and spawn s3Traversers for each bucket.
// This will modify the storedObject format a slight bit to add a "container" parameter.

type s3ServiceTraverser struct {
	ctx           context.Context
	bucketPattern string

	s3URL    s3URLPartsExtension
	s3Client *minio.Client

	// a generic function to notify that a new stored object has been enumerated
	incrementEnumerationCounter func()
}

func (t *s3ServiceTraverser) traverse(processor objectProcessor, filters []objectFilter) error {
	if bucketInfo, err := t.s3Client.ListBuckets(); err == nil {
		for _, v := range bucketInfo {
			// Match a pattern for the bucket name and the bucket name only
			if t.bucketPattern != "" {
				if ok, err := filepath.Match(t.bucketPattern, v.Name); err != nil {
					// Break if the pattern is invalid
					return err
				} else if !ok {
					// Ignore the bucket if it does not match the pattern
					continue
				}
			}

			tmpS3URL := t.s3URL
			tmpS3URL.BucketName = v.Name
			urlResult := tmpS3URL.URL()
			bucketTraverser, err := newS3Traverser(&urlResult, t.ctx, true, t.incrementEnumerationCounter)

			if err != nil {
				return err
			}

			middlemanProcessor := initAccountMiddlemanProcessor(v.Name, processor)

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
	t = &s3ServiceTraverser{ctx: ctx, incrementEnumerationCounter: incrementEnumerationCounter}

	var s3URLParts common.S3URLParts
	s3URLParts, err = common.NewS3URLParts(*rawURL)

	if err != nil {
		return
	} else if !s3URLParts.IsServiceSyntactically() {
		// Yoink the bucket name off and treat it as the pattern.
		t.bucketPattern = s3URLParts.BucketName

		s3URLParts.BucketName = ""
	}

	t.s3URL = s3URLPartsExtension{s3URLParts}

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
