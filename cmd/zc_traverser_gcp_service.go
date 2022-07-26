package cmd

import (
	gcpUtils "cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"google.golang.org/api/iterator"
	"net/url"
	"strings"
)

type gcpServiceTraverser struct {
	ctx           context.Context
	bucketPattern string
	cachedBuckets []string
	getProperties bool

	gcpURL    common.GCPURLParts
	gcpClient *gcpUtils.Client

	incrementEnumerationCounter enumerationCounterFunc
}

var projectID = ""

func (t *gcpServiceTraverser) IsDirectory(isSource bool) bool {
	return true //Account traversals are inherently folder based
}

func (t *gcpServiceTraverser) listContainers() (chan string, chan error) {
	buckets := make(chan string, 100) //S3 supports a max of 100 buckets per account
	e := make(chan error)

	go func() {
		var err error = nil
		defer func() {
			close(buckets)
			e <- err
			close(e)
		}()

		if len(t.cachedBuckets) != 0 {
			for _, v := range t.cachedBuckets {
				buckets <- v
			}
			return
		}

		bucketList := make([]string, 0)
		if projectID == "" {
			err = fmt.Errorf("ProjectID cannot be empty. Ensure that environment variable GOOGLE_CLOUD_PROJECT is not empty")
			return
		}
		ctx := context.Background()
		it := t.gcpClient.Buckets(ctx, projectID)
		for {
			battrs, err := it.Next()
			if err == iterator.Done {
				break
			} else if err != nil {
				return
			}
			if t.bucketPattern != "" {
				if ok, err := containerNameMatchesPattern(battrs.Name, t.bucketPattern); err != nil {
					return
				} else if !ok {
					continue
				}
			}

			buckets <- battrs.Name
			bucketList = append(bucketList, battrs.Name)
		}
		t.cachedBuckets = bucketList
		return
	}()

	return buckets, e
}

func (t *gcpServiceTraverser) Traverse(preprocessor objectMorpher, processor objectProcessor, filters []ObjectFilter) error {
	bucketList, errChan := t.listContainers()

	for v := range bucketList {
		tmpGCPURL := t.gcpURL
		tmpGCPURL.BucketName = v
		urlResult := tmpGCPURL.URL()
		bucketTraverser, err := newGCPTraverser(&urlResult, t.ctx, true, t.getProperties, t.incrementEnumerationCounter)

		if err != nil {
			WarnStdoutAndScanningLog(fmt.Sprintf("skip enumerating the bucket %q, could not enumerate: %s", v, err.Error()))
			continue
		}
		preprocessorForThisChild := preprocessor.FollowedBy(newContainerDecorator(v))

		err = bucketTraverser.Traverse(preprocessorForThisChild, processor, filters)

		if err != nil {
			if strings.Contains(err.Error(), "cannot list objects, The specified bucket does not exist") {
				WarnStdoutAndScanningLog(fmt.Sprintf("skip enumerating the bucket %q, as it does not exist.", v))
			}

			WarnStdoutAndScanningLog(fmt.Sprintf("failed to list objects in bucket %s: %s", v, err))
			continue
		}
	}
	return <-errChan
}

func newGCPServiceTraverser(rawURL *url.URL, ctx context.Context, getProperties bool, incrementEnumerationCounter enumerationCounterFunc) (*gcpServiceTraverser, error) {
	projectID = glcm.GetEnvironmentVariable(common.EEnvironmentVariable.GoogleCloudProject())
	t := &gcpServiceTraverser{
		ctx:                         ctx,
		incrementEnumerationCounter: incrementEnumerationCounter,
		getProperties:               getProperties,
	}
	gcpURLParts, err := common.NewGCPURLParts(*rawURL)

	if err != nil {
		return t, err
	} else if !gcpURLParts.IsServiceSyntactically() {
		t.bucketPattern = gcpURLParts.BucketName
		gcpURLParts.BucketName = ""
	}

	t.gcpURL = gcpURLParts
	t.gcpClient, err = common.CreateGCPClient(t.ctx)
	return t, nil
}
