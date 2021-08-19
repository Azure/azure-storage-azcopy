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

func (t *gcpServiceTraverser) listContainers() ([]string, error) {

	if len(t.cachedBuckets) == 0 {
		bucketList := make([]string, 0)
		if projectID == "" {
			return nil, fmt.Errorf("ProjectID cannot be empty. Ensure that environment variable GOOGLE_CLOUD_PROJECT is not empty")
		}
		ctx := context.Background()
		it := t.gcpClient.Buckets(ctx, projectID)
		for {
			battrs, err := it.Next()
			if err == iterator.Done {
				break
			} else if err != nil {
				return nil, err
			}
			if t.bucketPattern != "" {
				if ok, err := containerNameMatchesPattern(battrs.Name, t.bucketPattern); err != nil {
					return nil, err
				} else if !ok {
					continue
				}
			}
			bucketList = append(bucketList, battrs.Name)
		}
		t.cachedBuckets = bucketList
		return bucketList, nil
	} else {
		return t.cachedBuckets, nil
	}
}

func (t *gcpServiceTraverser) Traverse(preprocessor objectMorpher, processor objectProcessor, filters []ObjectFilter) error {
	bucketList, err := t.listContainers()

	if err != nil {
		return err
	}

	for _, v := range bucketList {
		tmpGCPURL := t.gcpURL
		tmpGCPURL.BucketName = v
		urlResult := tmpGCPURL.URL()
		bucketTraverser, err := newGCPTraverser(&urlResult, t.ctx, true, t.getProperties, t.incrementEnumerationCounter)

		if err != nil {
			return err
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
	return nil
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
