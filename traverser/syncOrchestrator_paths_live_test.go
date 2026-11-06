//go:build smslidingwindow

// Copyright Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package traverser_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/parallel"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
	"github.com/stretchr/testify/require"
)

// This opt-in test only lists existing data. Authenticate with az login and set
// AZCOPY_TEST_SYNC_SOURCE_URL to a container or directory containing unusual names.
func TestSyncDirectoryResourcesLive(t *testing.T) {
	sourceURL := os.Getenv("AZCOPY_TEST_SYNC_SOURCE_URL")
	if sourceURL == "" {
		t.Skip("set AZCOPY_TEST_SYNC_SOURCE_URL to run read-only live traversal")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	parts, err := blob.ParseURL(sourceURL)
	require.NoError(t, err)
	require.NotEmpty(t, parts.ContainerName)
	prefix := strings.Trim(parts.BlobName, "/")
	if prefix != "" {
		prefix += "/"
	}
	containerName := parts.ContainerName
	parts.ContainerName, parts.BlobName = "", ""
	credential, err := azidentity.NewAzureCLICredential(nil)
	require.NoError(t, err)
	client, err := service.NewClient(parts.String(), credential, nil)
	require.NoError(t, err)

	expected := make(map[string]int64)
	directories := map[string]bool{"/": true}
	pager := client.NewContainerClient(containerName).NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix:  &prefix,
		Include: container.ListBlobsInclude{Metadata: true},
	})
	for pager.More() {
		page, err := pager.NextPage(ctx)
		require.NoError(t, err)
		for _, item := range page.Segment.BlobItems {
			isFolder := false
			for key, value := range item.Metadata {
				if strings.EqualFold(key, "hdi_isfolder") && value != nil && strings.EqualFold(*value, "true") {
					isFolder = true
				}
			}
			if isFolder {
				continue
			}
			name := strings.TrimPrefix(*item.Name, prefix)
			expected[name] = *item.Properties.ContentLength
			for i, char := range name {
				if char == '/' {
					directories["/"+name[:i+1]] = true
				}
			}
		}
	}
	require.NotEmpty(t, expected)

	syncJob := newSyncPathsJob(common.EFromTo.BlobBlob(),
		common.ResourceString{Value: strings.TrimSuffix(sourceURL, "/")},
		common.ResourceString{})
	actual := make(map[string]int64)
	visited := make(map[string]bool)
	var visitedMu sync.Mutex
	results := parallel.Crawl(ctx, "/", func(directory parallel.Directory, enqueueDir func(parallel.Directory), enqueueOutput func(parallel.DirectoryEntry, error)) error {
		dir := directory.(string)
		visitedMu.Lock()
		duplicate := visited[dir]
		visited[dir] = true
		visitedMu.Unlock()
		if duplicate || !directories[dir] {
			return fmt.Errorf("duplicate or phantom directory %q", dir)
		}
		source, _ := traverser.SyncDirectoryResources(syncJob, dir)
		blobTraverser := traverser.NewBlobTraverser(source.Value, client, ctx, traverser.InitResourceTraverserOptions{
			IncrementEnumeration: func(common.EntityType, common.SymlinkHandlingType, common.HardlinkHandlingType) {},
		})
		return blobTraverser.Traverse(traverser.NoPreProccessor, func(object traverser.StoredObject) error {
			if object.EntityType == common.EEntityType.Folder() {
				enqueueDir(dir + object.RelativePath + "/")
				return nil
			}
			object.RelativePath = strings.TrimPrefix(dir+object.RelativePath, "/")
			enqueueOutput(object, nil)
			return nil
		}, nil)
	}, 16)
	for result := range results {
		item, err := result.Item()
		require.NoError(t, err)
		object := item.(traverser.StoredObject)
		_, duplicate := actual[object.RelativePath]
		require.False(t, duplicate, "duplicate file %q", object.RelativePath)
		actual[object.RelativePath] = object.Size
	}
	require.NoError(t, ctx.Err())
	require.Equal(t, expected, actual)
	var bytes int64
	for _, size := range actual {
		bytes += size
	}
	t.Logf("Matched flat listing: %d files, %d directories, %d bytes; no duplicates or phantom paths",
		len(actual), len(visited), bytes)
}
