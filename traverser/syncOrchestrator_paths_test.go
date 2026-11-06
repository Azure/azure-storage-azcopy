//go:build smslidingwindow

// Copyright Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package traverser_test

import (
	"net/url"
	"path/filepath"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
	"github.com/stretchr/testify/require"
)

// newSyncPathsJob wires the path-encoding hooks the way azcopy does in production.
func newSyncPathsJob(fromTo common.FromTo, source, destination common.ResourceString) *traverser.SyncJob {
	return &traverser.SyncJob{
		FromTo:                fromTo,
		Source:                source,
		Destination:           destination,
		EncodeSourcePath:      func(path string) string { return azcopy.PathEncodeRules(path, fromTo, false, true) },
		EncodeDestinationPath: func(path string) string { return azcopy.PathEncodeRules(path, fromTo, false, false) },
	}
}

func TestSyncDirectoryResourcesRemote(t *testing.T) {
	paths := []string{
		"/",
		"/ordinary/nested/",
		"/%/",
		"/#/",
		"/123456789:;<=>?@/",
		"/literal%2F/literal%23/",
		"/space + plus/&=/",
		"/\u4e2d\u6587/\U0001f600/",
		"/double//slash/",
		"/back\\slash/",
	}
	for _, fromTo := range []common.FromTo{
		common.EFromTo.BlobBlob(),
		common.EFromTo.BlobFSBlob(),
		common.EFromTo.BlobBlobFS(),
		common.EFromTo.BlobFSBlobFS(),
		common.EFromTo.S3Blob(),
	} {
		for _, relativePath := range paths {
			t.Run(fromTo.String()+relativePath, func(t *testing.T) {
				syncJob := newSyncPathsJob(fromTo,
					common.ResourceString{
						Value: "https://source.example/container/root%20%23%25",
						SAS:   "sig=source", ExtraQuery: "snapshot=source",
					},
					common.ResourceString{
						Value: "https://target.example/container/root%20%23%25",
						SAS:   "sig=target", ExtraQuery: "snapshot=target",
					})
				source, destination := traverser.SyncDirectoryResources(syncJob, relativePath)
				for _, resource := range []common.ResourceString{source, destination} {
					u, err := url.Parse(resource.Value)
					require.NoError(t, err)
					require.Equal(t, "/container/root #%"+relativePath, u.Path)
					require.Empty(t, u.Fragment)
					require.Empty(t, u.RawQuery)
					require.Equal(t, "https", u.Scheme)
				}
				require.Equal(t, syncJob.Source.SAS, source.SAS)
				require.Equal(t, syncJob.Source.ExtraQuery, source.ExtraQuery)
				require.Equal(t, syncJob.Destination.SAS, destination.SAS)
				require.Equal(t, syncJob.Destination.ExtraQuery, destination.ExtraQuery)
				require.Equal(t, "https://source.example/container/root%20%23%25", syncJob.Source.Value)
			})
		}
	}
}

func TestSyncDirectoryResourcesLocalSource(t *testing.T) {
	syncJob := newSyncPathsJob(common.EFromTo.LocalBlob(),
		common.ResourceString{Value: filepath.FromSlash("/source")},
		common.ResourceString{Value: "https://target.example/container"})
	source, destination := traverser.SyncDirectoryResources(syncJob, "/literal%25/#/space +/")
	require.Equal(t, filepath.FromSlash("/source/literal%25/#/space +/"), source.Value)
	u, err := url.Parse(destination.Value)
	require.NoError(t, err)
	require.Equal(t, "/container/literal%25/#/space +/", u.Path)
	require.Empty(t, u.Fragment)
}
