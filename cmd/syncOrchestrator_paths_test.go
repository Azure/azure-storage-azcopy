//go:build smslidingwindow

// Copyright Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package cmd

import (
	"net/url"
	"path/filepath"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

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
				cca := cookedSyncCmdArgs{
					fromTo: fromTo,
					source: common.ResourceString{
						Value: "https://source.example/container/root%20%23%25",
						SAS:   "sig=source", ExtraQuery: "snapshot=source",
					},
					destination: common.ResourceString{
						Value: "https://target.example/container/root%20%23%25",
						SAS:   "sig=target", ExtraQuery: "snapshot=target",
					},
				}
				source, destination := cca.syncDirectoryResources(relativePath)
				for _, resource := range []common.ResourceString{source, destination} {
					u, err := url.Parse(resource.Value)
					require.NoError(t, err)
					require.Equal(t, "/container/root #%"+relativePath, u.Path)
					require.Empty(t, u.Fragment)
					require.Empty(t, u.RawQuery)
					require.Equal(t, "https", u.Scheme)
				}
				require.Equal(t, cca.source.SAS, source.SAS)
				require.Equal(t, cca.source.ExtraQuery, source.ExtraQuery)
				require.Equal(t, cca.destination.SAS, destination.SAS)
				require.Equal(t, cca.destination.ExtraQuery, destination.ExtraQuery)
				require.Equal(t, "https://source.example/container/root%20%23%25", cca.source.Value)
			})
		}
	}
}

func TestSyncDirectoryResourcesLocalSource(t *testing.T) {
	cca := cookedSyncCmdArgs{
		fromTo:      common.EFromTo.LocalBlob(),
		source:      common.ResourceString{Value: filepath.FromSlash("/source")},
		destination: common.ResourceString{Value: "https://target.example/container"},
	}
	source, destination := cca.syncDirectoryResources("/literal%25/#/space +/")
	require.Equal(t, filepath.FromSlash("/source/literal%25/#/space +/"), source.Value)
	u, err := url.Parse(destination.Value)
	require.NoError(t, err)
	require.Equal(t, "/container/literal%25/#/space +/", u.Path)
	require.Empty(t, u.Fragment)
}
