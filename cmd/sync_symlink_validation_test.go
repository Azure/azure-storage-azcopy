package cmd

import (
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

func TestSyncSymlinkFlagValidation(t *testing.T) {
	const container = "https://example.blob.core.windows.net/container"
	local := t.TempDir()

	for _, test := range []struct {
		name     string
		src, dst string
		fromTo   common.FromTo
		follow   bool
		preserve bool
		wantErr  string
	}{
		{"follow from local to blob", local, container, common.EFromTo.LocalBlob(), true, false, ""},
		{"follow from blob", container, local, common.EFromTo.BlobLocal(), true, false, "only applicable when uploading from the local file system"},
		{"preserve to blob", local, container, common.EFromTo.LocalBlob(), false, true, "only supported when syncing between local and Azure Files NFS, or between Azure Files NFS shares"},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw := getDefaultSyncRawInput(test.src, test.dst)
			raw.fromTo = test.fromTo.String()
			raw.followSymlinks, raw.preserveSymlinks = test.follow, test.preserve
			cooked, err := raw.toCookedOptions()
			require.NoError(t, err)

			err = cooked.validate()
			if test.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, test.wantErr)
			}
		})
	}
}
