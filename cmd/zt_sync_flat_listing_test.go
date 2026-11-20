//go:build smslidingwindow
// +build smslidingwindow

package cmd

import (
	"fmt"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

// TestFlatSyncRespectsStreamingFlagAndRejectsOtherPairs covers argument cooking; traverser tests cover orchestrator validation.
func TestFlatSyncRespectsStreamingFlagAndRejectsOtherPairs(t *testing.T) {
	for _, pair := range []common.FromTo{
		common.EFromTo.BlobBlob(), common.EFromTo.BlobBlobFS(), common.EFromTo.BlobFSBlob(),
		common.EFromTo.BlobFSBlobFS(), common.EFromTo.S3Blob(), common.EFromTo.FileFile(),
	} {
		for _, streaming := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/streaming=%t", pair, streaming), func(t *testing.T) {
				raw := getDefaultSyncRawInput("https://source.blob.core.windows.net/c", "https://target.blob.core.windows.net/c")
				raw.fromTo = pair.String()
				raw.recursive = false
				raw.useFlatListing = true
				raw.useStreamingMergeJoin = streaming
				cooked, err := raw.toCookedOptions()
				if pair == common.EFromTo.BlobBlob() {
					require.NoError(t, err)
					require.True(t, cooked.useFlatListing)
					require.True(t, cooked.useStreamingMergeJoin)
				} else {
					require.ErrorContains(t, err, "flat sync requires")
				}
			})
		}
	}
	for _, streaming := range []bool{false, true} {
		t.Run(fmt.Sprintf("non-flat/streaming=%t", streaming), func(t *testing.T) {
			raw := getDefaultSyncRawInput("https://source.blob.core.windows.net/c", "https://target.blob.core.windows.net/c")
			raw.fromTo = common.EFromTo.BlobBlob().String()
			raw.recursive = false
			raw.useFlatListing = false
			raw.useStreamingMergeJoin = streaming
			cooked, err := raw.toCookedOptions()
			require.NoError(t, err)
			require.False(t, cooked.useFlatListing)
			require.Equal(t, streaming, cooked.useStreamingMergeJoin)
		})
	}
}
