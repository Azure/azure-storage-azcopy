package azcopy

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
	"github.com/stretchr/testify/require"
)

const (
	flatBlobSource = "https://source.blob.core.windows.net/container?sv=2020-08-04&sig=source"
	flatBlobTarget = "https://target.blob.core.windows.net/container?sv=2020-08-04&sig=target"
	flatFileTarget = "https://target.file.core.windows.net/share?sv=2020-08-04&sig=target"
)

func flatCopyOptions() CopyOptions {
	return CopyOptions{
		FromTo:         common.EFromTo.BlobBlob(),
		Recursive:      true,
		UseFlatListing: true,
		ErrorChannel:   make(chan traverser.TraverserErrorItemInfo),
	}
}

func TestFlatListingCopyAcceptsRecursiveBlobToBlob(t *testing.T) {
	cooked, err := newCookedCopyOptions(flatBlobSource, flatBlobTarget, flatCopyOptions())
	require.NoError(t, err)
	require.True(t, cooked.useFlatListing)
	require.False(t, cooked.includeDirectoryStubs)
}

func TestFlatListingCopyRejectsIneligibleTransfers(t *testing.T) {
	for _, test := range []struct {
		name   string
		target string
		adjust func(*CopyOptions)
	}{
		{"not recursive", flatBlobTarget, func(o *CopyOptions) { o.Recursive = false }},
		{"directory stubs", flatBlobTarget, func(o *CopyOptions) { o.IncludeDirectoryStubs = true }},
		{"not blob to blob", flatFileTarget, func(o *CopyOptions) { o.FromTo = common.EFromTo.BlobFile() }},
	} {
		t.Run(test.name, func(t *testing.T) {
			opts := flatCopyOptions()
			test.adjust(&opts)
			_, err := newCookedCopyOptions(flatBlobSource, test.target, opts)
			require.ErrorContains(t, err, "flat listing requires a recursive Blob-to-Blob copy")
		})
	}
}

func TestOrdinaryCopyIgnoresFlatListingRules(t *testing.T) {
	opts := flatCopyOptions()
	opts.UseFlatListing = false
	opts.Recursive = false
	cooked, err := newCookedCopyOptions(flatBlobSource, flatBlobTarget, opts)
	require.NoError(t, err)
	require.False(t, cooked.useFlatListing)
}

func flatSyncOptions() SyncOptions {
	return SyncOptions{
		FromTo:                common.EFromTo.BlobBlob(),
		UseFlatListing:        true,
		UseSyncOrchestrator:   true,
		UseStreamingMergeJoin: true,
		Recursive:             to.Ptr(false),
		ErrorChannel:          make(chan traverser.TraverserErrorItemInfo),
	}
}

func TestFlatListingSyncRequiresTheOrchestratorBuild(t *testing.T) {
	cooked, err := newCookedSyncOptions(flatBlobSource, flatBlobTarget, flatSyncOptions())
	if !traverser.UseSyncOrchestrator {
		require.ErrorContains(t, err, "smslidingwindow")
		return
	}
	require.NoError(t, err)
	require.True(t, cooked.useFlatListing)
}

func TestFlatListingSyncRejectsIneligibleTransfers(t *testing.T) {
	for _, test := range []struct {
		name   string
		target string
		// The orchestrator-build check runs first, so these cases can only reach the flat check with the tag.
		needsOrchestratorBuild bool
		adjust                 func(*SyncOptions)
	}{
		{"not blob to blob", flatFileTarget, true, func(o *SyncOptions) { o.FromTo = common.EFromTo.BlobFile() }},
		{"no orchestrator", flatBlobTarget, false, func(o *SyncOptions) { o.UseSyncOrchestrator = false }},
		{"no streaming merge-join", flatBlobTarget, true, func(o *SyncOptions) { o.UseStreamingMergeJoin = false }},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.needsOrchestratorBuild && !traverser.UseSyncOrchestrator {
				t.Skip("requires the smslidingwindow build tag")
			}
			opts := flatSyncOptions()
			test.adjust(&opts)
			_, err := newCookedSyncOptions(flatBlobSource, test.target, opts)
			require.ErrorContains(t, err, "flat sync requires")
		})
	}
}
