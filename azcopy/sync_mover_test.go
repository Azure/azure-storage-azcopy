package azcopy

import (
	"context"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
	"github.com/stretchr/testify/require"
)

func TestM3SyncCopiesOrchestratorConfiguration(t *testing.T) {
	settings := traverser.NewSyncOrchestratorOptions(100, true, time.Time{}, false, 3)
	before := settings
	first := &cookedSyncOptions{fromTo: common.EFromTo.LocalFileNFS()}
	second := &cookedSyncOptions{fromTo: common.EFromTo.FileFile()}
	options := SyncOptions{
		UseSyncOrchestrator:   true,
		UseStreamingMergeJoin: true,
		OrchestratorOptions:   &settings,
		Recursive:             to.Ptr(false),
	}
	require.NoError(t, first.applyDefaultsAndInferOptions(options))
	require.NoError(t, second.applyDefaultsAndInferOptions(options))
	require.Equal(t, before, settings)
	require.NotSame(t, first.orchestratorOptions, second.orchestratorOptions)
	require.NotEqual(t, *first.orchestratorOptions, *second.orchestratorOptions)
	require.True(t, first.useStreamingMergeJoin)
	require.False(t, first.recursive)
}

func TestM3SyncUsesInferredProtocolForDefaults(t *testing.T) {
	options := SyncOptions{PreserveInfo: to.Ptr(true)}
	cooked := &cookedSyncOptions{fromTo: common.EFromTo.FileNFSFileNFS()}
	require.NoError(t, cooked.applyDefaultsAndInferOptions(options))
	require.True(t, cooked.preserveInfo, "the inferred direction, not an omitted user override, determines NFS defaults")
}

func TestM3SyncRejectsMissingContext(t *testing.T) {
	client := &Client{}
	_, err := client.Sync(nil, "source", "destination", SyncOptions{})
	require.ErrorContains(t, err, "context")
	_, err = client.PrepareSync(nil, "source", "destination", SyncOptions{})
	require.ErrorContains(t, err, "context")
	_, err = client.PrepareSync(context.Background(), "", "", SyncOptions{})
	require.ErrorContains(t, err, "source and destination")
}

func TestM3SyncRejectsMissingDryRunCallbacks(t *testing.T) {
	cooked := &cookedSyncOptions{dryrun: true}
	require.ErrorContains(t, cooked.validateOptions(), "transfer handler")
	cooked.dryrunJobPartOrderHandler = func(common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		return common.CopyJobPartOrderResponse{}
	}
	cooked.deleteDestination = common.EDeleteDestination.True()
	require.ErrorContains(t, cooked.validateOptions(), "delete handler")
}
