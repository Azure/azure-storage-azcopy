package cmd

import (
	"context"
	"fmt"
	"runtime"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type SyncEnumeratorOptions struct {
	ErrorChannel    chan TraverserErrorItemInfo
	SyncOrchOptions *SyncOrchestratorOptions
}

func (options *SyncEnumeratorOptions) forJob(fromTo common.FromTo) *SyncEnumeratorOptions {
	if options == nil {
		options = NewSyncDefaultEnumeratorOptions()
	}
	owned := *options
	if options.SyncOrchOptions != nil {
		settings := *options.SyncOrchOptions
		settings.SetFromTo(fromTo)
		owned.SyncOrchOptions = &settings
	}
	return &owned
}

func NewSyncDefaultEnumeratorOptions() *SyncEnumeratorOptions {
	options := NewDefaultSyncOrchestratorOptions()
	if common.IsSyncOrchTestModeSet() {
		options = NewTestSyncOrchestratorOptions()
	}
	return &SyncEnumeratorOptions{SyncOrchOptions: &options}
}

func NewSyncEnumeratorOptions(errorChannelSize int, options *SyncOrchestratorOptions) *SyncEnumeratorOptions {
	return &SyncEnumeratorOptions{
		ErrorChannel:    make(chan TraverserErrorItemInfo, errorChannelSize),
		SyncOrchOptions: options,
	}
}

func (cca *cookedSyncCmdArgs) InitEnumerator(ctx context.Context, options *SyncEnumeratorOptions) (*syncEnumerator, error) {
	if ctx == nil {
		return nil, fmt.Errorf("a context is required for sync")
	}
	source, destination, err := cca.syncResourceStrings()
	if err != nil {
		return nil, err
	}
	prepared, err := Client.PrepareSync(ctx, source, destination, cca.librarySyncOptions(options))
	if err != nil {
		return nil, err
	}
	cca.preparedSync = prepared
	cca.orchestratorCancel = prepared.Cancel
	cca.jobID = prepared.State().JobID
	cca.refreshPreparedStats()
	return prepared.Enumerator(), nil
}

func IsDestinationCaseInsensitive(fromTo common.FromTo) bool {
	return fromTo.IsDownload() && runtime.GOOS == "windows"
}
