package cmd

import (
	"errors"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// The final part must not be dispatched when the caller's BeforeFinalPart
// hook fails, for either comparator layout.
func TestSyncFinalizeStopsWhenBeforeFinalPartFails(t *testing.T) {
	orchOptions := NewDefaultSyncOrchestratorOptions()
	cases := []struct {
		name  string
		build func(*cookedSyncCmdArgs, *SyncEnumeratorOptions) (*syncEnumerator, error)
		cca   *cookedSyncCmdArgs
	}{
		{
			name: "destination comparator",
			build: func(cca *cookedSyncCmdArgs, o *SyncEnumeratorOptions) (*syncEnumerator, error) {
				return GetSyncEnumeratorWithDestComparator(cca, common.EFolderPropertiesOption.NoFolders(),
					&common.CopyJobPartOrderRequest{}, nil, nil, newObjectIndexer(), nil, &copyTransferProcessor{},
					ResourceTraverserTemplate{}, ResourceTraverserTemplate{}, o)
			},
			cca: &cookedSyncCmdArgs{fromTo: common.EFromTo.BlobBlob(),
				destination: common.ResourceString{Value: "https://account.blob.core.windows.net/container"}},
		},
		{
			name: "source comparator",
			build: func(cca *cookedSyncCmdArgs, o *SyncEnumeratorOptions) (*syncEnumerator, error) {
				return GetSyncEnumeratorWithSrcComparator(cca, common.EFolderPropertiesOption.NoFolders(),
					&common.CopyJobPartOrderRequest{}, nil, nil, newObjectIndexer(), nil, &copyTransferProcessor{},
					ResourceTraverserTemplate{}, ResourceTraverserTemplate{}, o)
			},
			cca: &cookedSyncCmdArgs{fromTo: common.EFromTo.BlobLocal(),
				destination: common.ResourceString{Value: t.TempDir()}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stop := errors.New("stop")
			calls := 0
			o := &SyncEnumeratorOptions{
				SyncOrchOptions: &orchOptions,
				BeforeFinalPart: func() error { calls++; return stop },
			}
			e, err := tc.build(tc.cca, o)
			if err != nil {
				t.Fatalf("building the enumerator: %v", err)
			}
			// With the hook failing, finalize returns before dispatchFinalPart,
			// which would otherwise need a real job (and exit when nothing is
			// scheduled).
			if err := e.finalize(); !errors.Is(err, stop) {
				t.Fatalf("finalize: got %v, want %v", err, stop)
			}
			if calls != 1 {
				t.Fatalf("hook ran %d times, want 1", calls)
			}
		})
	}
}

func TestSyncEnumeratorOptionsWithoutBeforeFinalPart(t *testing.T) {
	var nilOptions *SyncEnumeratorOptions
	if err := nilOptions.beforeFinalPart(); err != nil {
		t.Fatalf("nil options: got %v, want nil", err)
	}
	if err := (&SyncEnumeratorOptions{}).beforeFinalPart(); err != nil {
		t.Fatalf("no hook: got %v, want nil", err)
	}
}
