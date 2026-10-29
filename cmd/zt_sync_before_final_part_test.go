// Copyright © Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cmd

import (
	"errors"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
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
			a := assert.New(t)
			stop := errors.New("stop")
			calls := 0
			o := &SyncEnumeratorOptions{
				SyncOrchOptions: &orchOptions,
				BeforeFinalPart: func() error { calls++; return stop },
			}
			e, err := tc.build(tc.cca, o)
			if !a.NoError(err, "building the enumerator") {
				return
			}
			// With the hook failing, finalize returns before dispatchFinalPart,
			// which would otherwise need a real job (and exit when nothing is
			// scheduled).
			a.ErrorIs(e.finalize(), stop)
			a.Equal(1, calls, "hook runs")
		})
	}
}
