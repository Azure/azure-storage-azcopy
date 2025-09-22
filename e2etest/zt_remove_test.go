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

package e2etest

import (
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func TestRemove_IncludeAfter(t *testing.T) {
	recreateFiles := []interface{}{
		folder(""),
		f("filea"),
	}

	skippedFiles := []interface{}{
		folder("fold1"),
		f("fold1/fileb"),
	}
	// these filters aren't supported for blobFS
	RunScenarios(t, eOperation.Remove(), eTestFromTo.Other(common.EFromTo.BlobTrash(), common.EFromTo.FileTrash()), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive: true,
	}, &hooks{
		beforeRunJob: func(h hookHelper) {
			// Pause for a includeAfter time
			time.Sleep(5 * time.Second)
			h.GetModifiableParameters().includeAfter = time.Now().Format(azcopy.ISO8601)
			// Pause then re-write all the files, so that their LastWriteTime is different from their creation time
			// So that when validating, our validation can be sure that the right datetime has ended up in the right
			// field
			time.Sleep(5 * time.Second)
			h.CreateFiles(testFiles{
				defaultSize:    "1K",
				shouldTransfer: recreateFiles,
			}, true, true, false)

			// And pause again, so that that the write times at the destination wont' just _automatically_ match the source times
			// (due to there being < 1 sec delay between creation and completion of copy). With this delay, we know they only match
			// if AzCopy really did preserve them
			time.Sleep(10 * time.Second) // we are assuming here, that the clock skew between source and dest is less than 10 secs
		},
	}, testFiles{
		defaultSize: "1K",
		// no need to set specific dates on these. Instead, we just mess with the write times in
		// beforeRunJob
		// TODO: is that what we really want, or do we want to set write times here?
		shouldTransfer: recreateFiles,
		shouldIgnore:   skippedFiles,
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestRemove_WithSnapshotsBlob(t *testing.T) {
	blobRemove := TestFromTo{
		desc:      "AllRemove",
		useAllTos: true,
		froms: []common.Location{
			common.ELocation.Blob(),
		},
		tos: []common.Location{
			common.ELocation.Unknown(),
		},
	}
	RunScenarios(t, eOperation.Remove(), blobRemove, eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive: true,
	}, &hooks{
		beforeRunJob: func(h hookHelper) {
			blobClient := h.GetSource().(*resourceBlobContainer).containerClient.NewBlobClient("filea")
			_, err := blobClient.CreateSnapshot(ctx, nil)
			if err != nil {
				t.Errorf("error creating snapshot %s", err)
			}
		},
		afterValidation: func(h hookHelper) {
			blobClient := h.GetSource().(*resourceBlobContainer).containerClient.NewBlobClient("filea")
			_, err := blobClient.Delete(ctx, &blob.DeleteOptions{DeleteSnapshots: to.Ptr(blob.DeleteSnapshotsOptionTypeInclude)})
			if err != nil {
				t.Errorf("error deleting blob %s", err)
			}
		},
	}, testFiles{
		defaultSize: "1K",
		shouldSkip: []interface{}{
			f("filea"),
		},
		objectTarget: objectTarget{objectName: "filea"},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestRemove_SnapshotsBlob(t *testing.T) {
	blobRemove := TestFromTo{
		desc:      "AllRemove",
		useAllTos: true,
		froms: []common.Location{
			common.ELocation.Blob(),
		},
		tos: []common.Location{
			common.ELocation.Unknown(),
		},
	}
	RunScenarios(t, eOperation.Remove(), blobRemove, eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive: true,
	}, &hooks{
		beforeRunJob: func(h hookHelper) {
			// Snapshot creation will happen in the getParam method
		},
		afterValidation: func(h hookHelper) {
			blobClient := h.GetSource().(*resourceBlobContainer).containerClient.NewBlobClient("filea")
			_, err := blobClient.Delete(ctx, nil)
			if err != nil {
				t.Errorf("error deleting blob %s", err)
			}
		},
	}, testFiles{
		defaultSize: "1K",
		shouldTransfer: []interface{}{
			folder(""),
			f("filea"),
		},
		objectTarget: objectTarget{objectName: "filea", snapshotid: true},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}
