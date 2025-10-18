package e2etest

import (
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func TestEmptyDir_CopySyncS2SBlob(t *testing.T) {
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.Other(common.EFromTo.BlobBlob()), eValidate.AutoPlusContent(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive: true,
	}, nil, testFiles{
		defaultSize: "1K",
		shouldTransfer: []interface{}{
			folder(""),
			folder("foo"),
			folder("foo/"),
			f("foo//file1.txt"),
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestEmptyDir_RemoveBlob(t *testing.T) {
	blobRemove := TestFromTo{
		desc:      "BlobRemove",
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
	}, nil, testFiles{
		defaultSize: "1K",
		shouldTransfer: []interface{}{
			folder(""),
			folder("foo"),
			folder("foo/"),
			f("foo//file1.txt"),
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}
