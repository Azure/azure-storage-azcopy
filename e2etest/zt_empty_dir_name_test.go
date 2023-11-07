package e2etest

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"testing"
)

func TestEmptyDir_CopyUploadSingleBlob(t *testing.T) {
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
