//go:build linux
// +build linux

package e2etest

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"testing"
)

func TestPOSIX_Symlinks(t *testing.T) {
	RunScenarios(t,
		eOperation.Copy(),
		// eTestFromTo.Other(common.EFromTo.BlobLocal(), common.EFromTo.BlobBlob(), common.EFromTo.LocalBlob()),
		eTestFromTo.Other(common.EFromTo.LocalBlob(), common.EFromTo.BlobLocal()),
		eValidate.Auto(),
		anonymousAuthOnly, // this is a small test, so running it with all cred types (which will really just be oauth and anon) is fine
		anonymousAuthOnly,
		params{
			recursive: true,
			// preservePOSIXProperties: true,
			symlinkHandling: common.ESymlinkHandlingType.Preserve(),
		},
		nil,
		testFiles{
			defaultSize: "1M",
			shouldTransfer: []interface{}{
				folder(""),
				"a",
				symlink("b", "a"),
				folder("c"),
				symlink("d", "c"),
			},
		},
		EAccountType.Standard(), EAccountType.Standard(), "",
	)
}
