package e2etest

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"testing"
)

func TestSMB_FromShareSnapshot(t *testing.T) {
	RunScenarios(t, eOperation.Copy(), eTestFromTo.Other(common.EFromTo.FileFile()), eValidate.AutoPlusContent(), params{
		recursive:              true,
		preserveSMBInfo:        true,
		preserveSMBPermissions: true,
	}, &hooks{
		// create a snapshot for the source share
		beforeRunJob: func(h hookHelper) {
			h.CreateSourceSnapshot()
		},
	}, testFiles{
		defaultSize: "4M",
		shouldTransfer: []interface{}{
			folder(""), // root folder
			folder("folder1"),
			f("folder1/filea"),
		},
	}, EAccountType.Standard(), "")
}
