package e2etest

import (
	"runtime"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func TestSMB_FromShareSnapshot(t *testing.T) {
	RunScenarios(t, eOperation.Copy(), eTestFromTo.Other(common.EFromTo.FileFile()), eValidate.AutoPlusContent(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive:              true,
		preserveSMBPermissions: true,

		// default, but present for clarity
		//preserveSMBInfo:        to.Ptr(true),
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
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestSMB_ToDevNull(t *testing.T) {
	isWindows := strings.EqualFold(runtime.GOOS, "windows")

	RunScenarios(t,
		eOperation.Copy(),
		eTestFromTo.Other(common.EFromTo.FileLocal()),
		eValidate.Auto(),
		anonymousAuthOnly,
		anonymousAuthOnly,
		params{
			recursive:              true,
			preserveSMBPermissions: isWindows,
			preserveSMBInfo:        to.Ptr(isWindows),
			preserveInfo:           to.Ptr(isWindows),
			checkMd5:               common.EHashValidationOption.FailIfDifferent(),
			destNull:               true,
		},
		nil,
		testFiles{
			defaultSize: defaultStringFileSize,
			shouldTransfer: []interface{}{
				folder(""),
				f("foo"),
				folder("a"),
				f("a/bar"),
			},
		},
		EAccountType.Standard(),
		EAccountType.Standard(),
		"",
	)
}
