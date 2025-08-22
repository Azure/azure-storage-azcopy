package e2etest

import (
	"runtime"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// TODO: add some tests (or modify the above) to make assertions about case preservation (or not) in metadata
//
//	See https://github.com/Azure/azure-storage-azcopy/issues/113 (which incidentally, I'm not observing in the tests above, for reasons unknown)
func TestProperties_SMBDates(t *testing.T) {
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.Other(common.EFromTo.LocalFile(), common.EFromTo.FileLocal()), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive: true,

		// default, but present for clarity
		//preserveSMBInfo:        to.Ptr(true),
	}, &hooks{
		beforeRunJob: func(h hookHelper) {
			// Pause then re-write all the files, so that their LastWriteTime is different from their creation time
			// So that when validating, our validation can be sure that the right datetime has ended up in the right
			// field
			time.Sleep(5 * time.Second)
			h.CreateFiles(h.GetTestFiles(), true, true, false)
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
		shouldTransfer: []interface{}{
			folder(""),
			"filea",
			folder("fold1"),
			"fold1/fileb",
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestProperties_SMBFlags(t *testing.T) {
	RunScenarios(t, eOperation.CopyAndSync(), eTestFromTo.Other(common.EFromTo.LocalFile(), common.EFromTo.FileFile(), common.EFromTo.FileLocal()), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive: true,

		// default, but present for clarity
		//preserveSMBInfo:        to.Ptr(true),
	}, nil, testFiles{
		defaultSize: "1K",
		shouldTransfer: []interface{}{
			folder("", with{smbAttributes: 2}), // hidden
			f("file1.txt", with{smbAttributes: 2}),
			folder("fldr1", with{smbAttributes: 2}),
			f("fldr1/file2.txt", with{smbAttributes: 2}),
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestProperties_SMBPermsAndFlagsWithIncludeAfter(t *testing.T) {
	recreateFiles := []interface{}{
		folder("", with{smbAttributes: 2}),
		f("filea", with{smbAttributes: 2}),
	}

	skippedFiles := []interface{}{
		folder("fold1", with{smbAttributes: 2}),
		f("fold1/fileb", with{smbAttributes: 2}),
	}

	RunScenarios(t, eOperation.Copy(), eTestFromTo.Other(common.EFromTo.FileLocal()), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive: true,

		// default, but present for clarity
		//preserveSMBInfo:        to.Ptr(true),
		// includeAfter: SET LATER
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

// TODO: Sync test for modern LMT getting
func TestProperties_SMBPermsAndFlagsWithSync(t *testing.T) {
	recreateFiles := []interface{}{
		folder("", with{smbAttributes: 2}),
		f("filea", with{smbAttributes: 2}),
		folder("fold2", with{smbAttributes: 2}),
		f("fold2/filec", with{smbAttributes: 2}),
	}

	transferredFiles := []interface{}{
		folder("fold1", with{smbAttributes: 2}),
		f("fold1/fileb", with{smbAttributes: 2}),
	}

	RunScenarios(t, eOperation.Sync(), eTestFromTo.Other(common.EFromTo.LocalFile(), common.EFromTo.FileLocal()), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive: true,

		// default, but present for clarity
		//preserveSMBInfo:        to.Ptr(true),
	}, &hooks{
		beforeRunJob: func(h hookHelper) {
			// Pause then re-write all the files, so that their LastWriteTime is different from their creation time
			// So that when validating, our validation can be sure that the right datetime has ended up in the right
			// field
			time.Sleep(5 * time.Second)
			h.CreateFiles(testFiles{
				defaultSize:    "1K",
				shouldTransfer: recreateFiles,
			}, false, false, true)

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
		shouldTransfer: transferredFiles,
		shouldIgnore:   recreateFiles,
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestProperties_SMBTimes(t *testing.T) {
	RunScenarios(
		t,
		eOperation.Sync(),
		eTestFromTo.Other(common.EFromTo.FileLocal()),
		eValidate.Auto(),
		anonymousAuthOnly,
		anonymousAuthOnly,
		params{
			recursive: true,

			// default, but present for clarity
			//preserveSMBInfo:        to.Ptr(true),
		},
		&hooks{
			beforeTestRun: func(h hookHelper) {
				if runtime.GOOS != "windows" {
					h.SkipTest()
				}
			},
		},
		testFiles{
			defaultSize: "1K",

			shouldSkip: []interface{}{
				folder("", with{lastWriteTime: time.Now().Add(-time.Hour)}),    // If the fix worked, these should not be overwritten.
				f("asdf.txt", with{lastWriteTime: time.Now().Add(-time.Hour)}), // If the fix did not work, we'll be relying upon the service's "real" LMT, which is not what we persisted, and an hour ahead of our files.
			},
		},
		EAccountType.Standard(),
		EAccountType.Standard(),
		"",
	)
}

func TestProperties_EnsureContainerBehavior(t *testing.T) {
	RunScenarios(
		t,
		eOperation.Copy(),
		eTestFromTo.Other(common.EFromTo.FileFile()),
		eValidate.Auto(),
		anonymousAuthOnly,
		anonymousAuthOnly,
		params{
			recursive:              true,
			preserveSMBInfo:        to.Ptr(true),
			preserveSMBPermissions: true,
		},
		nil,
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				folder(""),
				f("aeiou.txt"),
				folder("a"),
				f("a/asdf.txt"),
				folder("b"),
				f("b/1234.txt"),
			},
		},
		EAccountType.Standard(),
		EAccountType.Standard(),
		"",
	)
}

func TestProperties_ForceReadOnly(t *testing.T) {
	RunScenarios(
		t,
		eOperation.Sync(),
		eTestFromTo.Other(common.EFromTo.FileFile()),
		eValidate.Auto(),
		anonymousAuthOnly,
		anonymousAuthOnly,
		params{
			recursive:         true,
			deleteDestination: common.EDeleteDestination.True(),
			forceIfReadOnly:   true,
		},
		&hooks{
			beforeRunJob: func(h hookHelper) {
				// Create dest-only item to get deleted; it MUST be read-only, or else this test is invalid.
				h.CreateFile(f("destOnly.txt", with{smbAttributes: 1}), false)
				// Create item to get overwritten; it MUST be read-only, or else this test is invalid.
				h.CreateFile(f("asdf.txt", with{smbAttributes: 1}), false)

				time.Sleep(10 * time.Second)
				h.CreateFile(f("asdf.txt"), true)
			},
			afterValidation: func(h hookHelper) {
				c := h.GetAsserter()

				objects := h.GetDestination().getAllProperties(c)
				_, ok := objects["destOnly.txt"]
				c.Assert(ok, equals(), false, "Did not expect to find destOnly.txt in destination")
			},
		},
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				folder(""),
				f("asdf.txt"),
			},
		},
		EAccountType.Standard(),
		EAccountType.Standard(),
		"",
	)
}
