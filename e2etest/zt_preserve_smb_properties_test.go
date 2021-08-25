// +build windows

package e2etest

import (
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"golang.org/x/sys/windows"
)

const SampleSDDL = "O:<placeholder>G:<placeholder>D:AI(A;ID;FA;;;SY)(A;ID;FA;;;BA)(A;ID;FA;;;<placeholder>)S:NO_ACCESS_CONTROL"
const RootSampleSDDL = "O:<placeholder>G:<placeholder>D:PAI(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)(A;OICI;FA;;;<placeholder>)S:NO_ACCESS_CONTROL"
const FolderSampleSDDL = "O:<placeholder>G:<placeholder>D:AI(A;OICIID;FA;;;SY)(A;OICIID;FA;;;BA)(A;OICIID;FA;;;<placeholder>)S:NO_ACCESS_CONTROL"
const SampleSDDLPlaceHolder = "<placeholder>"

func AdjustSDDLToLocal(sample, placeholder string) (sddlOut string, err error) {
	nameBuffer := make([]uint16, 50)
	bufSize := uint32(len(nameBuffer))

	for {
		err = windows.GetUserNameEx(windows.NameSamCompatible, &nameBuffer[0], &bufSize)

		if err == windows.ERROR_INSUFFICIENT_BUFFER {
			// Win32 APIs will adjust our buffer size, we just need to reallocate
			nameBuffer = make([]uint16, bufSize)

			continue
		} else if err != nil {
			return "", err
		}

		break
	}

	// thankfully the windows package does this for us
	sid, _, _, err := windows.LookupSID("", windows.UTF16ToString(nameBuffer))
	if err != nil {
		return "", err
	}

	return strings.ReplaceAll(sample, placeholder, sid.String()), nil
}

func TestProperties_SMBPermissionsSDDLPreserved(t *testing.T) {
	fileSDDL, err := AdjustSDDLToLocal(SampleSDDL, SampleSDDLPlaceHolder)
	if err != nil {
		t.Error(err)
	}

	rootSDDL, err := AdjustSDDLToLocal(RootSampleSDDL, SampleSDDLPlaceHolder)
	if err != nil {
		t.Error(err)
	}

	folderSDDL, err := AdjustSDDLToLocal(FolderSampleSDDL, SampleSDDLPlaceHolder)
	if err != nil {
		t.Error(err)
	}

	RunScenarios(
		t,
		eOperation.Copy(),
		// FileLocal isn't supported in the test suite, because it requires SeBackupPrivilege for accurate testing
		// Anybody know how to get that going on Azure pipelines? Or just hand a UAC escalation prompt off to the suite runner so it can be tested locally?
		eTestFromTo.Other(
			common.EFromTo.LocalFile(),
			// common.EFromTo.FileFile(), // TODO: finish inquiring with Jason Shay about this wonkiness. Context: Auto-inherit bit is getting flipped on S2S unrelated to azcopy
			),
		eValidate.Auto(),
		params{
			recursive: true,
			preserveSMBInfo: true,
			preserveSMBPermissions: true,
		},
		nil,
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				folder("", with{ smbPermissionsSddl: rootSDDL}),
				f("file1", with{ smbPermissionsSddl: fileSDDL}),
				f("file2.txt", with{ smbPermissionsSddl: fileSDDL}),
				folder("fldr1", with{ smbPermissionsSddl: folderSDDL}),
				f("fldr1/file3.txt", with{ smbPermissionsSddl: fileSDDL}),
			},
		})
}

// TODO: add some tests (or modify the above) to make assertions about case preservation (or not) in metadata
//    See https://github.com/Azure/azure-storage-azcopy/issues/113 (which incidentally, I'm not observing in the tests above, for reasons unknown)
//
func TestProperties_SMBDates(t *testing.T) {
	RunScenarios(
		t,
		eOperation.CopyAndSync(),
		eTestFromTo.Other(common.EFromTo.LocalFile(), common.EFromTo.FileLocal()), // these are the only pairs where we preserve last write time AND creation time
		eValidate.Auto(),
		params{
			recursive:       true,
			preserveSMBInfo: true,
		},
		&hooks{
			beforeRunJob: func(h hookHelper) {
				// Pause then re-write all the files, so that their LastWriteTime is different from their creation time
				// So that when validating, our validation can be sure that the right datetime has ended up in the right
				// field
				time.Sleep(5 * time.Second)
				h.CreateFiles(h.GetTestFiles(), true)
				// And pause again, so that that the write times at the destination wont' just _automatically_ match the source times
				// (due to there being < 1 sec delay between creation and completion of copy). With this delay, we know they only match
				// if AzCopy really did preserve them
				time.Sleep(10 * time.Second) // we are assuming here, that the clock skew between source and dest is less than 10 secs
			},
		},
		testFiles{
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
		})
}

func TestProperties_SMBFlags(t *testing.T) {
	RunScenarios(
		t,
		eOperation.CopyAndSync(),
		eTestFromTo.Other(common.EFromTo.LocalFile(), common.EFromTo.FileFile(), common.EFromTo.FileLocal()),
		eValidate.Auto(),
		params{
			recursive: true,
			preserveSMBInfo: true,
		},
		nil,
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface {}{
				folder("", with{smbAttributes: 2}), // hidden
				f("file1.txt", with{smbAttributes: 2}),
				folder("fldr1", with{smbAttributes: 2}),
				f("fldr1/file2.txt", with{smbAttributes: 2}),
			},
		},
	)
}
