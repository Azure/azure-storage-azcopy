//go:build windows
// +build windows

package e2etest

import (
	"strings"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"golang.org/x/sys/windows"
)

const SampleSDDL = "O:<placeholder>G:<placeholder>D:AI(A;ID;FA;;;SY)(A;ID;FA;;;BA)(A;ID;FA;;;<placeholder>)(D;;FX;;;SY)S:NO_ACCESS_CONTROL"
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

func TestPermissions_SMBSDDLPreserved(t *testing.T) {
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

	RunScenarios(t, eOperation.Copy(), eTestFromTo.Other(
		common.EFromTo.LocalFileSMB(),
		common.EFromTo.FileSMBLocal(),
		common.EFromTo.FileSMBFileSMB(),
	), eValidate.Auto(), anonymousAuthOnly, anonymousAuthOnly, params{
		recursive:              true,
		preserveSMBPermissions: true,

		// default, but present for clarity
		//preserveSMBInfo:        to.Ptr(true),
	}, nil, testFiles{
		defaultSize: "1K",
		shouldTransfer: []interface{}{
			folder("", with{smbPermissionsSddl: rootSDDL}),
			f("file1", with{smbPermissionsSddl: fileSDDL}),
			f("file2.txt", with{smbPermissionsSddl: fileSDDL}),
			folder("fldr1", with{smbPermissionsSddl: folderSDDL}),
			f("fldr1/file3.txt", with{smbPermissionsSddl: fileSDDL}),
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestPermissions_SMBWithCopyWithShareRoot(t *testing.T) {
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
		eOperation.Copy(), // Sync already shares the root by default.
		eTestFromTo.Other(common.EFromTo.LocalFileSMB()),
		eValidate.Auto(),
		anonymousAuthOnly,
		anonymousAuthOnly,
		params{
			recursive:              true,
			invertedAsSubdir:       true,
			preserveSMBPermissions: true,

			// default, but present for clarity
			//preserveSMBInfo:        to.Ptr(true),
		},
		nil,
		testFiles{
			defaultSize: "1K",
			destTarget:  "newName",

			shouldTransfer: []interface{}{
				folder("", with{smbAttributes: 2, smbPermissionsSddl: rootSDDL}),
				f("asdf.txt", with{smbAttributes: 2, smbPermissionsSddl: fileSDDL}),
				folder("a", with{smbAttributes: 2, smbPermissionsSddl: folderSDDL}),
				f("a/asdf.txt", with{smbAttributes: 2, smbPermissionsSddl: fileSDDL}),
				folder("a/b", with{smbAttributes: 2, smbPermissionsSddl: folderSDDL}),
				f("a/b/asdf.txt", with{smbAttributes: 2, smbPermissionsSddl: fileSDDL}),
			},
		},
		EAccountType.Standard(),
		EAccountType.Standard(),
		"",
	)
}
