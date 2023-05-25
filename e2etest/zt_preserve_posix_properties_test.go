//go:build linux
// +build linux

package e2etest

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"testing"
	"time"
)

// Block/char device rep is untested due to difficulty to test
func TestPOSIX_SpecialFilesToBlob(t *testing.T) {
	ptr := func(u uint32) *uint32 {
		return &u
	}

	RunScenarios(
		t,
		eOperation.Copy(),
		eTestFromTo.Other(common.EFromTo.LocalBlob(), common.EFromTo.BlobLocal()), // no blobblob since that's just metadata and we already test that
		eValidate.Auto(),
		allCredentialTypes, // this relies upon a working source info provider; this validates appropriate creds are supplied to it.
		anonymousAuthOnly,
		params{
			recursive: true,
			preservePOSIXProperties: true,
			symlinkHandling: common.ESymlinkHandlingType.Preserve(),
		},
		nil,
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				folder(""),
				f("fifo", with{ posixProperties: objectUnixStatContainer{ mode: ptr(common.DEFAULT_FILE_PERM | common.S_IFIFO) } }), // fifo should work
				f("sock", with{ posixProperties: objectUnixStatContainer{ mode: ptr(common.DEFAULT_FILE_PERM | common.S_IFSOCK) } }), // sock should work
				"a",
				symlink("b", "a"), //symlink to real target should succeed
				symlink("d", "c"), //symlink to nowhere should succeed
			},
		},
		EAccountType.Standard(), EAccountType.Standard(), "",
		)
}

// *** TESTS DISABLED UNTIL POSIX PROPS HNS PR ***

// Block/char device rep is untested due to difficulty to test
func TestPOSIX_SpecialFilesToHNS(t *testing.T) {
	ptr := func(u uint32) *uint32 {
		return &u
	}

	RunScenarios(
		t,
		eOperation.Copy(),
		eTestFromTo.Other(common.EFromTo.LocalBlob()), // no blobblob since that's just metadata and we already test that
		eValidate.Auto(),
		anonymousAuthOnly, // this is a small test, so running it with all cred types (which will really just be oauth and anon) is fine
		anonymousAuthOnly,
		params{
			recursive: true,
			preservePOSIXProperties: true,
			symlinkHandling: common.ESymlinkHandlingType.Preserve(),
		},
		nil,
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				folder(""),
				f("fifo", with{ posixProperties: objectUnixStatContainer{ mode: ptr(common.DEFAULT_FILE_PERM | common.S_IFIFO) } }), // fifo should work
				f("sock", with{ posixProperties: objectUnixStatContainer{ mode: ptr(common.DEFAULT_FILE_PERM | common.S_IFSOCK) } }), // sock should work
				"a",
				symlink("b", "a"), //symlink to real target should succeed
				symlink("d", "c"), //symlink to nowhere should succeed
			},
		},
		EAccountType.HierarchicalNamespaceEnabled(), EAccountType.Standard(), "",
	)
}

// Block/char device rep is untested due to difficulty to test
func TestPOSIX_SpecialFilesFromHNS(t *testing.T) {
	RunScenarios(
		t,
		eOperation.Copy(),
		eTestFromTo.Other(common.EFromTo.BlobLocal()), // no blobblob since that's just metadata and we already test that
		eValidate.Auto(),
		anonymousAuthOnly, // this is a small test, so running it with all cred types (which will really just be oauth and anon) is fine
		anonymousAuthOnly,
		params{
			recursive: true,
			preservePOSIXProperties: true,
			symlinkHandling: common.ESymlinkHandlingType.Preserve(),
		},
		nil,
		testFiles{
			defaultSize: "1K",
			shouldTransfer: []interface{}{
				folder("", with{ posixProperties: objectUnixStatContainer{ modTime: pointerTo(time.Now().Add(time.Second*-5))}}),
				f("fifo", with{ posixProperties: objectUnixStatContainer{ mode: pointerTo(uint32(common.DEFAULT_FILE_PERM | common.S_IFIFO)) } }),  // fifo should work
				f("sock", with{ posixProperties: objectUnixStatContainer{ mode: pointerTo(uint32(common.DEFAULT_FILE_PERM | common.S_IFSOCK)) } }), // sock should work
				f("a", with{ posixProperties: objectUnixStatContainer{ modTime: pointerTo(time.Now().Add(time.Second*-5))}}),
				symlink("b", "a"), //symlink to real target should succeed
				symlink("d", "c"), //symlink to nowhere should succeed
			},
		},
		EAccountType.Standard(), EAccountType.HierarchicalNamespaceEnabled(), "",
	)
}