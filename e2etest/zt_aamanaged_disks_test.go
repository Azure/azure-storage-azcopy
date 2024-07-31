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
	"flag"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"runtime"
	"testing"
	"time"
)

// Purpose: Tests for the special cases that relate to moving managed disks (default local VHD to page blob; special handling for
//     md- and md-impex URLs.

var runManagedDiskSnapshot = flag.Bool("md-snapshot", false, "Whether or not to run snapshot managed disk tests")

func TestManagedDisks_NoOAuthRequired(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Limit runs to Linux so no simultaneous runs occur")
		return
	}

	RunScenarios(
		t,
		eOperation.Copy(),
		eTestFromTo.Other(common.EFromTo.BlobLocal()),
		eValidate.Auto(),
		anonymousAuthOnly,
		anonymousAuthOnly,
		params{
			disableParallelTesting: true,
		},
		nil,
		testFiles{
			shouldTransfer: []interface{}{
				"",
			},
		}, // Managed disks will always have a transfer target of ""
		EAccountType.Standard(),
		EAccountType.StdManagedDisk(),
		"",
	)
}

func TestManagedDisks_Snapshot(t *testing.T) {
	if runManagedDiskSnapshot != nil && !*runManagedDiskSnapshot {
		t.Skip("Temporarily disabled test until cause is found for sudden failure")
		return
	}

	RunScenarios(
		t,
		eOperation.Copy(),
		eTestFromTo.Other(common.EFromTo.BlobLocal(), common.EFromTo.BlobBlob()), // It's relevant to test blobblob since this interfaces with x-ms-copysourceauthorization
		eValidate.Auto(),
		anonymousAuthOnly,
		anonymousAuthOnly,
		params{
			disableParallelTesting: true,
		},
		nil,
		testFiles{
			shouldTransfer: []interface{}{
				"",
			},
		}, // Managed disks will always have a transfer target of ""
		EAccountType.Standard(),
		EAccountType.ManagedDiskSnapshot(),
		"",
	)
}

func TestManagedDisks_SnapshotOAuth(t *testing.T) {
	if runManagedDiskSnapshot != nil && !*runManagedDiskSnapshot {
		t.Skip("Temporarily disabled test until cause is found for sudden failure")
		return
	}

	RunScenarios(
		t,
		eOperation.Copy(),
		eTestFromTo.Other(common.EFromTo.BlobLocal(), common.EFromTo.BlobBlob()), // It's relevant to test blobblob since this interfaces with x-ms-copysourceauthorization
		eValidate.Auto(),
		[]common.CredentialType{common.ECredentialType.MDOAuthToken()},
		[]common.CredentialType{common.ECredentialType.Anonymous(), common.ECredentialType.OAuthToken()},
		params{
			disableParallelTesting: true,
		},
		nil,
		testFiles{
			shouldTransfer: []interface{}{
				"",
			},
		}, // Managed disks will always have a transfer target of ""
		EAccountType.Standard(),
		EAccountType.ManagedDiskSnapshotOAuth(),
		"",
	)
}

// Service issue causes occasional flickers in feature functionality; enough that testing is problematic. Temporarily disabled until issue is resolved.
func TestManagedDisks_OAuthRequired(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Limit runs to Linux so no simultaneous runs occur")
		return
	}

	RunScenarios(
		t,
		eOperation.Copy(),
		eTestFromTo.Other(common.EFromTo.BlobLocal(), common.EFromTo.BlobBlob()), // It's relevant to test blobblob since this interfaces with x-ms-copysourceauthorization
		eValidate.Auto(),
		[]common.CredentialType{common.ECredentialType.MDOAuthToken()},
		[]common.CredentialType{common.ECredentialType.Anonymous(), common.ECredentialType.OAuthToken()},
		params{
			disableParallelTesting: true, // testing is implemented with a single managed disk
		},
		&hooks{
			beforeRunJob: func(h hookHelper) {
				// try giving the service some time to think
				time.Sleep(time.Second * 30)
			},
		},
		testFiles{
			shouldTransfer: []interface{}{
				"",
			},
		}, // Managed disks will always have a transfer target of ""
		EAccountType.Standard(),
		EAccountType.OAuthManagedDisk(),
		"",
	)
}

// Test a managed disk of size 7.9 TB
func TestManagedDisks_LargeManagedDisk(t *testing.T) {
	if runLocallyOnly != nil && !*runLocallyOnly {
		t.Skip("This test runs locally only. Set up config for managed disk of size 7.9 TB " +
			"(AZCOPY_E2E_LARGE_MANAGED_DISK_CONFIG) and run this test.")
		return
	}

	RunScenarios(
		t,
		eOperation.Copy(),
		eTestFromTo.Other(common.EFromTo.BlobLocal(), common.EFromTo.BlobBlob()),
		eValidate.Auto(),
		anonymousAuthOnly,
		anonymousAuthOnly,
		params{
			disableParallelTesting: true,
		},
		nil,
		testFiles{
			shouldTransfer: []interface{}{
				"", // Managed disks will always have a transfer target of ""
			},
		},
		EAccountType.Standard(),
		EAccountType.LargeManagedDisk(),
		"",
	)
}

// Test a managed disk snapshot of size 7.9 TB
func TestManagedDisks_LargeSnapshot(t *testing.T) {
	if runLocallyOnly != nil && !*runLocallyOnly {
		t.Skip("This test runs locally only. Set up config for managed disk snapshot of size 7.9 TB " +
			"(AZCOPY_E2E_LARGE_MANAGED_DISK_SNAPSHOT_CONFIG) and run this test.")
		return
	}

	RunScenarios(
		t,
		eOperation.Copy(),
		eTestFromTo.Other(common.EFromTo.BlobLocal(), common.EFromTo.BlobBlob()),
		eValidate.Auto(),
		anonymousAuthOnly,
		anonymousAuthOnly,
		params{
			disableParallelTesting: true,
		},
		nil,
		testFiles{
			shouldTransfer: []interface{}{
				"", // Managed disks will always have a transfer target of ""
			},
		},
		EAccountType.Standard(),
		EAccountType.LargeManagedDiskSnapshot(),
		"",
	)
}
