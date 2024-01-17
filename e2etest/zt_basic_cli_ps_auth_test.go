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
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// Purpose: Tests AZCLI and powershell auth tests

func TestBasic_AzCLIAuth(t *testing.T) {
	RunScenarios(t, eOperation.Copy(), eTestFromTo.Other(common.EFromTo.BlobBlob()), eValidate.Auto(), oAuthOnly, oAuthOnly, params{ // Pass flag values that the test requires. The params struct is a superset of Copy and Sync params
		recursive: true,
	}, &hooks{
		beforeTestRun: func(h hookHelper) {
			tenId, appId, clientSecret := GlobalInputManager{}.GetServicePrincipalAuth()
			args := []string{
				"login",
				"--service-principal",
				"-u=" + appId,
				"-p=" + clientSecret,
			}
			if tenId != "" {
				args = append(args, "--tenant="+tenId)
			}

			out, err := exec.Command("az", args...).Output()
			if err != nil {
				e := err.(*exec.ExitError)
				t.Logf(string(e.Stderr))
				t.Logf(string(out))
				t.Logf("Failed to login with AzCLI " + err.Error())
				t.FailNow()
			}
			os.Setenv("AZCOPY_AUTO_LOGIN_TYPE", "AZCLI")
		},
	}, testFiles{
		defaultSize: "1K",
		shouldTransfer: []interface{}{
			"wantedfile",
			folder("sub/subsub"),
			"sub/subsub/filea",
			"sub/subsub/filec",
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestBasic_AzCLIAuthLowerCase(t *testing.T) {
	RunScenarios(t, eOperation.Copy(), eTestFromTo.Other(common.EFromTo.BlobBlob()), eValidate.Auto(), oAuthOnly, oAuthOnly, params{ // Pass flag values that the test requires. The params struct is a superset of Copy and Sync params
		recursive: true,
	}, &hooks{
		beforeTestRun: func(h hookHelper) {
			tenId, appId, clientSecret := GlobalInputManager{}.GetServicePrincipalAuth()
			args := []string{
				"login",
				"--service-principal",
				"-u=" + appId,
				"-p=" + clientSecret,
			}
			if tenId != "" {
				args = append(args, "--tenant="+tenId)
			}

			out, err := exec.Command("az", args...).Output()
			if err != nil {
				e := err.(*exec.ExitError)
				t.Logf(string(e.Stderr))
				t.Logf(string(out))
				t.Logf("Failed to login with AzCLI " + err.Error())
				t.FailNow()
			}
			os.Setenv("AZCOPY_AUTO_LOGIN_TYPE", "azcli")
		},
	}, testFiles{
		defaultSize: "1K",
		shouldTransfer: []interface{}{
			"wantedfile",
			folder("sub/subsub"),
			"sub/subsub/filea",
			"sub/subsub/filec",
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestBasic_PSAuth(t *testing.T) {
	RunScenarios(t, eOperation.Copy(), eTestFromTo.Other(common.EFromTo.BlobBlob()), eValidate.Auto(), oAuthOnly, oAuthOnly, params{ // Pass flag values that the test requires. The params struct is a superset of Copy and Sync params
		recursive: true,
	}, &hooks{
		beforeTestRun: func(h hookHelper) {
			if runtime.GOOS != "windows" {
				h.SkipTest()
			}
			tenId, appId, clientSecret := GlobalInputManager{}.GetServicePrincipalAuth()
			cmd := `$secret = ConvertTo-SecureString -String %s -AsPlainText -Force;
				$cred = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList %s, $secret;
				Connect-AzAccount -ServicePrincipal -Credential $cred`
			if tenId != "" {
				cmd += " -Tenant " + tenId
			}

			script := fmt.Sprintf(cmd, clientSecret, appId)
			out, err := exec.Command("powershell", script).Output()
			if err != nil {
				e := err.(*exec.ExitError)
				t.Logf(string(e.Stderr))
				t.Logf(string(out))
				t.Logf("Failed to login with Powershell " + err.Error())
				t.FailNow()
			}
			os.Setenv("AZCOPY_AUTO_LOGIN_TYPE", "PSCRED")
		},
	}, testFiles{
		defaultSize: "1K",
		shouldTransfer: []interface{}{
			"wantedfile",
			folder("sub/subsub"),
			"sub/subsub/filea",
			"sub/subsub/filec",
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}

func TestBasic_PSAuthCamelCase(t *testing.T) {
	RunScenarios(t, eOperation.Copy(), eTestFromTo.Other(common.EFromTo.BlobBlob()), eValidate.Auto(), oAuthOnly, oAuthOnly, params{ // Pass flag values that the test requires. The params struct is a superset of Copy and Sync params
		recursive: true,
	}, &hooks{
		beforeTestRun: func(h hookHelper) {
			if runtime.GOOS != "windows" {
				h.SkipTest()
			}
			tenId, appId, clientSecret := GlobalInputManager{}.GetServicePrincipalAuth()
			cmd := `$secret = ConvertTo-SecureString -String %s -AsPlainText -Force;
				$cred = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList %s, $secret;
				Connect-AzAccount -ServicePrincipal -Credential $cred`
			if tenId != "" {
				cmd += " -Tenant " + tenId
			}

			script := fmt.Sprintf(cmd, clientSecret, appId)
			out, err := exec.Command("powershell", script).Output()
			if err != nil {
				e := err.(*exec.ExitError)
				t.Logf(string(e.Stderr))
				t.Logf(string(out))
				t.Logf("Failed to login with Powershell " + err.Error())
				t.FailNow()
			}
			os.Setenv("AZCOPY_AUTO_LOGIN_TYPE", "PsCred")
		},
	}, testFiles{
		defaultSize: "1K",
		shouldTransfer: []interface{}{
			"wantedfile",
			folder("sub/subsub"),
			"sub/subsub/filea",
			"sub/subsub/filec",
		},
	}, EAccountType.Standard(), EAccountType.Standard(), "")
}
