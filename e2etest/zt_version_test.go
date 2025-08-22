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
	"github.com/stretchr/testify/assert"
	"os/exec"
	"regexp"
	"strings"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func TestVersionCommand(t *testing.T) {
	azcopyVersionString := "azcopy version " + common.AzcopyVersion
	newVersionInfo := regexp.MustCompile("INFO: azcopy.* .*: A newer version .* is available to download")
	cmd := exec.Command(GlobalInputManager{}.GetExecutablePath(), "--version")
	o, err := cmd.Output()
	if err != nil {
		t.Log("Version failed with error " + err.Error())
		t.FailNow()
	}

	output := string(o)

	//fail if no output
	if output == "" {
		t.Log("Version command returned empty string.")
		t.FailNow()
	}

	output = strings.TrimSpace(output)

	lines := strings.Split(string(output), "\n")

	//there should be max of 2 lines, with first describing version
	// and second stating if a newer version is available.
	if len(lines) > 2 {
		t.Log("Invalid output " + string(output))
		t.FailNow()
	}
	//first line should contain the version similar to "azcopy version 10.22.0"
	if lines[0] != azcopyVersionString {
		t.Log("Invalid version string: " + lines[0])
		t.FailNow()
	}

	//second line, if present, should be a "new version available" message
	if len(lines) == 2 && !newVersionInfo.Match([]byte(lines[1])) {
		t.Log("Second Line does not contain new version info " + lines[1])
		t.FailNow()
	}
}

func TestCheckVersionCommand(t *testing.T) {
	a := assert.New(t)
	cmd := exec.Command(GlobalInputManager{}.GetExecutablePath(), "--check-version")
	o, err := cmd.Output()
	if err != nil {
		t.Log("Version check failed with error " + err.Error())
		t.FailNow()
	}

	output := string(o)

	if output == "" {
		t.Log("Version check command returned empty string")
		t.FailNow()
	}

	output = strings.TrimSpace(output)
	lines := strings.Split(output, "\n")

	// There might be a warning message in the output, we should skip this
	if len(lines) > 1 {
		skipLine := func() (val int) {
			for i, line := range lines {
				if strings.Contains(line, "WARN:") {
					return i
				}
			}
			return
		}()
		lines = lines[skipLine+1:]

		// There should now be a max of one line for if the version is up to date or old
		if len(lines) > 1 {
			t.Log("Invalid output " + string(output))
			t.FailNow()
		}
	}

	// Set of output options
	newVersionInfo := regexp.MustCompile("INFO: azcopy.* .*: A newer version .* is available to download")
	upToDateVersionInfo := regexp.MustCompile("INFO: Current azcopy version *.*.* up to date")

	if !(newVersionInfo.Match([]byte(lines[0])) || upToDateVersionInfo.Match([]byte(lines[0]))) {
		t.Log("Proper version checking information not returned")
		t.FailNow()
	}

	// Can only be one of two
	a.True(newVersionInfo.Match([]byte(lines[0])) || upToDateVersionInfo.Match([]byte(lines[0])))
}
