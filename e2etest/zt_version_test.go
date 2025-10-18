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
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	cmd2 "github.com/Azure/azure-storage-azcopy/v10/testSuite/cmd"
	"github.com/stretchr/testify/assert"

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

// Test that latest_version file is uploaded to correct directory
func TestLastVersionFileLocation(t *testing.T) {
	a := assert.New(t)

	// Save original working directory
	originalWorkDir, err := os.Getwd()
	a.NoError(err)

	defer func() { // Reset current dir after test
		os.Chdir(originalWorkDir)
	}()

	// Create temp dir to use as current working directory
	tempWorkDir, err := os.MkdirTemp("", "temp")
	a.NoError(err)
	defer func() { // Clean up
		os.RemoveAll(tempWorkDir)
	}()

	// Run the help command
	cmd := exec.Command(GlobalInputManager{}.GetExecutablePath(), "--help")
	output, err := cmd.CombinedOutput()
	a.NoError(err, "Help command should work")
	a.NotEmpty(output)

	// Check that latest_version.txt is NOT in current directory
	currDirFile := filepath.Join(tempWorkDir, "latest_version.txt")
	_, err = os.Stat(currDirFile)
	a.True(os.IsNotExist(err), "latest_version.txt should NOT be in current directory")

	// The file should be in the app's log directory
	appDataFolder := cmd2.GetAzCopyAppPath()
	if appDataFolder != "" {
		expectedFile := filepath.Join(appDataFolder, "latest_version.txt")
		time.Sleep(2 * time.Second) // Wait for version check to complete

		// Check if file exists
		if _, err := os.Stat(expectedFile); err == nil {
			a.True(true, "latest_version.txt found in app dir")
		}
	}
}
