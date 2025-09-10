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

package common_test

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/stretchr/testify/assert"
	"os"
	"runtime"
	"syscall"
	"testing"
)

func TestCreateParentDirectoryIfNotExist(t *testing.T) {
	a := assert.New(t)

	// set up job part manager
	plan := &ste.JobPartPlanHeader{}
	fpo := common.EFolderPropertiesOption.AllFolders()

	tracker := ste.NewFolderCreationTracker(fpo, func(index ste.JpptFolderIndex) *ste.JobPartPlanTransfer {
		return plan.Transfer(index.TransferIndex)
	})
	fileName := "stuff.txt"

	// when destination path is defined as "/" in linux, the source file becomes the destination path string
	// AzCopy reaches out of bounds error, returns user friendly error
	err := common.CreateParentDirectoryIfNotExist(fileName, tracker)
	pathSep := common.DeterminePathSeparator(fileName)
	a.Errorf(err, "error: Path separator "+pathSep+" not found in destination path. On Linux, this may occur if the destination is the root file, such as '/'. If this is the case, please consider changing your destination path.")

	// in the case where destination file is specified with root "/" and source file name "stuff.txt" in linux,
	// the system will fail to create the directory
	err = common.CreateParentDirectoryIfNotExist("/"+fileName, tracker)
	a.Errorf(err, "mkdir : The system cannot find the path specified.")

	// when relative path provided (i.e., "/stuff.txt", or "stuff.txt") in windows,
	// full path is passed as destination string and destination path string will be "C://path/to/file/stuff.txt"
	// this will safely complete and return nil
	file, err := os.Create(fileName)
	a.NoError(err)
	defer os.Remove(file.Name())
	defer file.Close()

	// Get the file path
	path, err := os.Getwd()
	a.NoError(err)

	fullPath := path + "\\" + fileName
	err = common.CreateParentDirectoryIfNotExist(fullPath, tracker)
	a.Nil(err)
}

// Test EINTR errors are not returned on Linux
func TestCreateFileOfSizeWithWriteThroughOption(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("EINTR errors are POSIX specific")
		return
	}
	a := assert.New(t)
	destinationPath := "/"

	plan := &ste.JobPartPlanHeader{}
	fpo := common.EFolderPropertiesOption.AllFolders()
	tracker := ste.NewFolderCreationTracker(fpo, func(index ste.JpptFolderIndex) *ste.JobPartPlanTransfer {
		return plan.Transfer(index.TransferIndex)
	})

	_, err := common.CreateFileOfSizeWithWriteThroughOption(destinationPath, 1,
		false,
		tracker,
		false)

	if err != nil {
		a.NotEqual(syscall.EINTR, err)
		return
	}
	a.NoError(err, fmt.Sprintf("Error creating file: %v", err))

}
