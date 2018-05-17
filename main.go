// Copyright © 2017 Microsoft <wastore@microsoft.com>
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

package main

import (
	"github.com/Azure/azure-storage-azcopy/cmd"
	//"github.com/Azure/azure-storage-azcopy/ste"
	"os"
	//"os/exec"
	//"strconv"
	"github.com/Azure/azure-storage-azcopy/ste"
	//"os/exec"
)

var eexitCode = exitCode(0)
type exitCode int32

func (exitCode) success() exitCode { return exitCode(0) }
func (exitCode) error() exitCode   { return exitCode(-1) }

func main() {
	os.Exit(int(mainWithExitCode()))
}

func mainWithExitCode() exitCode {
	// If insufficient arguments, show usage & terminate
	if len(os.Args) == 1 {
		cmd.Execute()
		return eexitCode.success()
	}
	go cmd.ReadStandardInputToCancelJob(cmd.CancelChannel)
	azcopyAppPathFolder := GetAzCopyAppPath()
	go ste.MainSTE(300, 500, azcopyAppPathFolder)
	cmd.Execute()
	return eexitCode.success()
}

