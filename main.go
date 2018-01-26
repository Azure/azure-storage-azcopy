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
	"github.com/Azure/azure-storage-azcopy/ste"
	"os"
	"os/exec"
)

func main() {
	if len(os.Args) == 1 {
		cmd.Execute()
		return
	}
	switch os.Args[1] {
	case "inproc": // STE is launched in process
		go ste.InitializeSTE()
		cmd.Execute()
	case "ste": // the program is being launched as the STE, the init function runs on main go-routine
		ste.InitializeSTE()
	default:
		// STE is launched as an independent process
		newProcessCommand := exec.Command(os.Args[0], "ste")
		err := newProcessCommand.Start()
		if err != nil {
			panic(err)
			os.Exit(1)
		}
		cmd.Execute()
	}
}
