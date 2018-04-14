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
	"github.com/Azure/azure-storage-azcopy/common"
	//"github.com/Azure/azure-storage-azcopy/ste"
	"os"
	//"os/exec"
	//"strconv"
	"github.com/Azure/azure-storage-azcopy/ste"
	//"os/exec"
)

type exitCode common.EnumInt32

func (exitCode) success() exitCode { return exitCode{Value: 0} }
func (exitCode) error() exitCode   { return exitCode{Value: -1} }

func main() {
	os.Exit(int(mainWithExitCode().Value))
}

func mainWithExitCode() exitCode {
	// If insufficient arguments, show usage & terminate
	if len(os.Args) == 1 {
		cmd.Execute()
		return exitCode{}.success()
	}
	azcopyAppPathFolder := GetAzCopyAppPath()
	go ste.MainSTE(100, 150, azcopyAppPathFolder)
	cmd.Execute()
	return exitCode{}.success()
	//
	//common.Rpc = common.NewHttpClient("http://localhost:1337").Send
	//switch os.Args[1] {
	//case "inproc": // STE is launched in process
	//	go ste.InitializeSTE(100, 500)
	//	cmd.Execute()
	//case "ste": // the program is being launched as the STE, the init function runs on main go-routine
	//	if len(os.Args) == 4 {
	//		numOfEngineWorker, err := strconv.Atoi(os.Args[2])
	//		if err != nil {
	//			panic("Cannot parse number of engine workers, please give a positive integer.")
	//		}
	//
	//		targetRateInMBps, err := strconv.Atoi(os.Args[3])
	//		if err != nil {
	//			panic("Cannot parse target rate in MB/s, please give a positive integer.")
	//		}
	//
	//		ste.InitializeSTE(numOfEngineWorker, targetRateInMBps)
	//	} else if len(os.Args) == 2 {
	//		// use default number of engine worker and target rate to initialize ste
	//		ste.InitializeSTE(100, 500)
	//	} else {
	//		panic("Wrong number of arguments for STE mode! Please contact your developer.")
	//	}
	//default:
	//	//STE is launched as an independent process
	//	//args := append(os.Args, "ste")
	//	//args = append(os.Args, "--detached")
	//	newProcessCommand := exec.Command(os.Args[0], "ste")
	//	// On Windows, osModifyProcessCommand add syscall.CREATE_NEW_PROCESS_GROUP; on other OSes, it's a no-op
	//	err := osModifyProcessCommand(newProcessCommand).Start()
	//	if err != nil {
	//		panic(err)
	//		os.Exit(1)
	//	}
	//	cmd.Execute()
	//}
	////ste.InitializeSTE()
}
