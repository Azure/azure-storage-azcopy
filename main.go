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
	"fmt"
	"os"
	"strconv"

	"github.com/Azure/azure-storage-azcopy/cmd"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
)

// get the lifecycle manager to print messages
var glcm = common.GetLifecycleMgr()

func main() {
	azcopyAppPathFolder := GetAzCopyAppPath()
	// If insufficient arguments, show usage & terminate
	if len(os.Args) == 1 {
		cmd.Execute(azcopyAppPathFolder)
		return
	}

	// Perform os specific initialization
	_, err := ProcessOSSpecificInitialization()
	if err != nil {
		panic(err)
	}

	/*  AZCOPY_CONCURRENCY_VALUE, and replaced with three separate params
	// Get the value of environment variable AZCOPY_CONCURRENCY_VALUE
	// If the environment variable is set, it defines the number of concurrent connections
	// transfer engine will spawn. If not set, transfer engine will spawn the default number
	// of concurrent connections
	defaultConcurrentConnections := 500  // increased from 300, with the addition of sendRateLimiter
	                                     // The difference between this number and sendRateLimiter's cap
	                                     // equals the number of connections that can be awaiting
	                                     // a reply from the server. (sendRateLimiter caps the number
	                                     // that can actively be sending).*/
	concurrencyValue := os.Getenv("AZCOPY_CONCURRENCY_VALUE")
	if concurrencyValue != "" {
		fmt.Printf("\n")
		fmt.Printf("**** Warning AZCOPY_CONCURRENCY_VALUE is replaced by %s, %s and %s ***\n",
			ste.AZCOPY_CONCURRENT_SEND_COUNT_NAME,
			ste.AZCOPY_CONCURRENT_WAIT_COUNT_NAME,
			ste.AZCOPY_CONCURRENT_FILE_READ_COUNT_NAME)
 		fmt.Printf("\n")
	}

	concurrencyParams := ste.ConcurrencyParams{}
	initConcurrencyValue(&concurrencyParams.ConcurrentSendCount, ste.AZCOPY_CONCURRENT_SEND_COUNT_NAME, ste.DefaultConcurrentSendCount)
	initConcurrencyValue(&concurrencyParams.ConcurrentWaitCount, ste.AZCOPY_CONCURRENT_WAIT_COUNT_NAME, ste.DefaultConcurrentWaitCount)
	initConcurrencyValue(&concurrencyParams.ConcurrentFileReadCount,       ste.AZCOPY_CONCURRENT_FILE_READ_COUNT_NAME,       ste.DefaultFileReadCount)
	concurrencyParams.Dump()

	ste.MainSTE(concurrencyParams, 2400, azcopyAppPathFolder)

	cmd.Execute(azcopyAppPathFolder)
	glcm.Exit("", common.EExitCode.Success())
}

func initConcurrencyValue(value *int, name string, defaultValue int) {
	stringValue := os.Getenv(name)
	if stringValue == "" {
		*value = defaultValue
	} else {
		val, err := strconv.ParseInt(stringValue, 10, 32)
		if err != nil {
			panic(fmt.Sprintf("error parsing the env variable named %s with value %v. " +
				  "Failed with error %s", name, stringValue, err.Error()))
		}
		*value = int(val)
	}
}
