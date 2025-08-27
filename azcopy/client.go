// Copyright © 2025 Microsoft <wastore@microsoft.com>
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

package azcopy

import (
	"log"
	"runtime"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
)

type Client struct {
	CurrentJobID common.JobID // TODO (gapra): In future this should only be set when there is a current job running. On complete, this should be cleared. It can also behave as something we can check to see if a current job is running
	logLevel     common.LogLevel
}

type ClientOptions struct {
	CapMbps float64
}

func NewClient(opts ClientOptions) (Client, error) {
	c := Client{
		logLevel: common.ELogLevel.Info(), // Default: Info
	}
	common.InitializeFolders()
	configureGoMaxProcs()
	// Perform os specific initialization
	azcopyMaxFileAndSocketHandles, err := processOSSpecificInitialization()
	if err != nil {
		log.Fatalf("initialization failed: %v", err)
	}
	// startup of the STE happens here, so that the startup can access the values of command line parameters that are defined for "root" command
	concurrencySettings := ste.NewConcurrencySettings(azcopyMaxFileAndSocketHandles)
	err = jobsAdmin.MainSTE(concurrencySettings, opts.CapMbps)
	if err != nil {
		return c, err
	}
	return c, nil
}

func (c *Client) SetLogLevel(level *common.LogLevel) {
	if level == nil {
		c.logLevel = common.ELogLevel.Info() // Default to Info if no level is provided
	} else {
		c.logLevel = *level
	}
}

func (c Client) GetLogLevel() common.LogLevel {
	return c.logLevel
}

// Ensure we always have more than 1 OS thread running goroutines, since there are issues with having just 1.
// (E.g. version check doesn't happen at login time, if have only one go proc. Not sure why that happens if have only one
// proc. Is presumably due to the high CPU usage we see on login if only 1 CPU, even tho can't see any busy-wait in that code)
func configureGoMaxProcs() {
	isOnlyOne := runtime.GOMAXPROCS(0) == 1
	if isOnlyOne {
		runtime.GOMAXPROCS(2)
	}
}
