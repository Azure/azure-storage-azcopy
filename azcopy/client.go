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

const (
	oauthLoginSessionCacheKeyName     = "AzCopyOAuthTokenCache"
	oauthLoginSessionCacheServiceName = "AzCopyV10"
	oauthLoginSessionCacheAccountName = "AzCopyOAuthTokenCache"
)

const (
	// Base10Mega For networking throughput in Mbps, (and only for networking), we divide by 1000*1000 (not 1024 * 1024) because
	// networking is traditionally done in base 10 units (not base 2).
	// E.g. "gigabit ethernet" means 10^9 bits/sec, not 2^30. So by using base 10 units
	// we give the best correspondence to the sizing of the user's network pipes.
	// See https://networkengineering.stackexchange.com/questions/3628/iec-or-si-units-binary-prefixes-used-for-network-measurement
	// NOTE that for everything else in the app (e.g. sizes of files) we use the base 2 units (i.e. 1024 * 1024) because
	// for RAM and disk file sizes, it is conventional to use the power-of-two-based units.
	Base10Mega = 1000 * 1000
)

// It's not pretty that this one is read directly by credential util.
// But doing otherwise required us passing it around in many places, even though really
// it can be thought of as an "ambient" property. That's the (weak?) justification for implementing
// it as a global
var TrustedSuffixes string

type Client struct {
	CurrentJobID      common.JobID                  // TODO (gapra): In future this should only be set when there is a current job running. On complete, this should be cleared. It can also behave as something we can check to see if a current job is running
	oauthTokenManager *common.UserOAuthTokenManager // OAuth token manager for the current user, used for authentication
	logLevel          common.LogLevel
}

type ClientOptions struct {
	CapMbps         float64
	TrustedSuffixes string
	LogLevel        *common.LogLevel
}

func NewClient(opts ClientOptions) (Client, error) {
	c := Client{
		logLevel: common.IffNil(opts.LogLevel, common.ELogLevel.Info()), // Default: Info
	}
	TrustedSuffixes = opts.TrustedSuffixes
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
	// only one UserOAuthTokenManager should exist in azcopy process for current user.
	// (a given AzcopyJobPlanFolder is mapped to current user)
	if common.AzcopyJobPlanFolder == "" {
		panic("invalid state, AzcopyJobPlanFolder should not be an empty string")
	}
	cacheName := common.GetEnvironmentVariable(common.EEnvironmentVariable.LoginCacheName())

	c.oauthTokenManager = common.NewUserOAuthTokenManagerInstance(common.CredCacheOptions{
		DPAPIFilePath: common.AzcopyJobPlanFolder,
		KeyName:       common.Iff(cacheName != "", cacheName, oauthLoginSessionCacheKeyName),
		ServiceName:   oauthLoginSessionCacheServiceName,
		AccountName:   common.Iff(cacheName != "", cacheName, oauthLoginSessionCacheAccountName),
	})
	return c, nil
}

// GetUserOAuthTokenManagerInstance gets or creates OAuthTokenManager for current user.
// Note: Currently, only support to have TokenManager for one user mapping to one tenantID.
func (c *Client) GetUserOAuthTokenManagerInstance() *common.UserOAuthTokenManager {
	return c.oauthTokenManager
}

func (c *Client) GetLogLevel() common.LogLevel {
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

type JobContext struct {
	JobID   common.JobID
	LogPath string
}
