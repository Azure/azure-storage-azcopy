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

package common

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/go-autorest/autorest/adal"
	"github.com/Azure/go-autorest/autorest/date"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type SourceTokenCredential interface {
	Token() string
	setToken(newToken string)
}

func refreshSourceTokenCredential(ctx context.Context, tokenCredential SourceTokenCredential, credInfo CredentialInfo, credOpOptions CredentialOpOptions) time.Duration {
	scope := []string{StorageScope}
	if credInfo.CredentialType == ECredentialType.MDOAuthToken() {
		scope = []string{ManagedDiskScope}
	}
	newToken, err := credInfo.S2SSourceTokenCredential.GetToken(ctx, policy.TokenRequestOptions{Scopes: scope})
	if err != nil {
		// Fail to get new token.
		if _, ok := err.(adal.TokenRefreshError); ok && strings.Contains(err.Error(), "refresh token has expired") {
			credOpOptions.logError(fmt.Sprintf("failed to refresh token, OAuth refresh token has expired, please log in with azcopy login command again. (Error details: %v)", err))
		} else {
			credOpOptions.logError(fmt.Sprintf("failed to refresh token, please check error details and try to log in with azcopy login command again. (Error details: %v)", err))
		}
		// Try to refresh again according to existing token's info.
		return refreshPolicyHalfOfExpiryWithin(&adal.Token{
			AccessToken:  newToken.Token,
			ExpiresOn: json.Number(strconv.FormatInt(int64(newToken.ExpiresOn.Sub(date.UnixEpoch())/time.Second), 10)),
		}, credOpOptions)
	}

	// Token has been refreshed successfully.
	tokenCredential.setToken(newToken.Token)
	credOpOptions.logInfo(fmt.Sprintf("%v token refreshed successfully", time.Now().UTC()))

	// Calculate wait duration, and schedule next refresh.
	return refreshPolicyHalfOfExpiryWithin(&adal.Token{
		AccessToken:  newToken.Token,
		ExpiresOn: json.Number(strconv.FormatInt(int64(newToken.ExpiresOn.Sub(date.UnixEpoch())/time.Second), 10)),
	}, credOpOptions)
}

// NewSourceTokenCredential creates a token credential for use with role-based access control (RBAC) access to Azure Storage
// resources. You initialize the SourceTokenCredential with an initial token value. If you pass a non-nil value for
// tokenRefresher, then the function you pass will be called immediately (so it can refresh and change the
// SourceTokenCredential's token value by calling SetToken; your tokenRefresher function must return a time.Duration
// indicating how long the SourceTokenCredential object should wait before calling your tokenRefresher function again.
func NewSourceTokenCredential(ctx context.Context, credInfo CredentialInfo, credOpOptions CredentialOpOptions) SourceTokenCredential {
	tCred := &tokenCredential{}
	tCred.credInfo = credInfo
	tCred.ctx = ctx
	tCred.credOpOptions = credOpOptions
	scope := []string{StorageScope}
	if credInfo.CredentialType == ECredentialType.MDOAuthToken() {
		scope = []string{ManagedDiskScope}
	}
	newToken, err := credInfo.S2SSourceTokenCredential.GetToken(ctx, policy.TokenRequestOptions{Scopes: scope})
	if err != nil {
		credOpOptions.logError(fmt.Sprintf("Failed to get source token credential. (Error details: %v)", err))
	}
	tCred.setToken(newToken.Token) // We don't set it above to guarantee atomicity

	tCred.startRefresh(refreshSourceTokenCredential)
	runtime.SetFinalizer(tCred, func(deadTC *tokenCredential) {
		deadTC.stopRefresh()
	})
	return tCred
}

// tokenCredential is a pipeline.Factory is the credential's policy factory.
type tokenCredential struct {
	credInfo CredentialInfo
	credOpOptions CredentialOpOptions
	ctx      context.Context
	token atomic.Value

	// The members below are only used if the user specified a tokenRefresher callback function.
	timer          *time.Timer
	tokenRefresher func(ctx context.Context, tokenCredential SourceTokenCredential, credInfo CredentialInfo, credOpOptions CredentialOpOptions) time.Duration
	lock           sync.Mutex
	stopped        bool
}

// Token returns the current token value
func (f *tokenCredential) Token() string { return f.token.Load().(string) }

// SetToken changes the current token value
func (f *tokenCredential) setToken(token string) { f.token.Store(token) }

// startRefresh calls refresh which immediately calls tokenRefresher
// and then starts a timer to call tokenRefresher in the future.
func (f *tokenCredential) startRefresh(tokenRefresher func(ctx context.Context, tokenCredential SourceTokenCredential, credInfo CredentialInfo, credOpOptions CredentialOpOptions) time.Duration) {
	f.tokenRefresher = tokenRefresher
	f.stopped = false // In case user calls StartRefresh, StopRefresh, & then StartRefresh again
	f.refresh()
}

// refresh calls the user's tokenRefresher so they can refresh the token (by
// calling SetToken) and then starts another time (based on the returned duration)
// in order to refresh the token again in the future.
func (f *tokenCredential) refresh() {
	d := f.tokenRefresher(f.ctx, f, f.credInfo, f.credOpOptions) // Invoke the user's refresh callback outside of the lock
	f.lock.Lock()
	if !f.stopped {
		f.timer = time.AfterFunc(d, f.refresh)
	}
	f.lock.Unlock()
}

// stopRefresh stops any pending timer and sets stopped field to true to prevent
// any new timer from starting.
// NOTE: Stopping the timer allows the GC to destroy the tokenCredential object.
func (f *tokenCredential) stopRefresh() {
	f.lock.Lock()
	f.stopped = true
	if f.timer != nil {
		f.timer.Stop()
	}
	f.lock.Unlock()
}