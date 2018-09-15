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
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/Azure/azure-storage-azcopy/azbfs"
	"github.com/Azure/azure-storage-blob-go/2018-03-28/azblob"
)

// ==============================================================================================
// credential factories
// ==============================================================================================

// CreateCredentialOptions contains optional params for creating credentials
type CreateCredentialOptions struct {
	LogInfo  func(string)
	LogError func(string)
	Panic    func(error)
	CallerID string
}

// callerMessage formats caller message prefix.
func (o CreateCredentialOptions) callerMessage() string {
	return IffString(o.CallerID == "", o.CallerID, o.CallerID+" ")
}

// logInfo logs info, if LogInfo is specified in CreateCredentialOptions.
func (o CreateCredentialOptions) logInfo(str string) {
	if o.LogInfo != nil {
		o.LogInfo(o.callerMessage() + str)
	}
}

// logError logs error, if LogError is specified in CreateCredentialOptions.
func (o CreateCredentialOptions) logError(str string) {
	if o.LogError != nil {
		o.LogError(o.callerMessage() + str)
	}
}

// panicError uses built-in panic if no Panic is specified in CreateCredentialOptions.
func (o CreateCredentialOptions) panicError(err error) {
	newErr := fmt.Errorf("%s%v", o.callerMessage(), err)
	if o.Panic == nil {
		panic(newErr)
	} else {
		o.Panic(newErr)
	}
}

// CreateBlobCredential creates Blob credential according to credential info.
func CreateBlobCredential(ctx context.Context, credInfo CredentialInfo, options CreateCredentialOptions) azblob.Credential {
	credential := azblob.NewAnonymousCredential()

	if credInfo.CredentialType == ECredentialType.OAuthToken() {
		if credInfo.OAuthTokenInfo.IsEmpty() {
			options.panicError(errors.New("invalid state, cannot get valid OAuth token information"))
		}

		// Create TokenCredential with refresher.
		return azblob.NewTokenCredential(
			credInfo.OAuthTokenInfo.AccessToken,
			func(credential azblob.TokenCredential) time.Duration {
				return refreshBlobToken(ctx, credInfo.OAuthTokenInfo, credential, options)
			})
	}

	return credential
}

func refreshBlobToken(ctx context.Context, tokenInfo OAuthTokenInfo, tokenCredential azblob.TokenCredential, options CreateCredentialOptions) time.Duration {
	newToken, err := tokenInfo.Refresh(ctx)
	if err != nil {
		options.logError(fmt.Sprintf("failed to refresh token, due to error: %v", err))
	}
	tokenCredential.SetToken(newToken.AccessToken)

	options.logInfo(fmt.Sprintf("%v token refreshed", time.Now().UTC()))

	// Calculate wait duration, and schedule next refresh.
	waitDuration := newToken.Expires().Sub(time.Now().UTC()) - DefaultTokenExpiryWithinThreshold
	if waitDuration < time.Second {
		waitDuration = time.Nanosecond
	}

	if GlobalTestOAuthInjection.DoTokenRefreshInjection {
		waitDuration = GlobalTestOAuthInjection.TokenRefreshDuration
	}

	return waitDuration
}

// CreateBlobFSCredential creates BlobFS credential according to credential info.
func CreateBlobFSCredential(ctx context.Context, credInfo CredentialInfo, options CreateCredentialOptions) azbfs.Credential {
	switch credInfo.CredentialType {
	case ECredentialType.OAuthToken():
		if credInfo.OAuthTokenInfo.IsEmpty() {
			options.panicError(errors.New("invalid state, cannot get valid OAuth token information"))
		}

		// Create TokenCredential with refresher.
		return azbfs.NewTokenCredential(
			credInfo.OAuthTokenInfo.AccessToken,
			func(credential azbfs.TokenCredential) time.Duration {
				return refreshBlobFSToken(ctx, credInfo.OAuthTokenInfo, credential, options)
			})

	case ECredentialType.SharedKey():
		// Get the Account Name and Key variables from environment
		name := os.Getenv("ACCOUNT_NAME")
		key := os.Getenv("ACCOUNT_KEY")
		// If the ACCOUNT_NAME and ACCOUNT_KEY are not set in environment variables
		if name == "" || key == "" {
			options.panicError(errors.New("ACCOUNT_NAME and ACCOUNT_KEY environment vars must be set before creating the blobfs SharedKey credential"))
		}
		// create the shared key credentials
		return azbfs.NewSharedKeyCredential(name, key)

	default:
		options.panicError(fmt.Errorf("invalid state, credential type %v is not supported", credInfo.CredentialType))
	}

	panic("work around the compiling, logic wouldn't reach here")
}

func refreshBlobFSToken(ctx context.Context, tokenInfo OAuthTokenInfo, tokenCredential azbfs.TokenCredential, options CreateCredentialOptions) time.Duration {
	newToken, err := tokenInfo.Refresh(ctx)
	if err != nil {
		options.logError(fmt.Sprintf("failed to refresh token, due to error: %v", err))
	}
	tokenCredential.SetToken(newToken.AccessToken)

	options.logInfo(fmt.Sprintf("%v token refreshed", time.Now().UTC()))

	// Calculate wait duration, and schedule next refresh.
	waitDuration := newToken.Expires().Sub(time.Now().UTC()) - DefaultTokenExpiryWithinThreshold
	if waitDuration < time.Second {
		waitDuration = time.Nanosecond
	}
	if GlobalTestOAuthInjection.DoTokenRefreshInjection {
		waitDuration = GlobalTestOAuthInjection.TokenRefreshDuration
	}

	return waitDuration
}
