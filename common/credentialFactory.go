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
	"math"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-storage-azcopy/azbfs"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/go-autorest/autorest/adal"
	minio "github.com/minio/minio-go"
	"github.com/minio/minio-go/pkg/credentials"
)

// ==============================================================================================
// credential factories
// ==============================================================================================

// CredentialOpOptions contains credential operations' parameters.
type CredentialOpOptions struct {
	LogInfo  func(string)
	LogError func(string)
	Panic    func(error)
	CallerID string

	// Used to cancel operations, if fatal error happend during operation.
	Cancel context.CancelFunc
}

// callerMessage formats caller message prefix.
func (o CredentialOpOptions) callerMessage() string {
	return IffString(o.CallerID == "", o.CallerID, o.CallerID+" ")
}

// logInfo logs info, if LogInfo is specified in CredentialOpOptions.
func (o CredentialOpOptions) logInfo(str string) {
	if o.LogInfo != nil {
		o.LogInfo(o.callerMessage() + str)
	}
}

// logError logs error, if LogError is specified in CredentialOpOptions.
func (o CredentialOpOptions) logError(str string) {
	if o.LogError != nil {
		o.LogError(o.callerMessage() + str)
	}
}

// panicError uses built-in panic if no Panic is specified in CredentialOpOptions.
func (o CredentialOpOptions) panicError(err error) {
	newErr := fmt.Errorf("%s%v", o.callerMessage(), err)
	if o.Panic == nil {
		panic(newErr)
	} else {
		o.Panic(newErr)
	}
}

func (o CredentialOpOptions) cancel() {
	if o.Cancel != nil {
		o.Cancel()
	} else {
		o.panicError(errors.New("cancel the operations"))
	}
}

// CreateBlobCredential creates Blob credential according to credential info.
func CreateBlobCredential(ctx context.Context, credInfo CredentialInfo, options CredentialOpOptions) azblob.Credential {
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

// refreshPolicyHalfOfExpiryWithin is used for calculating next refresh time,
// it checkes how long it will be before the token get expired, and use half of the value as
// duration to wait.
func refreshPolicyHalfOfExpiryWithin(token *adal.Token, options CredentialOpOptions) time.Duration {
	if token == nil {
		// Invalid state, token should not be nil, cancel the operation and stop refresh
		options.logError("invalid state, token is nil, cancel will be triggered")
		options.cancel()
		return time.Duration(math.MaxInt64)
	}

	waitDuration := token.Expires().Sub(time.Now().UTC()) / 2
	// In case of refresh flooding
	if waitDuration < time.Second {
		waitDuration = time.Second
	}

	if GlobalTestOAuthInjection.DoTokenRefreshInjection {
		waitDuration = GlobalTestOAuthInjection.TokenRefreshDuration
	}

	options.logInfo(fmt.Sprintf("next token refresh's wait duration: %v", waitDuration))

	return waitDuration
}

func refreshBlobToken(ctx context.Context, tokenInfo OAuthTokenInfo, tokenCredential azblob.TokenCredential, options CredentialOpOptions) time.Duration {
	newToken, err := tokenInfo.Refresh(ctx)
	if err != nil {
		// Fail to get new token.
		if _, ok := err.(adal.TokenRefreshError); ok && strings.Contains(err.Error(), "refresh token has expired") {
			options.logError(fmt.Sprintf("failed to refresh token, OAuth refresh token has expired, please log in with azcopy login command again. (Error details: %v)", err))
		} else {
			options.logError(fmt.Sprintf("failed to refresh token, please check error details and try to log in with azcopy login command again. (Error details: %v)", err))
		}
		// Try to refresh again according to original token's info.
		return refreshPolicyHalfOfExpiryWithin(&(tokenInfo.Token), options)
	}

	// Token has been refreshed successfully.
	tokenCredential.SetToken(newToken.AccessToken)
	options.logInfo(fmt.Sprintf("%v token refreshed successfully", time.Now().UTC()))

	// Calculate wait duration, and schedule next refresh.
	return refreshPolicyHalfOfExpiryWithin(newToken, options)
}

// CreateBlobFSCredential creates BlobFS credential according to credential info.
func CreateBlobFSCredential(ctx context.Context, credInfo CredentialInfo, options CredentialOpOptions) azbfs.Credential {
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
			options.panicError(errors.New("ACCOUNT_NAME and ACCOUNT_KEY environment variables must be set before creating the blobfs SharedKey credential"))
		}
		// create the shared key credentials
		return azbfs.NewSharedKeyCredential(name, key)

	default:
		options.panicError(fmt.Errorf("invalid state, credential type %v is not supported", credInfo.CredentialType))
	}

	panic("work around the compiling, logic wouldn't reach here")
}

// CreateS3Credential creates AWS S3 credential according to credential info.
func CreateS3Credential(ctx context.Context, credInfo CredentialInfo, options CredentialOpOptions) (*credentials.Credentials, error) {
	switch credInfo.CredentialType {
	case ECredentialType.S3AccessKey():
		accessKeyID := os.Getenv("AWS_ACCESS_KEY_ID")
		secretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
		sessionToken := os.Getenv("AWS_SESSION_TOKEN")

		if accessKeyID == "" || secretAccessKey == "" {
			return nil, errors.New("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables must be set before creating the S3 AccessKey credential")
		}

		// create and return s3 credential
		return credentials.NewStaticV4(accessKeyID, secretAccessKey, sessionToken), nil // S3 uses V4 signature
	default:
		options.panicError(fmt.Errorf("invalid state, credential type %v is not supported", credInfo.CredentialType))
	}
	panic("work around the compiling, logic wouldn't reach here")
}

func refreshBlobFSToken(ctx context.Context, tokenInfo OAuthTokenInfo, tokenCredential azbfs.TokenCredential, options CredentialOpOptions) time.Duration {
	newToken, err := tokenInfo.Refresh(ctx)
	if err != nil {
		// Fail to get new token.
		if _, ok := err.(adal.TokenRefreshError); ok && strings.Contains(err.Error(), "refresh token has expired") {
			options.logError(fmt.Sprintf("failed to refresh token, OAuth refresh token has expired, please log in with azcopy login command again. (Error details: %v)", err))
		} else {
			options.logError(fmt.Sprintf("failed to refresh token, please check error details and try to log in with azcopy login command again. (Error details: %v)", err))
		}
		// Try to refresh again according to existing token's info.
		return refreshPolicyHalfOfExpiryWithin(&(tokenInfo.Token), options)
	}

	// Token has been refreshed successfully.
	tokenCredential.SetToken(newToken.AccessToken)
	options.logInfo(fmt.Sprintf("%v token refreshed successfully", time.Now().UTC()))

	// Calculate wait duration, and schedule next refresh.
	return refreshPolicyHalfOfExpiryWithin(newToken, options)
}

// ==============================================================================================
// S3 credential related factory methods
// ==============================================================================================
func CreateS3Client(ctx context.Context, credInfo CredentialInfo, option CredentialOpOptions) (*minio.Client, error) {
	// Currently only support access key
	credential, err := CreateS3Credential(ctx, credInfo, option)
	if err != nil {
		return nil, err
	}

	return minio.NewWithCredentials(credInfo.S3CredentialInfo.Endpoint, credential, true, credInfo.S3CredentialInfo.Region)
}

type S3ClientFactory struct {
	s3Clients map[CredentialInfo]*minio.Client
	lock      sync.RWMutex
}

// NewS3ClientFactory creates new S3 client factory.
func NewS3ClientFactory() S3ClientFactory {
	return S3ClientFactory{
		s3Clients: make(map[CredentialInfo]*minio.Client),
	}
}

// GetS3Client gets S3 client from pool, or create a new S3 client if no client created for specific credInfo.
func (f *S3ClientFactory) GetS3Client(ctx context.Context, credInfo CredentialInfo, option CredentialOpOptions) (*minio.Client, error) {
	f.lock.RLock()
	s3Client, ok := f.s3Clients[credInfo]
	f.lock.RUnlock()

	if ok {
		return s3Client, nil
	}

	f.lock.Lock()
	defer f.lock.Unlock()
	if s3Client, ok := f.s3Clients[credInfo]; !ok {
		newS3Client, err := CreateS3Client(ctx, credInfo, option)
		if err != nil {
			return nil, err
		}

		f.s3Clients[credInfo] = newS3Client
		return newS3Client, nil
	} else {
		return s3Client, nil
	}
}
