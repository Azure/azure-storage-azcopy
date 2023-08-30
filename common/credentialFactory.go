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
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"math"
	"strings"
	"sync"
	"time"

	gcpUtils "cloud.google.com/go/storage"

	"github.com/Azure/azure-storage-azcopy/v10/azbfs"
	"github.com/Azure/go-autorest/autorest/adal"
	"github.com/minio/minio-go"
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

	// Used to cancel operations, if fatal error happened during operation.
	Cancel context.CancelFunc
}

// callerMessage formats caller message prefix.
func (o CredentialOpOptions) callerMessage() string {
	return Iff(o.CallerID == "", o.CallerID, o.CallerID+" ")
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

// GetSourceBlobCredential gets the TokenCredential based on the cred info
func GetSourceBlobCredential(credInfo CredentialInfo, options CredentialOpOptions) (azcore.TokenCredential, error) {
	if credInfo.CredentialType.IsAzureOAuth() {
		if credInfo.OAuthTokenInfo.IsEmpty() {
			options.panicError(errors.New("invalid state, cannot get valid OAuth token information"))
		}
		if credInfo.S2SSourceTokenCredential != nil {
			return credInfo.S2SSourceTokenCredential, nil
		} else {
			return credInfo.OAuthTokenInfo.GetTokenCredential()
		}
	}
	return nil, nil
}

// refreshPolicyHalfOfExpiryWithin is used for calculating next refresh time,
// it checks how long it will be before the token get expired, and use half of the value as
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

// CreateBlobFSCredential creates BlobFS credential according to credential info.
func CreateBlobFSCredential(ctx context.Context, credInfo CredentialInfo, options CredentialOpOptions) azbfs.Credential {
	cred := azbfs.NewAnonymousCredential()

	switch credInfo.CredentialType {
	case ECredentialType.OAuthToken():
		if credInfo.OAuthTokenInfo.IsEmpty() {
			options.panicError(errors.New("invalid state, cannot get valid OAuth token information"))
		}

		// Create TokenCredential with refresher.
		cred = azbfs.NewTokenCredential(
			credInfo.OAuthTokenInfo.AccessToken,
			func(credential azbfs.TokenCredential) time.Duration {
				return refreshBlobFSToken(ctx, credInfo.OAuthTokenInfo, credential, options)
			})

	case ECredentialType.SharedKey():
		// Get the Account Name and Key variables from environment
		name := lcm.GetEnvironmentVariable(EEnvironmentVariable.AccountName())
		key := lcm.GetEnvironmentVariable(EEnvironmentVariable.AccountKey())
		// If the ACCOUNT_NAME and ACCOUNT_KEY are not set in environment variables
		if name == "" || key == "" {
			options.panicError(errors.New("ACCOUNT_NAME and ACCOUNT_KEY environment variables must be set before creating the blobfs SharedKey credential"))
		}
		// create the shared key credentials
		cred = azbfs.NewSharedKeyCredential(name, key)

	case ECredentialType.Anonymous():
		// do nothing

	default:
		options.panicError(fmt.Errorf("invalid state, credential type %v is not supported", credInfo.CredentialType))
	}

	return cred
}

// CreateS3Credential creates AWS S3 credential according to credential info.
func CreateS3Credential(ctx context.Context, credInfo CredentialInfo, options CredentialOpOptions) (*credentials.Credentials, error) {
	glcm := GetLifecycleMgr()
	switch credInfo.CredentialType {
	case ECredentialType.S3PublicBucket():
		return credentials.NewStatic("", "", "", credentials.SignatureAnonymous), nil
	case ECredentialType.S3AccessKey():
		accessKeyID := glcm.GetEnvironmentVariable(EEnvironmentVariable.AWSAccessKeyID())
		secretAccessKey := glcm.GetEnvironmentVariable(EEnvironmentVariable.AWSSecretAccessKey())
		sessionToken := glcm.GetEnvironmentVariable(EEnvironmentVariable.AwsSessionToken())

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
func CreateS3Client(ctx context.Context, credInfo CredentialInfo, option CredentialOpOptions, logger ILogger) (*minio.Client, error) {
	if credInfo.CredentialType == ECredentialType.S3PublicBucket() {
		cred := credentials.NewStatic("", "", "", credentials.SignatureAnonymous)
		return minio.NewWithOptions(credInfo.S3CredentialInfo.Endpoint, &minio.Options{Creds: cred, Secure: true, Region: credInfo.S3CredentialInfo.Region})
	}
	// Support access key
	credential, err := CreateS3Credential(ctx, credInfo, option)
	if err != nil {
		return nil, err
	}
	s3Client, err := minio.NewWithCredentials(credInfo.S3CredentialInfo.Endpoint, credential, true, credInfo.S3CredentialInfo.Region)

	if logger != nil {
		s3Client.TraceOn(NewS3HTTPTraceLogger(logger, LogDebug))
	}
	return s3Client, err
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
func (f *S3ClientFactory) GetS3Client(ctx context.Context, credInfo CredentialInfo, option CredentialOpOptions, logger ILogger) (*minio.Client, error) {
	f.lock.RLock()
	s3Client, ok := f.s3Clients[credInfo]
	f.lock.RUnlock()

	if ok {
		return s3Client, nil
	}

	f.lock.Lock()
	defer f.lock.Unlock()
	if s3Client, ok := f.s3Clients[credInfo]; !ok {
		newS3Client, err := CreateS3Client(ctx, credInfo, option, logger)
		if err != nil {
			return nil, err
		}

		f.s3Clients[credInfo] = newS3Client
		return newS3Client, nil
	} else {
		return s3Client, nil
	}
}

// ====================================================================
// GCP credential factory related methods
// ====================================================================
func CreateGCPClient(ctx context.Context) (*gcpUtils.Client, error) {
	client, err := gcpUtils.NewClient(ctx)
	return client, err
}

type GCPClientFactory struct {
	gcpClients map[CredentialInfo]*gcpUtils.Client
	lock       sync.RWMutex
}

func NewGCPClientFactory() GCPClientFactory {
	return GCPClientFactory{
		gcpClients: make(map[CredentialInfo]*gcpUtils.Client),
	}
}

func (f *GCPClientFactory) GetGCPClient(ctx context.Context, credInfo CredentialInfo, option CredentialOpOptions) (*gcpUtils.Client, error) {
	f.lock.RLock()
	gcpClient, ok := f.gcpClients[credInfo]
	f.lock.RUnlock()

	if ok {
		return gcpClient, nil
	}
	f.lock.Lock()
	defer f.lock.Unlock()
	if gcpClient, ok := f.gcpClients[credInfo]; !ok {
		newGCPClient, err := CreateGCPClient(ctx)
		if err != nil {
			return nil, err
		}
		f.gcpClients[credInfo] = newGCPClient
		return newGCPClient, nil
	} else {
		return gcpClient, nil
	}
}

func GetCpkInfo(cpkInfo bool) *blob.CPKInfo {
	if !cpkInfo {
		return nil
	}

	// fetch EncryptionKey and EncryptionKeySHA256 from the environment variables
	glcm := GetLifecycleMgr()
	encryptionKey := glcm.GetEnvironmentVariable(EEnvironmentVariable.CPKEncryptionKey())
	encryptionKeySHA256 := glcm.GetEnvironmentVariable(EEnvironmentVariable.CPKEncryptionKeySHA256())
	encryptionAlgorithmAES256 := blob.EncryptionAlgorithmTypeAES256

	if encryptionKey == "" || encryptionKeySHA256 == "" {
		glcm.Error("fatal: failed to fetch cpk encryption key (" + EEnvironmentVariable.CPKEncryptionKey().Name +
			") or hash (" + EEnvironmentVariable.CPKEncryptionKeySHA256().Name + ") from environment variables")
	}

	return &blob.CPKInfo{
		EncryptionKey:       &encryptionKey,
		EncryptionKeySHA256: &encryptionKeySHA256,
		EncryptionAlgorithm: &encryptionAlgorithmAES256,
	}
}

func GetCpkScopeInfo(cpkScopeInfo string) *blob.CPKScopeInfo {
	if cpkScopeInfo == "" {
		return nil
	} else {
		return &blob.CPKScopeInfo{
			EncryptionScope: &cpkScopeInfo,
		}
	}
}
