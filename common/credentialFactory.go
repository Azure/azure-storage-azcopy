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
	"fmt"
	"sync"

	gcpUtils "cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
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

// panicError uses built-in panic if no Panic is specified in CredentialOpOptions.
func (o CredentialOpOptions) panicError(err error) {
	newErr := fmt.Errorf("%s%v", o.callerMessage(), err)
	if o.Panic == nil {
		panic(newErr)
	} else {
		o.Panic(newErr)
	}
}

// Constants for private network transport
const PeReCheckCooldownTimeInSecs = 3600
const PeCheckRetries = 3
const PeCheckIntervalInmilliSecs = 200

func createS3ClientForPrivateNetwork(credInfo CredentialInfo) (*minio.Client, error) {
	peIP := privateNetworkArgs.PrivateEndpointIPs
	baseS3Host := credInfo.S3CredentialInfo.Endpoint
	// Doublecheck endpoint should contain bucketname : "<bucketname>.s3.<region>.amazonaws.com"
	s3Host := privateNetworkArgs.BucketName + "." + credInfo.S3CredentialInfo.Endpoint
	transport := NewRoundRobinTransport(peIP, s3Host, PeReCheckCooldownTimeInSecs, PeCheckRetries, PeCheckIntervalInmilliSecs)
	// Create MinIO client
	client, err := minio.New(baseS3Host, &minio.Options{
		Creds:        credentials.New(credInfo.S3CredentialInfo.Provider),
		Secure:       true,
		Transport:    transport,
		Region:       credInfo.S3CredentialInfo.Region,
		BucketLookup: minio.BucketLookupDNS, // force virtual-hosted style
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create MinIO client: %v", err)
	}
	client.SetS3EnableDualstack(false)
	return client, nil
}

// CreateS3Credential creates AWS S3 credential according to credential info.
func CreateS3Credential(ctx context.Context, credInfo CredentialInfo, options CredentialOpOptions) (*credentials.Credentials, error) {
	switch credInfo.CredentialType {
	case ECredentialType.S3PublicBucket():
		return credentials.NewStatic("", "", "", credentials.SignatureAnonymous), nil
	case ECredentialType.S3AccessKey():
		accessKeyID := GetEnvironmentVariable(EEnvironmentVariable.AWSAccessKeyID())
		secretAccessKey := GetEnvironmentVariable(EEnvironmentVariable.AWSSecretAccessKey())
		sessionToken := GetEnvironmentVariable(EEnvironmentVariable.AwsSessionToken())

		// create and return s3 credential
		return credentials.NewStaticV4(accessKeyID, secretAccessKey, sessionToken), nil // S3 uses V4 signature
	default:
		options.panicError(fmt.Errorf("invalid state, credential type %v is not supported", credInfo.CredentialType))
	}
	panic("work around the compiling, logic wouldn't reach here")
}

func CreateS3ClientFromProvider(credInfo CredentialInfo) (*minio.Client, error) {
	if IsPrivateNetworkEnabled() {
		fmt.Println("Creating S3 Client for Private Network")
		s3Client, err := createS3ClientForPrivateNetwork(credInfo)
		return s3Client, err
	}
	fmt.Println("Creating S3 Client for public access")
	cred := credentials.New(credInfo.S3CredentialInfo.Provider)
	//s3Client, err := minio.NewWithCredentials(credInfo.S3CredentialInfo.Endpoint, creds, true, credInfo.S3CredentialInfo.Region)
	s3Client, err := minio.New(credInfo.S3CredentialInfo.Endpoint, &minio.Options{Creds: cred, Secure: true, Region: credInfo.S3CredentialInfo.Region})
	return s3Client, err
}

// ==============================================================================================
// S3 credential related factory methods
// ==============================================================================================
func CreateS3Client(ctx context.Context, credInfo CredentialInfo, option CredentialOpOptions, logger ILogger) (*minio.Client, error) {
	if credInfo.CredentialType == ECredentialType.S3PublicBucket() {
		cred := credentials.NewStatic("", "", "", credentials.SignatureAnonymous)
		return minio.New(credInfo.S3CredentialInfo.Endpoint, &minio.Options{Creds: cred, Secure: true, Region: credInfo.S3CredentialInfo.Region})
	}
	//support custom credential provider
	if credInfo.S3CredentialInfo.Provider != nil {
		fmt.Println("Using custom credentials")
		s3Client, err := CreateS3ClientFromProvider(credInfo)
		return s3Client, err
	}
	// Support access key
	credential, err := CreateS3Credential(ctx, credInfo, option)
	if err != nil {
		return nil, err
	}
	s3Client, err := minio.New(credInfo.S3CredentialInfo.Endpoint, &minio.Options{Creds: credential, Secure: true, Region: credInfo.S3CredentialInfo.Region})

	if logger != nil {
		s3Client.TraceOn(NewS3HTTPTraceLogger(logger, LogDebug))
	}
	return s3Client, err
}

type S3ClientFactory struct {
	s3Clients map[S3CredentialInfo]*minio.Client
	lock      sync.RWMutex
}

// NewS3ClientFactory creates new S3 client factory.
func NewS3ClientFactory() S3ClientFactory {
	return S3ClientFactory{
		s3Clients: make(map[S3CredentialInfo]*minio.Client),
	}
}

// GetS3Client gets S3 client from pool, or create a new S3 client if no client created for specific credInfo.
func (f *S3ClientFactory) GetS3Client(ctx context.Context, credInfo CredentialInfo, option CredentialOpOptions, logger ILogger) (*minio.Client, error) {
	f.lock.RLock()
	s3Client, ok := f.s3Clients[credInfo.S3CredentialInfo]
	f.lock.RUnlock()

	if ok {
		return s3Client, nil
	}

	f.lock.Lock()
	defer f.lock.Unlock()
	if s3Client, ok := f.s3Clients[credInfo.S3CredentialInfo]; !ok {
		newS3Client, err := CreateS3Client(ctx, credInfo, option, logger)
		if err != nil {
			return nil, err
		}

		f.s3Clients[credInfo.S3CredentialInfo] = newS3Client
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
	gcpClients map[GCPCredentialInfo]*gcpUtils.Client
	lock       sync.RWMutex
}

func NewGCPClientFactory() GCPClientFactory {
	return GCPClientFactory{
		gcpClients: make(map[GCPCredentialInfo]*gcpUtils.Client),
	}
}

func (f *GCPClientFactory) GetGCPClient(ctx context.Context, credInfo CredentialInfo, option CredentialOpOptions) (*gcpUtils.Client, error) {
	f.lock.RLock()
	gcpClient, ok := f.gcpClients[credInfo.GCPCredentialInfo]
	f.lock.RUnlock()

	if ok {
		return gcpClient, nil
	}
	f.lock.Lock()
	defer f.lock.Unlock()
	if gcpClient, ok := f.gcpClients[credInfo.GCPCredentialInfo]; !ok {
		newGCPClient, err := CreateGCPClient(ctx)
		if err != nil {
			return nil, err
		}
		f.gcpClients[credInfo.GCPCredentialInfo] = newGCPClient
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
	encryptionKey := GetEnvironmentVariable(EEnvironmentVariable.CPKEncryptionKey())
	encryptionKeySHA256 := GetEnvironmentVariable(EEnvironmentVariable.CPKEncryptionKeySHA256())
	encryptionAlgorithmAES256 := blob.EncryptionAlgorithmTypeAES256

	glcm := GetLifecycleMgr()
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
