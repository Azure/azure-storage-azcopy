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
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"

	gcpUtils "cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-storage-azcopy/v10/common/cred"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/Azure/azure-storage-azcopy/v10/common/ternary"

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
	return ternary.Iff(o.CallerID == "", o.CallerID, o.CallerID+" ")
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
const PeReCheckCooldownTimeInSecs = 600 // 10 minutes - time to wait before rechecking an unhealthy private endpoint
const PeCheckRetries = 3
const PeCheckIntervalInmilliSecs = 200

func getS3BucketLookup(endpoint string) minio.BucketLookupType {
	urlParts := S3URLParts{Endpoint: endpoint}
	switch urlParts.ProviderKind() {
	case S3ProviderOracle:
		if urlParts.IsOracleCloudStorageVirtualHosted() {
			return minio.BucketLookupDNS
		}
		return minio.BucketLookupPath
	case S3ProviderAlibaba:
		// Alibaba OSS requires virtual-hosted style.
		return minio.BucketLookupDNS
	case S3ProviderGoogle, S3ProviderIBM, S3ProviderOnPrem:
		return minio.BucketLookupPath
	default:
		// Default behavior for AWS S3 endpoints.
		return minio.BucketLookupDNS
	}
}

func createS3ClientForPrivateNetwork(credInfo CredentialInfo, cred *credentials.Credentials) (*minio.Client, error) {
	peIP := privateNetworkArgs.PrivateEndpointIPs
	baseS3Host := credInfo.S3CredentialInfo.Endpoint

	urlParts := S3URLParts{Endpoint: baseS3Host}
	isGCS := urlParts.IsGoogleCloudStorage()
	isS3CompatibleUrl := urlParts.IsS3CompatibleEndpoint()

	var s3Host string
	var tlsHost string          // hostname used for TLS ServerName verification
	minioEndpoint := baseS3Host // endpoint passed to minio.New()
	bucketLookup := getS3BucketLookup(baseS3Host)
	if isS3CompatibleUrl {
		// S3-compatible endpoint (GCS, OCI, or on-prem appliances).
		if bucketLookup == minio.BucketLookupDNS {
			bucketName := credInfo.S3CredentialInfo.BucketName
			if bucketName == "" {
				return nil, fmt.Errorf("bucket name is required for S3-compatible DNS-style endpoint: %s", baseS3Host)
			}
			s3Host = bucketName + "." + baseS3Host
			tlsHost = s3Host
		} else {
			tlsHost = baseS3Host
			s3Host = baseS3Host
		}

		if isGCS {
			// Minio lib only supports "storage.googleapis.com" as the GCS endpoint
			// (it has an internal check for that exact host), so override for regional/PSC
			// endpoints like storage.us-west2.rep.googleapis.com or storage-xyz.p.googleapis.com.
			// TLS and RoundRobinTransport still use the actual baseS3Host.
			s3Host = "storage.googleapis.com"
			minioEndpoint = s3Host
		}
	} else {
		// AWS S3 uses virtual-hosted style: "<bucketname>.s3.<region>.amazonaws.com"
		s3Host = privateNetworkArgs.BucketName + "." + credInfo.S3CredentialInfo.Endpoint
		// AWS certs support *.s3.<region>.amazonaws.com, so use s3Host for TLS
		tlsHost = s3Host
	}
	// Force HTTP/1.1 only for GCS non-global endpoints that send non-standard HTTP/2 frames
	forceHTTP11 := isGCS && baseS3Host != "storage.googleapis.com"
	transport := NewRoundRobinTransport(peIP, s3Host, tlsHost, PeReCheckCooldownTimeInSecs, PeCheckRetries, PeCheckIntervalInmilliSecs, forceHTTP11)
	var minioCred *credentials.Credentials
	if cred != nil {
		minioCred = cred
	} else {
		minioCred = credentials.New(credInfo.S3CredentialInfo.Provider)
	}

	// Create MinIO client
	client, err := minio.New(minioEndpoint, &minio.Options{
		Creds:        minioCred,
		Secure:       true,
		Transport:    transport,
		Region:       credInfo.S3CredentialInfo.Region,
		BucketLookup: bucketLookup,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create MinIO client for private endpoint %q: %w", minioEndpoint, err)
	}
	client.SetS3EnableDualstack(false)
	return client, nil
}

// CreateS3Credential creates AWS S3 credential according to credential info.
func CreateS3Credential(ctx context.Context, credInfo CredentialInfo, options CredentialOpOptions) (*credentials.Credentials, error) {
	switch credInfo.CredentialType {
	case enum.ECredentialType.S3PublicBucket():
		return credentials.NewStatic("", "", "", credentials.SignatureAnonymous), nil
	case enum.ECredentialType.S3AccessKey():
		accessKeyID := enum.EEnvironmentVariable.AWSAccessKeyID().Get()
		secretAccessKey := enum.EEnvironmentVariable.AWSSecretAccessKey().Get()
		sessionToken := enum.EEnvironmentVariable.AwsSessionToken().Get()

		// create and return s3 credential
		return credentials.NewStaticV4(accessKeyID, secretAccessKey, sessionToken), nil // S3 uses V4 signature
	default:
		options.panicError(fmt.Errorf("invalid state, credential type %v is not supported", credInfo.CredentialType))
	}
	panic("work around the compiling, logic wouldn't reach here")
}

func CreateS3ClientFromProvider(credInfo CredentialInfo) (*minio.Client, error) {
	if IsPrivateNetworkTransfer(ELocation.S3()) {
		fmt.Println("Creating S3 Client for Private Network")
		s3Client, err := createS3ClientForPrivateNetwork(credInfo, nil)
		return s3Client, err
	}
	fmt.Println("Creating S3 Client for public access")
	cred := credentials.New(credInfo.S3CredentialInfo.Provider)
	bucketLookup := getS3BucketLookup(credInfo.S3CredentialInfo.Endpoint)
	s3Client, err := minio.New(credInfo.S3CredentialInfo.Endpoint, &minio.Options{Creds: cred, Secure: true, Region: credInfo.S3CredentialInfo.Region, BucketLookup: bucketLookup})
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 client for endpoint %q: %w", credInfo.S3CredentialInfo.Endpoint, err)
	}
	return s3Client, err
}

// ==============================================================================================
// S3 credential related factory methods
// ==============================================================================================
func CreateS3Client(ctx context.Context, credInfo CredentialInfo, option CredentialOpOptions, logger ILogger) (*minio.Client, error) {
	bucketLookup := getS3BucketLookup(credInfo.S3CredentialInfo.Endpoint)

	if credInfo.CredentialType == enum.ECredentialType.S3PublicBucket() {
		cred := credentials.NewStatic("", "", "", credentials.SignatureAnonymous)
		client, err := minio.New(credInfo.S3CredentialInfo.Endpoint, &minio.Options{Creds: cred, Secure: true, Region: credInfo.S3CredentialInfo.Region, BucketLookup: bucketLookup})
		if err != nil {
			return nil, fmt.Errorf("failed to create anonymous S3 client for endpoint %q: %w", credInfo.S3CredentialInfo.Endpoint, err)
		}
		return client, nil
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
	if IsPrivateNetworkTransfer(ELocation.S3()) {
		fmt.Println("Creating S3 Client for Private Network")
		s3Client, err := createS3ClientForPrivateNetwork(credInfo, credential)
		return s3Client, err
	}
	s3Client, err := minio.New(credInfo.S3CredentialInfo.Endpoint, &minio.Options{Creds: credential, Secure: true, Region: credInfo.S3CredentialInfo.Region, BucketLookup: bucketLookup})
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 client for endpoint %q: %w", credInfo.S3CredentialInfo.Endpoint, err)
	}

	if logger != nil {
		s3Client.TraceOn(NewS3HTTPTraceLogger(logger, LogDebug))
	}
	return s3Client, err
}

type S3ClientFactory struct {
	s3Clients map[s3ClientCacheKey]*minio.Client
	lock      sync.RWMutex
}

type s3ClientCacheKey struct {
	info                cred.S3CredentialInfo
	credentialType      enum.CredentialType
	environmentIdentity [sha256.Size]byte
}

func s3CacheKey(info CredentialInfo) (s3ClientCacheKey, bool) {
	key := s3ClientCacheKey{info: info.S3CredentialInfo, credentialType: info.CredentialType}
	if provider := info.S3CredentialInfo.Provider; provider != nil {
		// Provider implementations may contain slices/maps. Never use those as map
		// keys or fall back to environment credentials when they are supplied.
		return key, reflect.TypeOf(provider).Comparable()
	}
	if info.CredentialType == enum.ECredentialType.S3AccessKey() {
		key.environmentIdentity = sha256.Sum256([]byte(strings.Join([]string{
			enum.EEnvironmentVariable.AWSAccessKeyID().Get(),
			enum.EEnvironmentVariable.AWSSecretAccessKey().Get(),
			enum.EEnvironmentVariable.AwsSessionToken().Get(),
		}, "\x00")))
	}
	return key, true
}

// NewS3ClientFactory creates new S3 client factory.
func NewS3ClientFactory() S3ClientFactory {
	return S3ClientFactory{
		s3Clients: make(map[s3ClientCacheKey]*minio.Client),
	}
}

// GetS3Client gets S3 client from pool, or create a new S3 client if no client created for specific credInfo.
func (f *S3ClientFactory) GetS3Client(ctx context.Context, credInfo CredentialInfo, option CredentialOpOptions, logger ILogger) (*minio.Client, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key, cacheable := s3CacheKey(credInfo)
	if !cacheable {
		return CreateS3Client(ctx, credInfo, option, logger)
	}
	f.lock.RLock()
	s3Client, ok := f.s3Clients[key]
	f.lock.RUnlock()

	if ok {
		return s3Client, nil
	}

	f.lock.Lock()
	defer f.lock.Unlock()
	if s3Client, ok := f.s3Clients[key]; !ok {
		newS3Client, err := CreateS3Client(ctx, credInfo, option, logger)
		if err != nil {
			return nil, err
		}

		if f.s3Clients == nil {
			f.s3Clients = make(map[s3ClientCacheKey]*minio.Client)
		}
		f.s3Clients[key] = newS3Client
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
	gcpClients map[cred.GCPCredentialInfo]*gcpUtils.Client
	lock       sync.RWMutex
}

func NewGCPClientFactory() GCPClientFactory {
	return GCPClientFactory{
		gcpClients: make(map[cred.GCPCredentialInfo]*gcpUtils.Client),
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

func GetCpkInfo(cpkInfo bool) (*blob.CPKInfo, error) {
	if !cpkInfo {
		return nil, nil
	}

	// fetch EncryptionKey and EncryptionKeySHA256 from the environment variables
	encryptionKey := enum.EEnvironmentVariable.CPKEncryptionKey().Get()
	encryptionKeySHA256 := enum.EEnvironmentVariable.CPKEncryptionKeySHA256().Get()
	encryptionAlgorithmAES256 := blob.EncryptionAlgorithmTypeAES256

	if encryptionKey == "" || encryptionKeySHA256 == "" {
		return nil, errors.New("fatal: failed to fetch cpk encryption key (" + enum.EEnvironmentVariable.CPKEncryptionKey().Name +
			") or hash (" + enum.EEnvironmentVariable.CPKEncryptionKeySHA256().Name + ") from environment variables")
	}

	return &blob.CPKInfo{
		EncryptionKey:       &encryptionKey,
		EncryptionKeySHA256: &encryptionKeySHA256,
		EncryptionAlgorithm: &encryptionAlgorithmAES256,
	}, nil
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
