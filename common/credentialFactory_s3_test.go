package common_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/cred"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"
)

type s3FactoryTestLogger struct{}

func (s3FactoryTestLogger) ShouldLog(common.LogLevel) bool { return false }
func (s3FactoryTestLogger) Log(common.LogLevel, string)    {}
func (s3FactoryTestLogger) Panic(err error)                { panic(err) }

func TestM6S3FactoryReportsTransportErrorWithoutLoggerPanic(t *testing.T) {
	original := minio.DefaultTransport
	t.Cleanup(func() { minio.DefaultTransport = original })
	failure := errors.New("synthetic transport failure")
	minio.DefaultTransport = func(bool) (*http.Transport, error) { return nil, failure }
	t.Setenv("AWS_ACCESS_KEY_ID", "unit-test-access")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "unit-test-secret")
	info := common.CredentialInfo{
		CredentialType:   enum.ECredentialType.S3AccessKey(),
		S3CredentialInfo: cred.S3CredentialInfo{Endpoint: "unit-test.invalid", Region: "us-east-1"},
	}
	client, err := common.CreateS3Client(context.Background(), info, common.CredentialOpOptions{}, s3FactoryTestLogger{})
	require.Nil(t, client)
	require.ErrorIs(t, err, failure)
	require.ErrorContains(t, err, "unit-test.invalid")
}

func TestM6S3FactorySeparatesCredentialTypeAndSession(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "unit-test-access")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "unit-test-secret")
	t.Setenv("AWS_SESSION_TOKEN", "unit-test-session-one")
	factory := common.NewS3ClientFactory()
	info := common.CredentialInfo{
		CredentialType:   enum.ECredentialType.S3PublicBucket(),
		S3CredentialInfo: cred.S3CredentialInfo{Endpoint: "unit-test.invalid", Region: "us-east-1"},
	}
	public, err := factory.GetS3Client(context.Background(), info, common.CredentialOpOptions{}, nil)
	require.NoError(t, err)
	info.CredentialType = enum.ECredentialType.S3AccessKey()
	first, err := factory.GetS3Client(context.Background(), info, common.CredentialOpOptions{}, nil)
	require.NoError(t, err)
	require.NotSame(t, public, first)
	reused, err := factory.GetS3Client(context.Background(), info, common.CredentialOpOptions{}, nil)
	require.NoError(t, err)
	require.Same(t, first, reused)
	t.Setenv("AWS_SESSION_TOKEN", "unit-test-session-two")
	second, err := factory.GetS3Client(context.Background(), info, common.CredentialOpOptions{}, nil)
	require.NoError(t, err)
	require.NotSame(t, first, second)
}

type nonComparableS3Provider []string

func (nonComparableS3Provider) Retrieve() (credentials.Value, error) {
	return credentials.Value{}, errors.New("constructor must not retrieve credentials")
}
func (p nonComparableS3Provider) RetrieveWithCredContext(*credentials.CredContext) (credentials.Value, error) {
	return p.Retrieve()
}
func (nonComparableS3Provider) IsExpired() bool { return false }

func TestM6S3FactoryKeepsInjectedProvidersWithoutEnvironmentFallback(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	factory := common.NewS3ClientFactory()
	info := common.CredentialInfo{
		CredentialType: enum.ECredentialType.S3AccessKey(),
		S3CredentialInfo: cred.S3CredentialInfo{
			Endpoint: "unit-test.invalid", Region: "us-east-1", Provider: nonComparableS3Provider{"provider"},
		},
	}
	client, err := factory.GetS3Client(context.Background(), info, common.CredentialOpOptions{}, nil)
	require.NoError(t, err)
	require.NotNil(t, client)
	firstProvider := &credentials.Static{Value: credentials.Value{AccessKeyID: "one", SecretAccessKey: "fake-one", SignerType: credentials.SignatureV4}}
	secondProvider := &credentials.Static{Value: credentials.Value{AccessKeyID: "two", SecretAccessKey: "fake-two", SignerType: credentials.SignatureV4}}
	info.S3CredentialInfo.Provider = firstProvider
	first, err := factory.GetS3Client(context.Background(), info, common.CredentialOpOptions{}, nil)
	require.NoError(t, err)
	info.S3CredentialInfo.Provider = secondProvider
	second, err := factory.GetS3Client(context.Background(), info, common.CredentialOpOptions{}, nil)
	require.NoError(t, err)
	require.NotSame(t, first, second)
}

func TestM6S3FactoryPreservesProviderSessionAndAddressing(t *testing.T) {
	original := minio.DefaultTransport
	t.Cleanup(func() { minio.DefaultTransport = original })
	dials := 0
	minio.DefaultTransport = func(bool) (*http.Transport, error) {
		return &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) {
			dials++
			return nil, errors.New("network access forbidden in constructor/signing test")
		}}, nil
	}
	t.Setenv("AWS_ACCESS_KEY_ID", "unused-environment-access")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "unused-environment-secret")
	provider := &credentials.Static{Value: credentials.Value{
		AccessKeyID: "provided-access", SecretAccessKey: "provided-secret",
		SessionToken: "provided-session", SignerType: credentials.SignatureV4,
	}}
	for _, scenario := range []struct {
		endpoint string
		path     string
		dns      bool
	}{
		{endpoint: "s3.us-east-1.amazonaws.com", path: "/object", dns: true},
		{endpoint: "namespace.compat.objectstorage.us-ashburn-1.oraclecloud.com", path: "/safe-bucket/object"},
		{endpoint: "namespace.vhcompat.objectstorage.us-ashburn-1.oraclecloud.com", path: "/object", dns: true},
		{endpoint: "oss-cn-hangzhou.aliyuncs.com", path: "/object", dns: true},
		{endpoint: "storage.googleapis.com", path: "/safe-bucket/object"},
	} {
		t.Run(scenario.endpoint, func(t *testing.T) {
			info := common.CredentialInfo{
				CredentialType: enum.ECredentialType.S3AccessKey(),
				S3CredentialInfo: cred.S3CredentialInfo{Endpoint: scenario.endpoint,
					Region: "us-east-1", BucketName: "safe-bucket", Provider: provider},
			}
			client, err := common.CreateS3Client(context.Background(), info, common.CredentialOpOptions{}, nil)
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			signed, err := client.PresignedGetObject(ctx, "safe-bucket", "object", time.Minute, nil)
			require.NoError(t, err)
			require.Equal(t, scenario.path, signed.Path)
			require.Equal(t, scenario.dns, strings.HasPrefix(signed.Host, "safe-bucket."))
			require.Contains(t, signed.Query().Get("X-Amz-Credential"), "provided-access/")
			require.Equal(t, "provided-session", signed.Query().Get("X-Amz-Security-Token"))
		})
	}
	require.Zero(t, dials, "signing with an explicit region must not resolve or contact the endpoint")
}
