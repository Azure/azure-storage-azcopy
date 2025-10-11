package common

import (
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// This testsuite does not reach GCP service, and runs even with GCP_TESTS=FALSE

func TestGCPURLParse(t *testing.T) {
	a := assert.New(t)
	u, _ := url.Parse("http://storage.cloud.google.com/bucket")
	p, err := NewGCPURLParts(*u)
	a.Nil(err)
	a.Equal("storage.cloud.google.com", p.Host)
	a.Equal("bucket", p.BucketName)
	a.Equal("", p.ObjectKey)
	a.Equal("http://storage.cloud.google.com/bucket", p.String())

	u, _ = url.Parse("https://storage.cloud.google.com")
	p, err = NewGCPURLParts(*u)
	a.Nil(err)
	a.Equal("", p.BucketName)
	a.Equal("", p.ObjectKey)
	a.Equal("https://storage.cloud.google.com", p.String())

	u, _ = url.Parse("http://storage.cloud.google.com/bucket/keyname/")
	p, err = NewGCPURLParts(*u)
	a.Nil(err)
	a.Equal("bucket", p.BucketName)
	a.Equal("keyname/", p.ObjectKey)
	a.Equal("http://storage.cloud.google.com/bucket/keyname/", p.String())

}

func TestGCPURLParseNegative(t *testing.T) {
	a := assert.New(t)
	u, _ := url.Parse("https://storage.cloud.googly.com/bucket")
	_, err := NewGCPURLParts(*u)
	a.NotNil(err)
	a.True(strings.Contains(err.Error(), invalidGCPURLErrorMessage))

	u, _ = url.Parse("https://mcdheestorage.blob.core.windows.net")
	_, err = NewGCPURLParts(*u)
	a.NotNil(err)
	a.True(strings.Contains(err.Error(), invalidGCPURLErrorMessage))
}

func TestIsGCPURL(t *testing.T) {
	a := assert.New(t)
	u, _ := url.Parse("http://storage.cloud.google.com/bucket/keyname/")
	isGCP := IsGCPURL(*u)
	a.True(isGCP)

	// Negative Test Cases
	u, _ = url.Parse("http://storage.cloudxgoogle.com/bucket/keyname/")
	isGCP = IsGCPURL(*u)
	a.False(isGCP)

	u, _ = url.Parse("http://storage.cloud.googlexcom/bucket/keyname/")
	isGCP = IsGCPURL(*u)
	a.False(isGCP)

	u, _ = url.Parse("http://storagexcloud.google.com/bucket/keyname/")
	isGCP = IsGCPURL(*u)
	a.False(isGCP)
}
