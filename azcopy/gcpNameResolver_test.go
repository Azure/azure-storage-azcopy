package azcopy

import (
	"strings"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
)

func TestGCPBucketNameToAzureResourceResolverBucketName(t *testing.T) {
	a := assert.New(t)
	r := NewGCPBucketNameToAzureResourcesResolver([]string{"bucket.name.1"})
	resolvedName, err := r.ResolveName("bucket.name.1")
	a.Nil(err)
	a.Equal("bucket-name-1", resolvedName)

	r = NewGCPBucketNameToAzureResourcesResolver([]string{"bucket-name"})
	resolvedName, err = r.ResolveName("bucket-name")
	a.Nil(err)
	a.Equal("bucket-name", resolvedName)

	r = NewGCPBucketNameToAzureResourcesResolver([]string{"bucket--name"})
	resolvedName, err = r.ResolveName("bucket--name")
	a.Nil(err)
	a.Equal("bucket-2-name", resolvedName)

	r = NewGCPBucketNameToAzureResourcesResolver([]string{"bucketvalidname"})
	resolvedName, err = r.ResolveName("bucketvalidname")
	a.Nil(err)
	a.Equal("bucketvalidname", resolvedName)

	r = NewGCPBucketNameToAzureResourcesResolver([]string{"0123456789.0123456789.0123456789.012345678901234567890123456789"})
	resolvedName, err = r.ResolveName("0123456789.0123456789.0123456789.012345678901234567890123456789")
	a.Nil(err)
	a.Equal("0123456789-0123456789-0123456789-012345678901234567890123456789", resolvedName)

	r = NewGCPBucketNameToAzureResourcesResolver([]string{"0123456789--01234567890123456789012345678901234567890123456789"})
	resolvedName, err = r.ResolveName("0123456789--01234567890123456789012345678901234567890123456789")
	a.Nil(err)
	a.Equal("0123456789-2-01234567890123456789012345678901234567890123456789", resolvedName)

	r = NewGCPBucketNameToAzureResourcesResolver([]string{"bucket_name_1"})
	resolvedName, err = r.ResolveName("bucket_name_1")
	a.Nil(err)
	a.Equal("bucket-name-1", resolvedName)

	r = NewGCPBucketNameToAzureResourcesResolver([]string{"bucket__name"})
	resolvedName, err = r.ResolveName("bucket__name")
	a.Nil(err)
	a.Equal("bucket-2-name", resolvedName)

	r = NewGCPBucketNameToAzureResourcesResolver([]string{"bucket-_name"})
	resolvedName, err = r.ResolveName("bucket-_name")
	a.Nil(err)
	a.Equal("bucket-2-name", resolvedName)
}

func TestGCPBucketNameToAzureResourceResolverMultipleBucketNames(t *testing.T) {
	a := assert.New(t)
	r := NewGCPBucketNameToAzureResourcesResolver(
		[]string{"bucket.name", "bucket-name", "bucket-name-2", "bucket-name-3",
			"bucket---name", "bucket-s--s---s", "abcdefghijklmnopqrstuvwxyz-s--s---s-s0123456789",
			"bucket--name", "bucket-2-name", "bucket-2-name-3", "bucket.compose----name.1---hello",
			"a-b---c", "a.b---c", "blah__name", "blah_bucket_1"})
	// Need resolve
	resolvedName, err := r.ResolveName("bucket---name")
	a.Nil(err)
	a.Equal("bucket-3-name", resolvedName)

	resolvedName, err = r.ResolveName("bucket-s--s---s")
	a.Nil(err)
	a.Equal("bucket-s-2-s-3-s", resolvedName)

	resolvedName, err = r.ResolveName("abcdefghijklmnopqrstuvwxyz-s--s---s-s0123456789")
	a.Nil(err)
	a.Equal("abcdefghijklmnopqrstuvwxyz-s-2-s-3-s-s0123456789", resolvedName)

	// Resolved, and need add further add suffix
	resolvedName, err = r.ResolveName("bucket.name")
	a.Nil(err)
	a.Equal("bucket-name-4", resolvedName)

	resolvedName, err = r.ResolveName("bucket--name")
	a.Nil(err)
	a.Equal("bucket-2-name-2", resolvedName)

	// Names don't need resolve
	resolvedName, err = r.ResolveName("bucket-name")
	a.Nil(err)
	a.Equal("bucket-name", resolvedName)

	resolvedName, err = r.ResolveName("bucket-name-2")
	a.Nil(err)
	a.Equal("bucket-name-2", resolvedName)

	resolvedName, err = r.ResolveName("bucket-name-3")
	a.Nil(err)
	a.Equal("bucket-name-3", resolvedName)

	resolvedName, err = r.ResolveName("bucket-2-name")
	a.Nil(err)
	a.Equal("bucket-2-name", resolvedName)

	resolvedName, err = r.ResolveName("bucket-2-name-3")
	a.Nil(err)
	a.Equal("bucket-2-name-3", resolvedName)

	resolvedName, err = r.ResolveName("bucket.compose----name.1---hello")
	a.Nil(err)
	a.Equal("bucket-compose-4-name-1-3-hello", resolvedName)

	resolvedName, err = r.ResolveName("blah__name")
	a.Nil(err)
	a.Equal("blah-2-name", resolvedName)

	resolvedName, err = r.ResolveName("blah_bucket_1")
	a.Nil(err)
	a.Equal("blah-bucket-1", resolvedName)

	resolvedNameCollision1, err := r.ResolveName("a.b---c")
	a.Nil(err)
	resolvedNameCollision2, err := r.ResolveName("a-b---c")
	a.Nil(err)

	a.EqualValues(1, common.Iff(resolvedNameCollision1 == "a-b-3-c", 1, 0)^common.Iff(resolvedNameCollision2 == "a-b-3-c", 1, 0))
	a.EqualValues(1, common.Iff(resolvedNameCollision1 == "a-b-3-c-2", 1, 0)^common.Iff(resolvedNameCollision2 == "a-b-3-c-2", 1, 0))
}

func TestGCPBucketNameToAzureResourceResolverNegative(t *testing.T) {
	a := assert.New(t)
	r := NewGCPBucketNameToAzureResourcesResolver([]string{"0123456789.0123456789.0123456789.012345678901234567890123456789", "0123456789-0123456789-0123456789-012345678901234567890123456789"}) // with length 64
	_, err := r.ResolveName("0123456789.0123456789.0123456789.012345678901234567890123456789")
	a.NotNil(err)
	a.True(strings.Contains(err.Error(), "invalid for destination"))

	r = NewGCPBucketNameToAzureResourcesResolver([]string{"0123456789--0123456789-0123456789012345678901234567890123456789"})
	_, err = r.ResolveName("0123456789--0123456789-0123456789012345678901234567890123456789")
	a.NotNil(err)
	a.True(strings.Contains(err.Error(), "invalid for destination"))

	r = NewGCPBucketNameToAzureResourcesResolver([]string{"namea"})
	_, err = r.ResolveName("specialnewnameb")
	a.Nil(err)
}
