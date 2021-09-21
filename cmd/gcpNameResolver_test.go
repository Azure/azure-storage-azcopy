package cmd

import (
	"github.com/nitin-deamon/azure-storage-azcopy/v10/common"
	chk "gopkg.in/check.v1"
	"strings"
)

type gcpNameResolverTestSuite struct{}

var _ = chk.Suite(&gcpNameResolverTestSuite{})

func (s *gcpNameResolverTestSuite) TestGCPBucketNameToAzureResourceResolverBucketName(c *chk.C) {
	r := NewGCPBucketNameToAzureResourcesResolver([]string{"bucket.name.1"})
	resolvedName, err := r.ResolveName("bucket.name.1")
	c.Assert(err, chk.IsNil)
	c.Assert(resolvedName, chk.Equals, "bucket-name-1")

	r = NewGCPBucketNameToAzureResourcesResolver([]string{"bucket-name"})
	resolvedName, err = r.ResolveName("bucket-name")
	c.Assert(err, chk.IsNil)
	c.Assert(resolvedName, chk.Equals, "bucket-name")

	r = NewGCPBucketNameToAzureResourcesResolver([]string{"bucket--name"})
	resolvedName, err = r.ResolveName("bucket--name")
	c.Assert(err, chk.IsNil)
	c.Assert(resolvedName, chk.Equals, "bucket-2-name")

	r = NewGCPBucketNameToAzureResourcesResolver([]string{"bucketvalidname"})
	resolvedName, err = r.ResolveName("bucketvalidname")
	c.Assert(err, chk.IsNil)
	c.Assert(resolvedName, chk.Equals, "bucketvalidname")

	r = NewGCPBucketNameToAzureResourcesResolver([]string{"0123456789.0123456789.0123456789.012345678901234567890123456789"})
	resolvedName, err = r.ResolveName("0123456789.0123456789.0123456789.012345678901234567890123456789")
	c.Assert(err, chk.IsNil)
	c.Assert(resolvedName, chk.Equals, "0123456789-0123456789-0123456789-012345678901234567890123456789")

	r = NewGCPBucketNameToAzureResourcesResolver([]string{"0123456789--01234567890123456789012345678901234567890123456789"})
	resolvedName, err = r.ResolveName("0123456789--01234567890123456789012345678901234567890123456789")
	c.Assert(err, chk.IsNil)
	c.Assert(resolvedName, chk.Equals, "0123456789-2-01234567890123456789012345678901234567890123456789")

	r = NewGCPBucketNameToAzureResourcesResolver([]string{"bucket_name_1"})
	resolvedName, err = r.ResolveName("bucket_name_1")
	c.Assert(err, chk.IsNil)
	c.Assert(resolvedName, chk.Equals, "bucket-name-1")

	r = NewGCPBucketNameToAzureResourcesResolver([]string{"bucket__name"})
	resolvedName, err = r.ResolveName("bucket__name")
	c.Assert(err, chk.IsNil)
	c.Assert(resolvedName, chk.Equals, "bucket-2-name")

	r = NewGCPBucketNameToAzureResourcesResolver([]string{"bucket-_name"})
	resolvedName, err = r.ResolveName("bucket-_name")
	c.Assert(err, chk.IsNil)
	c.Assert(resolvedName, chk.Equals, "bucket-2-name")

}

func (s *gcpNameResolverTestSuite) TestGCPBucketNameToAzureResourceResolverMultipleBucketNames(c *chk.C) {
	r := NewGCPBucketNameToAzureResourcesResolver(
		[]string{"bucket.name", "bucket-name", "bucket-name-2", "bucket-name-3",
			"bucket---name", "bucket-s--s---s", "abcdefghijklmnopqrstuvwxyz-s--s---s-s0123456789",
			"bucket--name", "bucket-2-name", "bucket-2-name-3", "bucket.compose----name.1---hello",
			"a-b---c", "a.b---c", "blah__name", "blah_bucket_1"})
	// Need resolve
	resolvedName, err := r.ResolveName("bucket---name")
	c.Assert(err, chk.IsNil)
	c.Assert(resolvedName, chk.Equals, "bucket-3-name")

	resolvedName, err = r.ResolveName("bucket-s--s---s")
	c.Assert(err, chk.IsNil)
	c.Assert(resolvedName, chk.Equals, "bucket-s-2-s-3-s")

	resolvedName, err = r.ResolveName("abcdefghijklmnopqrstuvwxyz-s--s---s-s0123456789")
	c.Assert(err, chk.IsNil)
	c.Assert(resolvedName, chk.Equals, "abcdefghijklmnopqrstuvwxyz-s-2-s-3-s-s0123456789")

	// Resolved, and need add further add suffix
	resolvedName, err = r.ResolveName("bucket.name")
	c.Assert(err, chk.IsNil)
	c.Assert(resolvedName, chk.Equals, "bucket-name-4")

	resolvedName, err = r.ResolveName("bucket--name")
	c.Assert(err, chk.IsNil)
	c.Assert(resolvedName, chk.Equals, "bucket-2-name-2")

	// Names don't need resolve
	resolvedName, err = r.ResolveName("bucket-name")
	c.Assert(err, chk.IsNil)
	c.Assert(resolvedName, chk.Equals, "bucket-name")

	resolvedName, err = r.ResolveName("bucket-name-2")
	c.Assert(err, chk.IsNil)
	c.Assert(resolvedName, chk.Equals, "bucket-name-2")

	resolvedName, err = r.ResolveName("bucket-name-3")
	c.Assert(err, chk.IsNil)
	c.Assert(resolvedName, chk.Equals, "bucket-name-3")

	resolvedName, err = r.ResolveName("bucket-2-name")
	c.Assert(err, chk.IsNil)
	c.Assert(resolvedName, chk.Equals, "bucket-2-name")

	resolvedName, err = r.ResolveName("bucket-2-name-3")
	c.Assert(err, chk.IsNil)
	c.Assert(resolvedName, chk.Equals, "bucket-2-name-3")

	resolvedName, err = r.ResolveName("bucket.compose----name.1---hello")
	c.Assert(err, chk.IsNil)
	c.Assert(resolvedName, chk.Equals, "bucket-compose-4-name-1-3-hello")

	resolvedName, err = r.ResolveName("blah__name")
	c.Assert(err, chk.IsNil)
	c.Assert(resolvedName, chk.Equals, "blah-2-name")

	resolvedName, err = r.ResolveName("blah_bucket_1")
	c.Assert(err, chk.IsNil)
	c.Assert(resolvedName, chk.Equals, "blah-bucket-1")

	resolvedNameCollision1, err := r.ResolveName("a.b---c")
	c.Assert(err, chk.IsNil)
	resolvedNameCollision2, err := r.ResolveName("a-b---c")
	c.Assert(err, chk.IsNil)

	c.Assert(common.Iffint8(resolvedNameCollision1 == "a-b-3-c", 1, 0)^common.Iffint8(resolvedNameCollision2 == "a-b-3-c", 1, 0), chk.Equals, int8(1))
	c.Assert(common.Iffint8(resolvedNameCollision1 == "a-b-3-c-2", 1, 0)^common.Iffint8(resolvedNameCollision2 == "a-b-3-c-2", 1, 0), chk.Equals, int8(1))
}

func (s *gcpNameResolverTestSuite) TestGCPBucketNameToAzureResourceResolverNegative(c *chk.C) {
	r := NewGCPBucketNameToAzureResourcesResolver([]string{"0123456789.0123456789.0123456789.012345678901234567890123456789", "0123456789-0123456789-0123456789-012345678901234567890123456789"}) // with length 64
	_, err := r.ResolveName("0123456789.0123456789.0123456789.012345678901234567890123456789")
	c.Assert(err, chk.NotNil)
	c.Assert(
		strings.Contains(err.Error(), "invalid for destination"),
		chk.Equals,
		true)

	r = NewGCPBucketNameToAzureResourcesResolver([]string{"0123456789--0123456789-0123456789012345678901234567890123456789"})
	_, err = r.ResolveName("0123456789--0123456789-0123456789012345678901234567890123456789")
	c.Assert(err, chk.NotNil)
	c.Assert(
		strings.Contains(err.Error(), "invalid for destination"),
		chk.Equals,
		true)

	r = NewGCPBucketNameToAzureResourcesResolver([]string{"namea"})
	_, err = r.ResolveName("specialnewnameb")
	c.Assert(err, chk.IsNil)

}
