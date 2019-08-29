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

package cmd

import (
	"strings"

	"github.com/Azure/azure-storage-azcopy/common"
	chk "gopkg.in/check.v1"
)

// Hookup to the testing framework
type s3NameResolverTestSuite struct{}

var _ = chk.Suite(&s3NameResolverTestSuite{})

func (s *s3NameResolverTestSuite) TestS3BucketNameToAzureResourceResolverSingleBucketName(c *chk.C) {
	r := NewS3BucketNameToAzureResourcesResolver([]string{"bucket.name.1"})
	resolvedName, err := r.ResolveName("bucket.name.1")
	c.Assert(err, chk.IsNil)
	c.Assert(resolvedName, chk.Equals, "bucket-name-1")

	r = NewS3BucketNameToAzureResourcesResolver([]string{"bucket-name"})
	resolvedName, err = r.ResolveName("bucket-name")
	c.Assert(err, chk.IsNil)
	c.Assert(resolvedName, chk.Equals, "bucket-name")

	r = NewS3BucketNameToAzureResourcesResolver([]string{"bucket--name"})
	resolvedName, err = r.ResolveName("bucket--name")
	c.Assert(err, chk.IsNil)
	c.Assert(resolvedName, chk.Equals, "bucket-2-name")

	r = NewS3BucketNameToAzureResourcesResolver([]string{"bucketvalidname"})
	resolvedName, err = r.ResolveName("bucketvalidname")
	c.Assert(err, chk.IsNil)
	c.Assert(resolvedName, chk.Equals, "bucketvalidname")

	r = NewS3BucketNameToAzureResourcesResolver([]string{"0123456789.0123456789.0123456789.012345678901234567890123456789"})
	resolvedName, err = r.ResolveName("0123456789.0123456789.0123456789.012345678901234567890123456789")
	c.Assert(err, chk.IsNil)
	c.Assert(resolvedName, chk.Equals, "0123456789-0123456789-0123456789-012345678901234567890123456789")

	r = NewS3BucketNameToAzureResourcesResolver([]string{"0123456789--01234567890123456789012345678901234567890123456789"})
	resolvedName, err = r.ResolveName("0123456789--01234567890123456789012345678901234567890123456789")
	c.Assert(err, chk.IsNil)
	c.Assert(resolvedName, chk.Equals, "0123456789-2-01234567890123456789012345678901234567890123456789")
}

func (s *s3NameResolverTestSuite) TestS3BucketNameToAzureResourceResolverMultipleBucketNames(c *chk.C) {
	r := NewS3BucketNameToAzureResourcesResolver(
		[]string{"bucket.name", "bucket-name", "bucket-name-2", "bucket-name-3",
			"bucket---name", "bucket-s--s---s", "abcdefghijklmnopqrstuvwxyz-s--s---s-s0123456789",
			"bucket--name", "bucket-2-name", "bucket-2-name-3", "bucket.compose----name.1---hello",
			"a-b---c", "a.b---c"})
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

	resolvedNameCollision1, err := r.ResolveName("a.b---c")
	c.Assert(err, chk.IsNil)
	resolvedNameCollision2, err := r.ResolveName("a-b---c")
	c.Assert(err, chk.IsNil)

	c.Assert(common.Iffint8(resolvedNameCollision1 == "a-b-3-c", 1, 0)^common.Iffint8(resolvedNameCollision2 == "a-b-3-c", 1, 0), chk.Equals, int8(1))
	c.Assert(common.Iffint8(resolvedNameCollision1 == "a-b-3-c-2", 1, 0)^common.Iffint8(resolvedNameCollision2 == "a-b-3-c-2", 1, 0), chk.Equals, int8(1))
}

func (s *s3NameResolverTestSuite) TestS3BucketNameToAzureResourceResolverNegative(c *chk.C) {
	r := NewS3BucketNameToAzureResourcesResolver([]string{"0123456789.0123456789.0123456789.012345678901234567890123456789", "0123456789-0123456789-0123456789-012345678901234567890123456789"}) // with length 64
	_, err := r.ResolveName("0123456789.0123456789.0123456789.012345678901234567890123456789")
	c.Assert(err, chk.NotNil)
	c.Assert(
		strings.Contains(err.Error(), "invalid for the destination"),
		chk.Equals,
		true)

	r = NewS3BucketNameToAzureResourcesResolver([]string{"0123456789--0123456789-0123456789012345678901234567890123456789"})
	_, err = r.ResolveName("0123456789--0123456789-0123456789012345678901234567890123456789")
	c.Assert(err, chk.NotNil)
	c.Assert(
		strings.Contains(err.Error(), "invalid for the destination"),
		chk.Equals,
		true)

	r = NewS3BucketNameToAzureResourcesResolver([]string{"namea"})
	_, err = r.ResolveName("specialnewnameb")
	c.Assert(err, chk.IsNil) // Bucket resolver now supports new names being injected
}
