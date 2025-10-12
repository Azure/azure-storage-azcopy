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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func TestS3BucketNameToAzureResourceResolverSingleBucketName(t *testing.T) {
	a := assert.New(t)
	r := NewS3BucketNameToAzureResourcesResolver([]string{"bucket.name.1"})
	resolvedName, err := r.ResolveName("bucket.name.1")
	a.NoError(err)
	a.Equal("bucket-name-1", resolvedName)

	r = NewS3BucketNameToAzureResourcesResolver([]string{"bucket-name"})
	resolvedName, err = r.ResolveName("bucket-name")
	a.NoError(err)
	a.Equal("bucket-name", resolvedName)

	r = NewS3BucketNameToAzureResourcesResolver([]string{"bucket--name"})
	resolvedName, err = r.ResolveName("bucket--name")
	a.NoError(err)
	a.Equal("bucket-2-name", resolvedName)

	r = NewS3BucketNameToAzureResourcesResolver([]string{"bucketvalidname"})
	resolvedName, err = r.ResolveName("bucketvalidname")
	a.NoError(err)
	a.Equal("bucketvalidname", resolvedName)

	r = NewS3BucketNameToAzureResourcesResolver([]string{"0123456789.0123456789.0123456789.012345678901234567890123456789"})
	resolvedName, err = r.ResolveName("0123456789.0123456789.0123456789.012345678901234567890123456789")
	a.NoError(err)
	a.Equal("0123456789-0123456789-0123456789-012345678901234567890123456789", resolvedName)

	r = NewS3BucketNameToAzureResourcesResolver([]string{"0123456789--01234567890123456789012345678901234567890123456789"})
	resolvedName, err = r.ResolveName("0123456789--01234567890123456789012345678901234567890123456789")
	a.NoError(err)
	a.Equal("0123456789-2-01234567890123456789012345678901234567890123456789", resolvedName)
}

func TestS3BucketNameToAzureResourceResolverMultipleBucketNames(t *testing.T) {
	a := assert.New(t)
	r := NewS3BucketNameToAzureResourcesResolver(
		[]string{"bucket.name", "bucket-name", "bucket-name-2", "bucket-name-3",
			"bucket---name", "bucket-s--s---s", "abcdefghijklmnopqrstuvwxyz-s--s---s-s0123456789",
			"bucket--name", "bucket-2-name", "bucket-2-name-3", "bucket.compose----name.1---hello",
			"a-b---c", "a.b---c"})
	// Need resolve
	resolvedName, err := r.ResolveName("bucket---name")
	a.NoError(err)
	a.Equal("bucket-3-name", resolvedName)

	resolvedName, err = r.ResolveName("bucket-s--s---s")
	a.NoError(err)
	a.Equal("bucket-s-2-s-3-s", resolvedName)

	resolvedName, err = r.ResolveName("abcdefghijklmnopqrstuvwxyz-s--s---s-s0123456789")
	a.NoError(err)
	a.Equal("abcdefghijklmnopqrstuvwxyz-s-2-s-3-s-s0123456789", resolvedName)

	// Resolved, and need add further add suffix
	resolvedName, err = r.ResolveName("bucket.name")
	a.NoError(err)
	a.Equal("bucket-name-4", resolvedName)

	resolvedName, err = r.ResolveName("bucket--name")
	a.NoError(err)
	a.Equal("bucket-2-name-2", resolvedName)

	// Names don't need resolve
	resolvedName, err = r.ResolveName("bucket-name")
	a.NoError(err)
	a.Equal("bucket-name", resolvedName)

	resolvedName, err = r.ResolveName("bucket-name-2")
	a.NoError(err)
	a.Equal("bucket-name-2", resolvedName)

	resolvedName, err = r.ResolveName("bucket-name-3")
	a.NoError(err)
	a.Equal("bucket-name-3", resolvedName)

	resolvedName, err = r.ResolveName("bucket-2-name")
	a.NoError(err)
	a.Equal("bucket-2-name", resolvedName)

	resolvedName, err = r.ResolveName("bucket-2-name-3")
	a.NoError(err)
	a.Equal("bucket-2-name-3", resolvedName)

	resolvedName, err = r.ResolveName("bucket.compose----name.1---hello")
	a.NoError(err)
	a.Equal("bucket-compose-4-name-1-3-hello", resolvedName)

	resolvedNameCollision1, err := r.ResolveName("a.b---c")
	a.NoError(err)
	resolvedNameCollision2, err := r.ResolveName("a-b---c")
	a.NoError(err)

	a.EqualValues(1, common.Iff(resolvedNameCollision1 == "a-b-3-c", 1, 0)^common.Iff(resolvedNameCollision2 == "a-b-3-c", 1, 0))
	a.EqualValues(1, common.Iff(resolvedNameCollision1 == "a-b-3-c-2", 1, 0)^common.Iff(resolvedNameCollision2 == "a-b-3-c-2", 1, 0))
}

func TestS3BucketNameToAzureResourceResolverNegative(t *testing.T) {
	a := assert.New(t)
	r := NewS3BucketNameToAzureResourcesResolver([]string{"0123456789.0123456789.0123456789.012345678901234567890123456789", "0123456789-0123456789-0123456789-012345678901234567890123456789"}) // with length 64
	_, err := r.ResolveName("0123456789.0123456789.0123456789.012345678901234567890123456789")
	a.NotNil(err)
	a.True(strings.Contains(err.Error(), "invalid for the destination"))

	r = NewS3BucketNameToAzureResourcesResolver([]string{"0123456789--0123456789-0123456789012345678901234567890123456789"})
	_, err = r.ResolveName("0123456789--0123456789-0123456789012345678901234567890123456789")
	a.NotNil(err)
	a.True(strings.Contains(err.Error(), "invalid for the destination"))

	r = NewS3BucketNameToAzureResourcesResolver([]string{"namea"})
	_, err = r.ResolveName("specialnewnameb")
	a.NoError(err) // Bucket resolver now supports new names being injected
}
