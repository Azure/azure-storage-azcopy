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
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestS3URLParse(t *testing.T) {
	a := assert.New(t)
	u, _ := url.Parse("http://bucket.s3.amazonaws.com")
	p, err := NewS3URLParts(*u)
	a.NoError(err)
	a.Equal("bucket.s3.amazonaws.com", p.Host)
	a.Equal("s3.amazonaws.com", p.Endpoint)
	a.Equal("bucket", p.BucketName)
	a.Equal("", p.ObjectKey)
	a.Equal("", p.Region)
	a.Equal("", p.Version)
	a.Equal("http://bucket.s3.amazonaws.com", p.String())

	u, _ = url.Parse("http://bucket.s3.amazonaws.com/")
	p, err = NewS3URLParts(*u)
	a.NoError(err)
	a.Equal("bucket", p.BucketName)
	a.Equal("s3.amazonaws.com", p.Endpoint)
	a.Equal("", p.ObjectKey)
	a.Equal("", p.Region)
	a.Equal("", p.Version)
	a.Equal("http://bucket.s3.amazonaws.com", p.String())

	u, _ = url.Parse("http://bucket.s3-aws-region.amazonaws.com/keydir/keysubdir/keyname")
	p, err = NewS3URLParts(*u)
	a.NoError(err)
	a.Equal("s3-aws-region.amazonaws.com", p.Endpoint)
	a.Equal("bucket", p.BucketName)
	a.Equal("keydir/keysubdir/keyname", p.ObjectKey)
	a.Equal("aws-region", p.Region)
	a.Equal("", p.Version)
	a.Equal("http://bucket.s3-aws-region.amazonaws.com/keydir/keysubdir/keyname", p.String())

	u, _ = url.Parse("http://bucket.s3-aws-region.amazonaws.com/keyname")
	p, err = NewS3URLParts(*u)
	a.NoError(err)
	a.Equal("s3-aws-region.amazonaws.com", p.Endpoint)
	a.Equal("bucket", p.BucketName)
	a.Equal("keyname", p.ObjectKey)
	a.Equal("aws-region", p.Region)
	a.Equal("", p.Version)
	a.Equal("http://bucket.s3-aws-region.amazonaws.com/keyname", p.String())

	u, _ = url.Parse("http://bucket.s3-aws-region.amazonaws.com/keyname/")
	p, err = NewS3URLParts(*u)
	a.NoError(err)
	a.Equal("s3-aws-region.amazonaws.com", p.Endpoint)
	a.Equal("bucket", p.BucketName)
	a.Equal("keyname/", p.ObjectKey)
	a.Equal("aws-region", p.Region)
	a.Equal("", p.Version)
	a.Equal("http://bucket.s3-aws-region.amazonaws.com/keyname/", p.String())

	// dual stack
	u, _ = url.Parse("http://bucket.s3.dualstack.aws-region.amazonaws.com/keyname/")
	p, err = NewS3URLParts(*u)
	a.NoError(err)
	a.Equal("s3.dualstack.aws-region.amazonaws.com", p.Endpoint)
	a.Equal("bucket", p.BucketName)
	a.Equal("keyname/", p.ObjectKey)
	a.Equal("aws-region", p.Region)
	a.Equal("", p.Version)
	a.Equal("http://bucket.s3.dualstack.aws-region.amazonaws.com/keyname/", p.String())

	u, _ = url.Parse("https://s3.amazonaws.com")
	p, err = NewS3URLParts(*u)
	a.NoError(err)
	a.Equal("s3.amazonaws.com", p.Endpoint)
	a.Equal("", p.BucketName)
	a.Equal("", p.ObjectKey)
	a.Equal("", p.Region)
	a.Equal("", p.Version)
	a.Equal("https://s3.amazonaws.com", p.String())

	u, _ = url.Parse("https://s3.amazonaws.com/")
	p, err = NewS3URLParts(*u)
	a.NoError(err)
	a.Equal("s3.amazonaws.com", p.Endpoint)
	a.Equal("", p.BucketName)
	a.Equal("", p.ObjectKey)
	a.Equal("", p.Region)
	a.Equal("", p.Version)
	a.Equal("https://s3.amazonaws.com", p.String())

	u, _ = url.Parse("https://s3-ap-southeast-1.amazonaws.com/")
	p, err = NewS3URLParts(*u)
	a.NoError(err)
	a.Equal("s3-ap-southeast-1.amazonaws.com", p.Endpoint)
	a.Equal("", p.BucketName)
	a.Equal("", p.ObjectKey)
	a.Equal("ap-southeast-1", p.Region)
	a.Equal("", p.Version)
	a.Equal("https://s3-ap-southeast-1.amazonaws.com", p.String())

	u, _ = url.Parse("https://s3-ap-southeast-1.amazonaws.com/jiac-art-awsbucket01")
	p, err = NewS3URLParts(*u)
	a.NoError(err)
	a.Equal("s3-ap-southeast-1.amazonaws.com", p.Endpoint)
	a.Equal("jiac-art-awsbucket01", p.BucketName)
	a.Equal("", p.ObjectKey)
	a.Equal("ap-southeast-1", p.Region)
	a.Equal("", p.Version)
	a.Equal("https://s3-ap-southeast-1.amazonaws.com/jiac-art-awsbucket01", p.String())

	u, _ = url.Parse("https://s3-ap-southeast-1.amazonaws.com/jiac-art-awsbucket01/")
	p, err = NewS3URLParts(*u)
	a.NoError(err)
	a.Equal("s3-ap-southeast-1.amazonaws.com", p.Endpoint)
	a.Equal("jiac-art-awsbucket01", p.BucketName)
	a.Equal("", p.ObjectKey)
	a.Equal("ap-southeast-1", p.Region)
	a.Equal("", p.Version)
	a.Equal("https://s3-ap-southeast-1.amazonaws.com/jiac-art-awsbucket01", p.String())

	u, _ = url.Parse("https://s3-ap-southeast-1.amazonaws.com/jiac-art-awsbucket01/Test.pdf")
	p, err = NewS3URLParts(*u)
	a.NoError(err)
	a.Equal("s3-ap-southeast-1.amazonaws.com", p.Endpoint)
	a.Equal("jiac-art-awsbucket01", p.BucketName)
	a.Equal("Test.pdf", p.ObjectKey)
	a.Equal("ap-southeast-1", p.Region)
	a.Equal("", p.Version)
	a.Equal("https://s3-ap-southeast-1.amazonaws.com/jiac-art-awsbucket01/Test.pdf", p.String())

	u, _ = url.Parse("https://s3-ap-southeast-1.amazonaws.com/jiac-art-awsbucket01/space+folder/Test.pdf")
	p, err = NewS3URLParts(*u)
	a.NoError(err)
	a.Equal("s3-ap-southeast-1.amazonaws.com", p.Endpoint)
	a.Equal("jiac-art-awsbucket01", p.BucketName)
	a.Equal("space+folder/Test.pdf", p.ObjectKey)
	a.Equal("ap-southeast-1", p.Region)
	a.Equal("", p.Version)
	a.Equal("https://s3-ap-southeast-1.amazonaws.com/jiac-art-awsbucket01/space+folder/Test.pdf", p.String())

	// Version testing
	u, _ = url.Parse("https://s3.ap-northeast-2.amazonaws.com/jiac-art-awsbucket02-versionenabled/Test.pdf?versionId=Cy0pgpqHDTR7RlMEwU_BxDVER2QN5lJJ")
	p, err = NewS3URLParts(*u)
	a.NoError(err)
	a.Equal("s3.ap-northeast-2.amazonaws.com", p.Endpoint)
	a.Equal("jiac-art-awsbucket02-versionenabled", p.BucketName)
	a.Equal("Test.pdf", p.ObjectKey)
	a.Equal("ap-northeast-2", p.Region)
	a.Equal("Cy0pgpqHDTR7RlMEwU_BxDVER2QN5lJJ", p.Version)
	a.Equal("https://s3.ap-northeast-2.amazonaws.com/jiac-art-awsbucket02-versionenabled/Test.pdf?versionId=Cy0pgpqHDTR7RlMEwU_BxDVER2QN5lJJ", p.String())

	// Version and dualstack testing
	u, _ = url.Parse("https://s3.dualstack.ap-northeast-2.amazonaws.com/jiac-art-awsbucket02-versionenabled/Test.pdf?versionId=Cy0pgpqHDTR7RlMEwU_BxDVER2QN5lJJ")
	p, err = NewS3URLParts(*u)
	a.NoError(err)
	a.Equal("s3.dualstack.ap-northeast-2.amazonaws.com", p.Endpoint)
	a.Equal("jiac-art-awsbucket02-versionenabled", p.BucketName)
	a.Equal("Test.pdf", p.ObjectKey)
	a.Equal("ap-northeast-2", p.Region)
	a.Equal("Cy0pgpqHDTR7RlMEwU_BxDVER2QN5lJJ", p.Version)
	a.Equal("https://s3.dualstack.ap-northeast-2.amazonaws.com/jiac-art-awsbucket02-versionenabled/Test.pdf?versionId=Cy0pgpqHDTR7RlMEwU_BxDVER2QN5lJJ", p.String())

}

func TestS3URLParseNegative(t *testing.T) {
	a := assert.New(t)
	u, _ := url.Parse("http://bucket.amazonawstypo.com")
	_, err := NewS3URLParts(*u)
	a.NotNil(err)
	a.True(strings.Contains(err.Error(), invalidS3URLErrorMessage))

	u, _ = url.Parse("http://bucket.s3.amazonawstypo.com")
	_, err = NewS3URLParts(*u)
	a.NotNil(err)
	a.True(strings.Contains(err.Error(), invalidS3URLErrorMessage))

	u, _ = url.Parse("http://s3-test.blob.core.windows.net")
	_, err = NewS3URLParts(*u)
	a.NotNil(err)
	a.True(strings.Contains(err.Error(), invalidS3URLErrorMessage))
}
