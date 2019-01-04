// Copyright © 2018 Microsoft <wastore@microsoft.com>
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
	"net/url"
	"strings"

	chk "gopkg.in/check.v1"
)

// Hookup to the testing framework
type s3URLPartsTestSuite struct{}

var _ = chk.Suite(&s3URLPartsTestSuite{})

func (s *s3URLPartsTestSuite) TestS3URLParse(c *chk.C) {
	u, _ := url.Parse("http://bucket.s3.amazonaws.com")
	p, err := NewS3URLParts(*u)
	c.Assert(err, chk.IsNil)
	c.Assert(p.Host, chk.Equals, "bucket.s3.amazonaws.com")
	c.Assert(p.Endpoint, chk.Equals, "s3.amazonaws.com")
	c.Assert(p.BucketName, chk.Equals, "bucket")
	c.Assert(p.ObjectKey, chk.Equals, "")
	c.Assert(p.Region, chk.Equals, "")
	c.Assert(p.Version, chk.Equals, "")

	u, _ = url.Parse("http://bucket.s3.amazonaws.com/")
	p, err = NewS3URLParts(*u)
	c.Assert(err, chk.IsNil)
	c.Assert(p.BucketName, chk.Equals, "bucket")
	c.Assert(p.Endpoint, chk.Equals, "s3.amazonaws.com")
	c.Assert(p.ObjectKey, chk.Equals, "")
	c.Assert(p.Region, chk.Equals, "")
	c.Assert(p.Version, chk.Equals, "")

	u, _ = url.Parse("http://bucket.s3-aws-region.amazonaws.com/keydir/keysubdir/keyname")
	p, err = NewS3URLParts(*u)
	c.Assert(err, chk.IsNil)
	c.Assert(p.Endpoint, chk.Equals, "s3-aws-region.amazonaws.com")
	c.Assert(p.BucketName, chk.Equals, "bucket")
	c.Assert(p.ObjectKey, chk.Equals, "keydir/keysubdir/keyname")
	c.Assert(p.Region, chk.Equals, "aws-region")
	c.Assert(p.Version, chk.Equals, "")

	u, _ = url.Parse("http://bucket.s3-aws-region.amazonaws.com/keyname")
	p, err = NewS3URLParts(*u)
	c.Assert(err, chk.IsNil)
	c.Assert(p.Endpoint, chk.Equals, "s3-aws-region.amazonaws.com")
	c.Assert(p.BucketName, chk.Equals, "bucket")
	c.Assert(p.ObjectKey, chk.Equals, "keyname")
	c.Assert(p.Region, chk.Equals, "aws-region")
	c.Assert(p.Version, chk.Equals, "")

	u, _ = url.Parse("http://bucket.s3-aws-region.amazonaws.com/keyname/")
	p, err = NewS3URLParts(*u)
	c.Assert(err, chk.IsNil)
	c.Assert(p.Endpoint, chk.Equals, "s3-aws-region.amazonaws.com")
	c.Assert(p.BucketName, chk.Equals, "bucket")
	c.Assert(p.ObjectKey, chk.Equals, "keyname/")
	c.Assert(p.Region, chk.Equals, "aws-region")
	c.Assert(p.Version, chk.Equals, "")

	u, _ = url.Parse("https://s3.amazonaws.com")
	p, err = NewS3URLParts(*u)
	c.Assert(err, chk.IsNil)
	c.Assert(p.Endpoint, chk.Equals, "s3.amazonaws.com")
	c.Assert(p.BucketName, chk.Equals, "")
	c.Assert(p.ObjectKey, chk.Equals, "")
	c.Assert(p.Region, chk.Equals, "")
	c.Assert(p.Version, chk.Equals, "")

	u, _ = url.Parse("https://s3.amazonaws.com/")
	p, err = NewS3URLParts(*u)
	c.Assert(err, chk.IsNil)
	c.Assert(p.Endpoint, chk.Equals, "s3.amazonaws.com")
	c.Assert(p.BucketName, chk.Equals, "")
	c.Assert(p.ObjectKey, chk.Equals, "")
	c.Assert(p.Region, chk.Equals, "")
	c.Assert(p.Version, chk.Equals, "")

	u, _ = url.Parse("https://s3-ap-southeast-1.amazonaws.com/")
	p, err = NewS3URLParts(*u)
	c.Assert(err, chk.IsNil)
	c.Assert(p.Endpoint, chk.Equals, "s3-ap-southeast-1.amazonaws.com")
	c.Assert(p.BucketName, chk.Equals, "")
	c.Assert(p.ObjectKey, chk.Equals, "")
	c.Assert(p.Region, chk.Equals, "ap-southeast-1")
	c.Assert(p.Version, chk.Equals, "")

	u, _ = url.Parse("https://s3-ap-southeast-1.amazonaws.com/jiac-art-awsbucket01")
	p, err = NewS3URLParts(*u)
	c.Assert(err, chk.IsNil)
	c.Assert(p.Endpoint, chk.Equals, "s3-ap-southeast-1.amazonaws.com")
	c.Assert(p.BucketName, chk.Equals, "jiac-art-awsbucket01")
	c.Assert(p.ObjectKey, chk.Equals, "")
	c.Assert(p.Region, chk.Equals, "ap-southeast-1")
	c.Assert(p.Version, chk.Equals, "")

	u, _ = url.Parse("https://s3-ap-southeast-1.amazonaws.com/jiac-art-awsbucket01/")
	p, err = NewS3URLParts(*u)
	c.Assert(err, chk.IsNil)
	c.Assert(p.Endpoint, chk.Equals, "s3-ap-southeast-1.amazonaws.com")
	c.Assert(p.BucketName, chk.Equals, "jiac-art-awsbucket01")
	c.Assert(p.ObjectKey, chk.Equals, "")
	c.Assert(p.Region, chk.Equals, "ap-southeast-1")
	c.Assert(p.Version, chk.Equals, "")

	u, _ = url.Parse("https://s3-ap-southeast-1.amazonaws.com/jiac-art-awsbucket01/Test.pdf")
	p, err = NewS3URLParts(*u)
	c.Assert(err, chk.IsNil)
	c.Assert(p.Endpoint, chk.Equals, "s3-ap-southeast-1.amazonaws.com")
	c.Assert(p.BucketName, chk.Equals, "jiac-art-awsbucket01")
	c.Assert(p.ObjectKey, chk.Equals, "Test.pdf")
	c.Assert(p.Region, chk.Equals, "ap-southeast-1")
	c.Assert(p.Version, chk.Equals, "")

	u, _ = url.Parse("https://s3-ap-southeast-1.amazonaws.com/jiac-art-awsbucket01/space+folder/Test.pdf")
	p, err = NewS3URLParts(*u)
	c.Assert(err, chk.IsNil)
	c.Assert(p.Endpoint, chk.Equals, "s3-ap-southeast-1.amazonaws.com")
	c.Assert(p.BucketName, chk.Equals, "jiac-art-awsbucket01")
	c.Assert(p.ObjectKey, chk.Equals, "space+folder/Test.pdf")
	c.Assert(p.Region, chk.Equals, "ap-southeast-1")
	c.Assert(p.Version, chk.Equals, "")

	// Version testing
	u, _ = url.Parse("https://s3.ap-northeast-2.amazonaws.com/jiac-art-awsbucket02-versionenabled/Test.pdf?versionId=Cy0pgpqHDTR7RlMEwU_BxDVER2QN5lJJ")
	p, err = NewS3URLParts(*u)
	c.Assert(err, chk.IsNil)
	c.Assert(p.Endpoint, chk.Equals, "s3.ap-northeast-2.amazonaws.com")
	c.Assert(p.BucketName, chk.Equals, "jiac-art-awsbucket02-versionenabled")
	c.Assert(p.ObjectKey, chk.Equals, "Test.pdf")
	c.Assert(p.Region, chk.Equals, "ap-northeast-2")
	c.Assert(p.Version, chk.Equals, "Cy0pgpqHDTR7RlMEwU_BxDVER2QN5lJJ")
}

func (s *s3URLPartsTestSuite) TestS3URLParseNegative(c *chk.C) {
	u, _ := url.Parse("http://bucket.amazonawstypo.com")
	_, err := NewS3URLParts(*u)
	c.Assert(err, chk.NotNil)
	c.Assert(strings.Contains(err.Error(), invalidS3URLErrorMessage), chk.Equals, true)
}
