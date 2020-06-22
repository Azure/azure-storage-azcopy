package common

import (
	chk "gopkg.in/check.v1"
	"net/url"
	"strings"
)

type gcpURLPartsTestSuite struct{}

var _ = chk.Suite(&gcpURLPartsTestSuite{})

func (s *gcpURLPartsTestSuite) TestGCPURLParse(c *chk.C) {
	u, _ := url.Parse("http://storage.cloud.google.com/bucket")
	p, err := NewGCPURLParts(*u)
	c.Assert(err, chk.IsNil)
	c.Assert(p.Host, chk.Equals, "storage.cloud.google.com")
	c.Assert(p.BucketName, chk.Equals, "bucket")
	c.Assert(p.ObjectKey, chk.Equals, "")
	c.Assert(p.String(), chk.Equals, "http://storage.cloud.google.com/bucket")

	u, _ = url.Parse("https://storage.cloud.google.com")
	p, err = NewGCPURLParts(*u)
	c.Assert(err, chk.IsNil)
	c.Assert(p.BucketName, chk.Equals, "")
	c.Assert(p.ObjectKey, chk.Equals, "")
	c.Assert(p.String(), chk.Equals, "https://storage.cloud.google.com")

	u, _ = url.Parse("http://storage.cloud.google.com/bucket/keyname/")
	p, err = NewGCPURLParts(*u)
	c.Assert(err, chk.IsNil)
	c.Assert(p.BucketName, chk.Equals, "bucket")
	c.Assert(p.ObjectKey, chk.Equals, "keyname/")
	c.Assert(p.String(), chk.Equals, "http://storage.cloud.google.com/bucket/keyname/")

}

func (s *gcpURLPartsTestSuite) TestGCPURLParseNegative(c *chk.C) {
	u, _ := url.Parse("https://storage.cloud.googly.com/bucket")
	_, err := NewGCPURLParts(*u)
	c.Assert(err, chk.NotNil)
	c.Assert(strings.Contains(err.Error(), invalidGCPURLErrorMessage), chk.Equals, true)

	u, _ = url.Parse("https://mcdheestorage.blob.core.windows.net")
	_, err = NewGCPURLParts(*u)
	c.Assert(err, chk.NotNil)
	c.Assert(strings.Contains(err.Error(), invalidGCPURLErrorMessage), chk.Equals, true)
}
