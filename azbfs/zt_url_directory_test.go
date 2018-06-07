package azbfs_test

import (
	chk "gopkg.in/check.v1" // go get gopkg.in/check.v1
	"context"
)
type DirectoryUrlSuite struct {}

var _ = chk.Suite(&DirectoryUrlSuite{})


func (s *DirectoryUrlSuite) TestFileCreateDeleteDefault(c *chk.C) {
	fsu := getBfsServiceURL()
	fsURL, _ := createNewFileSystem(c, fsu)
	name := generateDirectoryName()

	defer delFileSystem(c, fsURL)

	dirUrl := fsURL.NewDirectoryURL(name)
	_, err := dirUrl.Create(context.Background())
	c.Assert(err, chk.IsNil)
}
