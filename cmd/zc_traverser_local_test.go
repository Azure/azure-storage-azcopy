package cmd

import (
	"os"

	"github.com/nitin-deamon/azure-storage-azcopy/v10/common"
	chk "gopkg.in/check.v1"
)

type localTraverserTestSuite struct{}

var _ = chk.Suite(&localTraverserTestSuite{})

func (s *localTraverserTestSuite) TestCleanLocalPath(c *chk.C) {
	testCases := map[string]string{
		"/user/foo/bar":    "/user/foo/bar", // regular unix path with no change
		"/user/foo/bar/":   "/user/foo/bar", // regular unix path with extra slash
		"/user//foo//bar/": "/user/foo/bar", // regular unix path with double slashes
		"./foo/bar":        "foo/bar",       // relative unix path
		"../foo/bar":       "../foo/bar",    // relative unix path with parent dir
		"foo/bar":          "foo/bar",       // shorthand relative unix path
	}

	for orig, expected := range testCases {
		c.Assert(common.CleanLocalPath(orig), chk.Equals, expected)
	}
}

func (s *localTraverserTestSuite) TestCleanLocalPathForWindows(c *chk.C) {
	// ignore these tests when not running on Windows
	// as the path cleaning behavior depends on the platform
	if os.PathSeparator != '\\' {
		c.Skip("not running since the test applies to Windows only")
	}

	// Paths on Windows get consolidated to backwards-slash typically.
	testCases := map[string]string{
		`C:\foo\bar`:  `C:\foo\bar`, // regular windows path with no change
		`C:\foo\bar\`: `C:\foo\bar`, // regular windows path with extra slash
		`.\foo\bar`:   `foo\bar`,    // relative windows path
		`..\foo\bar`:  `..\foo\bar`, // relative windows path with parent dir
		`foo\bar`:     `foo\bar`,    // shorthand relative windows path
		`\\foo\bar\`:  `\\foo\bar`,  // network share
		`C:\`:         `C:\`,        // special case, the slash after colon is actually required
		`D:`:          `D:\`,        // special case, the slash after colon is actually required
		`c:\`:         `c:\`,        // special case, the slash after colon is actually required
		`c:`:          `c:\`,        // special case, the slash after colon is actually required
	}

	for orig, expected := range testCases {
		c.Assert(common.CleanLocalPath(orig), chk.Equals, expected)
	}
}
