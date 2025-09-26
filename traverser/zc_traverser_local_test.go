package traverser

import (
	"os"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
)

func TestCleanLocalPath(t *testing.T) {
	a := assert.New(t)
	testCases := map[string]string{
		"/user/foo/bar":    "/user/foo/bar", // regular unix path with no change
		"/user/foo/bar/":   "/user/foo/bar", // regular unix path with extra slash
		"/user//foo//bar/": "/user/foo/bar", // regular unix path with double slashes
		"./foo/bar":        "foo/bar",       // relative unix path
		"../foo/bar":       "../foo/bar",    // relative unix path with parent dir
		"foo/bar":          "foo/bar",       // shorthand relative unix path
	}

	for orig, expected := range testCases {
		a.Equal(expected, common.CleanLocalPath(orig))
	}
}

func TestCleanLocalPathForWindows(t *testing.T) {
	a := assert.New(t)
	// ignore these tests when not running on Windows
	// as the path cleaning behavior depends on the platform
	if os.PathSeparator != '\\' {
		t.Skip("not running since the test applies to Windows only")
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
		a.Equal(expected, common.CleanLocalPath(orig))
	}
}
