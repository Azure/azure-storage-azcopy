package common

import (
	"net/url"

	chk "gopkg.in/check.v1"
)

type extensionsTestSuite struct{}

var _ = chk.Suite(&extensionsTestSuite{})

func (s *extensionsTestSuite) TestGenerateFullPath(c *chk.C) {
	// the goal is to make sure the root path and child path are always combined correctly
	testCases := map[string][]string{
		"/usr/foo1/bla.txt": {"/usr/foo1", "bla.txt"},    // normal case
		"/usr/foo2/bla.txt": {"/usr/foo2/", "bla.txt"},   // normal case
		"/usr/foo3/bla.txt": {"/usr/foo3", "/bla.txt"},   // normal case
		"/usr/foo4/bla.txt": {"/usr/foo4/", "/bla.txt"},  // normal case
		"/usr/foo5/bla.txt": {"/usr/foo5/bla.txt", ""},   // single file
		"/usr/foo6/bla.txt": {"/usr/foo6/bla.txt", "/"},  // single file
		"/usr/foo7/bla.txt": {"/usr/foo7/bla.txt/", ""},  // single file
		"/usr/foo8/bla.txt": {"/usr/foo8/bla.txt/", "/"}, // single file
		"bla1.txt":          {"", "bla1.txt"},            // no parent
		"bla2.txt":          {"", "/bla2.txt"},           // no parent
		"bla3.txt":          {"/", "bla3.txt"},           // no parent
		"bla4.txt":          {"/", "/bla4.txt"},          // no parent
		"C://bla1.txt":      {"C://", "bla1.txt"},        // edge case: if root has intentional path separator at the end
		"C://bla2.txt":      {"C://", "/bla2.txt"},       // edge case: if root has intentional path separator at the end
	}

	// Action & Assert
	for expectedFullPath, input := range testCases {
		resultFullPath := GenerateFullPath(input[0], input[1])

		c.Assert(resultFullPath, chk.Equals, expectedFullPath)
	}
}

func (*extensionsTestSuite) TestURLWithPlusDecodedInPath(c *chk.C) {
	type expectedResults struct {
		expectedResult  string
		expectedRawPath string
		expectedPath    string
	}

	// Keys are converted to URLs before running tests.
	replacementTests := map[string]expectedResults{
		// These URLs will produce a raw path, because it has both encoded characters and decoded characters.
		"https://example.com/%2A+*": {
			expectedResult:  "https://example.com/%2A%20*",
			expectedRawPath: "/%2A%20*",
			expectedPath:    "/* *",
		},
		// encoded character at end to see if we go out of bounds
		"https://example.com/*+%2A": {
			expectedRawPath: "/*%20%2A",
			expectedPath:    "/* *",
			expectedResult:  "https://example.com/*%20%2A",
		},
		// multiple pluses in a row to see if we can handle it
		"https://example.com/%2A+++*": {
			expectedResult:  "https://example.com/%2A%20%20%20*",
			expectedRawPath: "/%2A%20%20%20*",
			expectedPath:    "/*   *",
		},

		// This behaviour doesn't require much testing since, prior to the text processing errors changes, it was exactly what we used.
		"https://example.com/a+b": {
			expectedResult: "https://example.com/a%20b",
			expectedPath:   "/a b",
			// no raw path, this URL wouldn't have one (because there's no special encoded chars)
		},
	}

	for k, v := range replacementTests {
		uri, err := url.Parse(k)
		c.Assert(err, chk.IsNil)

		extension := URLExtension{*uri}.URLWithPlusDecodedInPath()

		c.Assert(extension.Path, chk.Equals, v.expectedPath)
		c.Assert(extension.RawPath, chk.Equals, v.expectedRawPath)
		c.Assert(extension.String(), chk.Equals, v.expectedResult)
	}
}
