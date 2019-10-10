package common

import (
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
