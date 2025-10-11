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

package ste

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInferContentType(t *testing.T) {
	a := assert.New(t)
	// Arrange
	partMgr := jobPartMgr{}

	// the goal is to make sure content type detection at least works for static websites
	testCases := map[string]string{
		"/usr/foo/bla.txt":             "text/plain",
		"/usr/foo/bla.html":            "text/html",
		"/usr/foo/bla.css":             "text/css",
		"/usr/foo/bla.js":              "application/javascript",
		"/usr/foo/bla.json":            "application/json",
		"/usr/foo/bla.jpeg":            "image/jpeg",
		"/usr/foo/bla.png":             "image/png",
		"/usr/foo/bla.multiple.dot.js": "application/javascript",
		"/usr/foo/no/extension":        "application/octet-stream",
		"/usr/foo/bla.HTML":            "text/html",
	}

	// Action & Assert
	for testPath, expectedType := range testCases {
		contentType := partMgr.inferContentType(testPath, make([]byte, 5))

		// make sure the inferred type is correct
		// we use Contains to check because charset is also in contentType
		a.True(strings.Contains(contentType, expectedType))
	}
}
