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

package parallel

import (
	"context"
	"golang.org/x/sys/windows"
	chk "gopkg.in/check.v1"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// Hookup to the testing framework
func Test(t *testing.T) { chk.TestingT(t) }

type fileSystemCrawlerSuite struct{}

var _ = chk.Suite(&fileSystemCrawlerSuite{})
var ctx = context.Background()

func (s *fileSystemCrawlerSuite) TestParallelEnumeration(c *chk.C) {
	var err error
	dir := "/lib"
	if runtime.GOOS == "windows" {
		dir, err = windows.GetSystemDirectory()
		c.Assert(err, chk.IsNil)
	}

	// standard (Go SDK) file walk
	stdResults := make(map[string]struct{})
	stdAccessDenied := make(map[string]struct{})
	_ = filepath.Walk(dir, func(path string, _ os.FileInfo, fileErr error) error {
		if fileErr == nil {
			stdResults[path] = struct{}{}
		} else if strings.Contains(fileErr.Error(), "Access is denied") {
			stdAccessDenied[path] = struct{}{} // for a directory that cannot have _it's contents_ enumerated, filepath.Walk treats the directory as an error, whereas parallel.Walk returns a valid entry for the directory PLUS a separate error for trying to enumerate it
		}
		return nil
	})

	// our parallel walk
	parallelResults := make(map[string]struct{})
	Walk(dir, 16, func(path string, _ os.FileInfo, fileErr error) error {
		if fileErr == nil {
			parallelResults[path] = struct{}{}
		}
		return nil
	})

	// check results.
	// check all std results exist, and remove them from the parallel results
	for key := range stdResults {
		if _, ok := parallelResults[key]; ok {
			delete(parallelResults, key)
		} else {
			c.Error("expected " + key)
			c.Fail()
		}
	}
	// repeat for cases where access was denied in the standard enumeration (since we still pick up those dirs in the parallel enumeration (we just don't pick up their contents))
	for key := range stdAccessDenied {
		if _, ok := parallelResults[key]; ok {
			delete(parallelResults, key)
		} else {
			c.Error("expected in access denied results" + key)
			c.Fail()
		}
	}
	// assert that everything has been removed now
	for key := range parallelResults {
		c.Error("unexpected extra " + key)
		c.Fail()
	}
}

func (s *fileSystemCrawlerSuite) TestRootErrorsAreSignalled(c *chk.C) {
	receivedError := false
	nonExistentDir := filepath.Join(os.TempDir(), "Big random-named directory that almost certainly doesn't exist 85784362628398473732827384")
	Walk(nonExistentDir, 16, func(path string, _ os.FileInfo, fileErr error) error {
		if fileErr != nil && path == nonExistentDir {
			receivedError = true
		}
		return nil
	})
	c.Assert(receivedError, chk.Equals, true)
}
