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
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	chk "gopkg.in/check.v1"
)

// Hookup to the testing framework
func Test(t *testing.T) { chk.TestingT(t) }

type fileSystemCrawlerSuite struct{}

var _ = chk.Suite(&fileSystemCrawlerSuite{})
var ctx = context.Background()

var windowsSystemDirectory = ""

func (s *fileSystemCrawlerSuite) TestParallelEnumerationFindsTheRightFiles(c *chk.C) {
	dir := "/usr"
	if runtime.GOOS == "windows" {
		dir = windowsSystemDirectory
		c.Assert(dir, chk.Not(chk.Equals), "")
	}

	// standard (Go SDK) file walk
	stdResults := make(map[string]struct{})
	stdAccessDenied := make(map[string]struct{})
	_ = filepath.Walk(dir, func(path string, _ os.FileInfo, fileErr error) error {
		if fileErr == nil {
			stdResults[path] = struct{}{}
		} else if strings.Contains(fileErr.Error(), "denied") {
			stdAccessDenied[path] = struct{}{} // for a directory that cannot have _it's contents_ enumerated, filepath.Walk treats the directory as an error, whereas parallel.Walk returns a valid entry for the directory PLUS a separate error for trying to enumerate it
		}
		return nil
	})

	// our parallel walk
	parallelResults := make(map[string]struct{})
	Walk(nil, dir, 16, false, func(path string, _ os.FileInfo, fileErr error) error {
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

func (s *fileSystemCrawlerSuite) TestParallelEnumerationGetsTheRightFileInfo_NormalStat(c *chk.C) {
	s.doTestParallelEnumerationGetsTheRightFileInfo(false, c)
}

func (s *fileSystemCrawlerSuite) TestParallelEnumerationGetsTheRightFileInfo_ParallelStat(c *chk.C) {
	s.doTestParallelEnumerationGetsTheRightFileInfo(true, c)
}

func (s *fileSystemCrawlerSuite) doTestParallelEnumerationGetsTheRightFileInfo(parallelStat bool, c *chk.C) {

	dir := "/usr"
	if runtime.GOOS == "windows" {
		dir = windowsSystemDirectory
		c.Assert(dir, chk.Not(chk.Equals), "")
		dir = dir[0:1] + ":\\" + "Program Files" // need one where the contents won't change while our test runs
	}

	// standard (Go SDK) file walk
	stdResults := make(map[string]os.FileInfo)
	_ = filepath.Walk(dir, func(path string, fi os.FileInfo, fileErr error) error {
		if fileErr == nil {
			stdResults[path] = fi
		}
		return nil
	})

	// our parallel walk
	parallelResults := make(map[string]os.FileInfo)
	Walk(nil, dir, 64, parallelStat, func(path string, fi os.FileInfo, fileErr error) error {
		if fileErr == nil {
			parallelResults[path] = fi
		}
		return nil
	})

	// check results.
	c.Assert(len(stdResults) > 1000, chk.Equals, true) // assert that we got a decent number of results
	// check all std results have the same file info in the parallel results
	for key := range stdResults {
		if pInfo, ok := parallelResults[key]; ok {
			stdInfo := stdResults[key]
			c.Assert(pInfo.Name(), chk.Equals, stdInfo.Name(), chk.Commentf(key))
			c.Assert(pInfo.Mode(), chk.Equals, stdInfo.Mode(), chk.Commentf(key))
			c.Assert(pInfo.IsDir(), chk.Equals, stdInfo.IsDir(), chk.Commentf(key))
			if pInfo.ModTime() != stdInfo.ModTime() {
				// Sometimes, Windows will give us the creation time instead of the last write time for a directory
				// That's documented. (In fact, it's not clear to the author of this test how or why we are sometimes seeing the actual last write time,
				// but in any case, we see in tests where pInfo gets the creationTime (in the last write time field,
				// as documented) but stdInfo gets the actual LastWriteTime there.
				// For documentation, see https://docs.microsoft.com/en-us/windows/win32/api/minwinbase/ns-minwinbase-win32_find_dataa
				// We can tolerate this discrepancy, because we already know that directory last write times don't tell us any useful
				// info (e.g. on Windows, they reflect changes to children, but not to grandchildren).  Because they don't tell us
				// anything useful, we don't use them.
				// But a discrepancy on a file (not a directory) would be bad.
				//
				// Note that the directory discrepancies are reproducible from PowerShell on Win 10, so it's not a Go thing.
				// Looking at the directory C:\Program Files\Microsoft SQL Server\90
				// PS C:\Program Files\Microsoft SQL Server> (get-item 90).LastWriteTime
				// Tuesday, August 27, 2019 5:29:08 AM
				// PS C:\Program Files\Microsoft SQL Server> (gci | ? Name -eq 90).LastWriteTime  # ask for same info, via different API
				// Tuesday, August 27, 2019 5:26:53 AM     // and it gives is the creation datetime instead
				c.Assert(pInfo.IsDir(), chk.Equals, true, chk.Commentf(key+" times are different, which is only OK on directories"))
			}
			if !stdInfo.IsDir() {
				// std Enumeration gives 4096 as size of some directories on Windows, but parallel sizes them as zero
				// See here for what the std one is doing: https://stackoverflow.com/questions/33593335/size-of-directory-from-os-stat-lstat
				// Parallel one, on Windows, gets the info from the find result, so that's probably why its zero there for dirs
				c.Assert(pInfo.Size(), chk.Equals, stdInfo.Size(), chk.Commentf(key))
			}
			// don't check the Sys() method.  It can be different, and we don't mind that
		} else {
			c.Error("expected " + key)
			c.Fail()
		}
	}
}

func (s *fileSystemCrawlerSuite) TestRootErrorsAreSignalled(c *chk.C) {
	receivedError := false
	nonExistentDir := filepath.Join(os.TempDir(), "Big random-named directory that almost certainly doesn't exist 85784362628398473732827384")
	Walk(nil, nonExistentDir, 16, false, func(path string, _ os.FileInfo, fileErr error) error {
		if fileErr != nil && path == nonExistentDir {
			receivedError = true
		}
		return nil
	})
	c.Assert(receivedError, chk.Equals, true)
}
