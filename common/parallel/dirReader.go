// Copyright © Microsoft <wastore@microsoft.com>
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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// NewDirReader makes a directory reader.  If parallelStat is true, then the reader
// uses a pool of go-routines to do the lookups from name of directory entry to full os.FileInfo.
// Useful on Linux, but not Windows.
// Why do we need this? Because on Linux os.Readdir does the same lookups, but it does them sequentially which hurts performance.
// Alternatives like https://github.com/karrick/godirwalk avoid the lookup all together, but only if you don't need any information
// about each entry other than whether its a file or directory.  We definitely also need to know whether its a symlink.
// And, in our current architecture, we also need to get the size and LMT for the file.
func NewDirReader(totalAvailableParallelisim int, parallelStat bool) (DirReader, int) {
	if parallelStat {
		r := linuxDirReader{
			ch: make(chan linuxDirEntry, 10000),
		}

		// allocate 3/4 of available parallelism to dir reads
		parallelismForDirReads := int(float32(totalAvailableParallelisim*3) / 4)
		if parallelismForDirReads < 1 {
			parallelismForDirReads = 1
		}
		remainingParallelism := totalAvailableParallelisim - parallelismForDirReads
		if remainingParallelism < 1 {
			remainingParallelism = 1
		}

		// spin up workers
		for i := 0; i < parallelismForDirReads; i++ {
			go r.worker()
		}

		return r, remainingParallelism
	} else {
		// we're not reading file properties in parallel
		return &defaultDirReader{}, totalAvailableParallelisim
	}
}

////////////////////////

type defaultDirReader struct{}

// Readdir in the default reader just makes the normal OS read call
// On Windows, this is performant because Go does not have to make any additional OS calls to hydrate the raw results into os.FileInfos.
func (_ defaultDirReader) Readdir(dir *os.File, n int) ([]os.FileInfo, error) {
	return dir.Readdir(n)
}

func (_ defaultDirReader) Close() {
	// noop
}

/////////////////////////

type linuxDirEntry struct {
	parentDir *os.File
	name      string
	resultCh  chan failableFileInfo
}

type linuxDirReader struct {
	ch chan linuxDirEntry
}

var ReaddirTimeoutError = errors.New("readdir timed out getting file properties")

func (r linuxDirReader) Readdir(dir *os.File, n int) ([]os.FileInfo, error) {
	for try := 1; ; try++ {
		timeout := time.Duration(try*try) * time.Minute
		result, err := r.doReaddir(dir, n, timeout)
		if err == ReaddirTimeoutError && try <= 3 { // we saw a few timeouts in customer testing prior to adding this
			continue
		}
		return result, err
	}
}

func (r linuxDirReader) doReaddir(dir *os.File, n int, timeout time.Duration) ([]os.FileInfo, error) {
	// get the names
	names, err := dir.Readdirnames(n)
	if err != nil {
		return nil, err // TODO: is it correct to assume that if err has a value, then names is empty?
	}

	// enqueue the LStatting
	resCh := make(chan failableFileInfo, len(names)) // big enough that it will never get full and cause deadlocks
	for _, n := range names {
		r.ch <- linuxDirEntry{
			parentDir: dir,
			name:      n,
			resultCh:  resCh,
		}
	}

	// collect the results
	res := make([]os.FileInfo, 0, 256)
	for i := 0; i < len(names); i++ {
		select {
		case r := <-resCh:
			res = append(res, r) // r is failableFileInfo, so may carry its own error with it
		case <-time.After(timeout):
			return nil, ReaddirTimeoutError
		}
	}

	return res, nil
}

// worker reads linuxDirEntries from channel, LStat's them, and sends the result back on the
// entry-specific channel
func (r linuxDirReader) worker() {
	for {
		e, ok := <-r.ch
		if !ok {
			return
		}
		path := filepath.Join(e.parentDir.Name(), e.name)
		fi, err := os.Lstat(path) // Lstat because we don't want to follow symlinks
		if err != nil {
			err = fmt.Errorf("%w (this error is harmless if the file '%s' has just been deleted. But in any other case, this error may be a real error)",
				err, path)
		}
		select {
		case e.resultCh <- failableFileInfoImpl{fi, err}:
		default: // give up on this one, so this worker doesn't get permanently blocked
		}
	}
}

func (r linuxDirReader) Close() {
	close(r.ch) // so workers know to shutdown
}

type failableFileInfoImpl struct {
	os.FileInfo
	error error
}

func (f failableFileInfoImpl) Error() error {
	return f.error
}
