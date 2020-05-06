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

// NewDirReader makes a Linux directory reader which uses a pool of go-routines to do the lookups from
// name of directory entry to full os.FileInfo.
// Why do we need this? Because on Linux os.Readdir does the same lookups, but it does them sequentially which hurts performance.
// Alternatives like https://github.com/karrick/godirwalk avoid the lookup all together, but only if you don't need any information
// about each entry other than whether its a file or directory.  We definitely also need to know whether its a symlink.
// And, in our current architecture, we also need to get the size and LMT for the file.
func NewDirReader(totalAvailableParallelisim int) (DirReader, int) {
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
}

type linuxDirEntry struct {
	parentDir *os.File
	name      string
	resultCh  chan lstatResult
}

type lstatResult struct {
	fi  os.FileInfo
	err error
}

type linuxDirReader struct {
	ch chan linuxDirEntry
}

var ReaddirTimeoutError = errors.New("readdir timed out getting file properties")

func (r linuxDirReader) Readdir(dir *os.File, n int) ([]os.FileInfo, error) {
	// get the names
	names, err := dir.Readdirnames(n)
	if err != nil {
		return nil, err // TODO: is it correct to assume that if err has a value, then names is empty?
	}

	// enqueue the LStatting
	resCh := make(chan lstatResult, 1000)
	for _, n := range names {
		r.ch <- linuxDirEntry{
			parentDir: dir,
			name:      n,
			resultCh:  resCh,
		}
	}

	// collect the results
	var lastErr error
	res := make([]os.FileInfo, 0, 256)
	for i := 0; i < len(names); i++ {
		select {
		case r := <-resCh:
			if r.err == nil {
				res = append(res, r.fi)
			} else {
				// if there was an error in the Lstatting, just assume that the file was deleted between the time we saw it and the
				// time we Lstatted it.  (But remember the error, in case it happens ALL the time)
				lastErr = r.err
			}
		case <-time.After(time.Minute):
			return nil, ReaddirTimeoutError
		}
	}

	if len(res) == 0 {
		return nil, fmt.Errorf("readdir all LStat calls failed with last error %w", lastErr) // TODO: doing this because it's not valid for this method to return empty result and nil error, IIRC, when n > 0. Check that?
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
		if err == nil {
			e.resultCh <- lstatResult{fi: fi}
		} else {
			e.resultCh <- lstatResult{err: err}
		}
	}
}

func (r linuxDirReader) Close() {
	close(r.ch) // so workers know to shutdown
}
