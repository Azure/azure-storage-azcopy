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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var windowsSystemDirectory = ""

func TestCrawlContinuesAfterDirectoryErrors(t *testing.T) {
	for _, message := range []string{"403 AuthorizationPermissionMismatch", "directory listing failed"} {
		t.Run(message, func(t *testing.T) {
			const failedDirs, healthyDirs = 1100, 100
			directoryErr := errors.New(message)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			var healthy atomic.Int64
			output, stats := CrawlWithStats(ctx, -1, func(dir Directory, enqueue func(Directory), _ func(DirectoryEntry, error)) error {
				n := dir.(int)
				if n == -1 {
					for i := 0; i < failedDirs+healthyDirs; i++ {
						enqueue(i)
					}
					return nil
				}
				if n < failedDirs {
					return directoryErr
				}
				healthy.Add(1)
				return nil
			}, 64)

			var received int
			for result := range output {
				_, err := result.Item()
				assert.ErrorIs(t, err, directoryErr)
				received++
			}
			require.NoError(t, ctx.Err(), "crawl must finish rather than wait for cancellation")
			assert.Equal(t, failedDirs, received)
			assert.Equal(t, int64(healthyDirs), healthy.Load())
			assert.Zero(t, atomic.LoadInt64(&stats.ActiveWorkers))
			assert.Zero(t, atomic.LoadInt64(&stats.QueuedDirs))
		})
	}
}

func TestCrawlErrorSendStopsOnCancellation(t *testing.T) {
	// Exercise the worker directly so the output stays full until the worker exits.
	// Reading it before exit would hide an uncancellable send.
	const workers = 64
	directoryErr := errors.New("403 AuthorizationPermissionMismatch")
	output := make(chan CrawlResult, 1000)
	for i := 0; i < cap(output); i++ {
		output <- CrawlResult{err: directoryErr}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{}, workers)
	c := &crawler{
		output:      output,
		parallelism: workers,
		stats:       &CrawlStats{},
		cond:        sync.NewCond(&sync.Mutex{}),
		workerBody: func(Directory, func(Directory), func(DirectoryEntry, error)) error {
			started <- struct{}{}
			return directoryErr
		},
	}
	for i := 0; i < workers+1; i++ {
		c.unstartedDirs = append(c.unstartedDirs, i)
	}
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go c.workerLoop(ctx, &wg, i)
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		// Unblock workers even if the implementation regresses, without leaking goroutines.
		for i := 0; i < workers; i++ {
			select {
			case <-output:
			default:
			}
		}
		<-done
	})
	for i := 0; i < workers; i++ {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("workers did not reach directory error handling")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("workers remained blocked on full output after cancellation")
	}
	assert.Len(t, output, cap(output), "cancellation must not require draining output")
	assert.Zero(t, atomic.LoadInt64(&c.stats.ActiveWorkers))
	assert.Equal(t, int64(1), atomic.LoadInt64(&c.stats.QueuedDirs))
}

func TestCrawlQueueLimitOptions(t *testing.T) {
	for _, tc := range []struct {
		name  string
		limit int
		want  int64
	}{
		{"default", 0, 1_000_000},
		{"negative", -1, 1_000_000},
		{"override", 100_000_000, 100_000_000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output, stats := CrawlWithStats(context.Background(), "root",
				func(Directory, func(Directory), func(DirectoryEntry, error)) error { return nil },
				8, CrawlOptions{MaxQueueDirectories: tc.limit})
			for range output {
			}
			if stats.MaxQueueDirectories != tc.want {
				t.Fatalf("effective threshold = %d, want %d", stats.MaxQueueDirectories, tc.want)
			}
		})
	}
}

func TestCrawlQueueLimitShutdown(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		limit, queued, worker int
		recentShutdown        bool
		wantShutdown          bool
	}{
		{"at threshold", 4, 4, 7, false, false},
		{"above threshold", 4, 5, 7, false, true},
		{"protected worker", 4, 5, 2, false, false},
		{"gradual shutdown", 4, 5, 7, true, false},
		{"old million limit", 1_000_000, 1_000_001, 7, false, true},
		{"raised fifty million limit", 50_000_000, 1_000_001, 7, false, false},
		{"raised hundred million limit", 100_000_000, 1_000_001, 7, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &crawler{
				unstartedDirs:       []Directory{"root"},
				parallelism:         8,
				maxQueueDirectories: tc.limit,
				stats:               &CrawlStats{},
				cond:                sync.NewCond(&sync.Mutex{}),
				workerBody: func(_ Directory, enqueue func(Directory), _ func(DirectoryEntry, error)) error {
					for i := 0; i < tc.queued; i++ {
						enqueue("child")
					}
					return nil
				},
			}
			if tc.recentShutdown {
				c.lastAutoShutdown = time.Now()
			}
			keepWorking, err := c.processOneDirectory(context.Background(), tc.worker)
			if err != nil || keepWorking == tc.wantShutdown {
				t.Fatalf("keepWorking=%t, err=%v, want shutdown=%t", keepWorking, err, tc.wantShutdown)
			}
			if c.stats.PeakQueuedDirs != int64(tc.queued) || c.stats.QueuedDirs != int64(tc.queued) || c.stats.ActiveWorkers != 0 {
				t.Fatalf("unexpected stats: %+v", c.stats)
			}
		})
	}
}

func TestParallelEnumerationFindsTheRightFiles(t *testing.T) {
	a := assert.New(t)
	dir := "/usr"
	if runtime.GOOS == "windows" {
		dir = windowsSystemDirectory
		a.NotEqual("", dir)
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
	Walk(context.TODO(), dir, 16, false, func(path string, _ os.FileInfo, fileErr error) error {
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
			t.Error("expected " + key)
			t.Fail()
		}
	}
	// repeat for cases where access was denied in the standard enumeration (since we still pick up those dirs in the parallel enumeration (we just don't pick up their contents))
	for key := range stdAccessDenied {
		if _, ok := parallelResults[key]; ok {
			delete(parallelResults, key)
		} else {
			t.Error("expected in access denied results" + key)
			t.Fail()
		}
	}
	// assert that everything has been removed now
	for key := range parallelResults {
		t.Error("unexpected extra " + key)
		t.Fail()
	}
}

func TestParallelEnumerationGetsTheRightFileInfo_NormalStat(t *testing.T) {
	doTestParallelEnumerationGetsTheRightFileInfo(false, t)
}

func TestParallelEnumerationGetsTheRightFileInfo_ParallelStat(t *testing.T) {
	doTestParallelEnumerationGetsTheRightFileInfo(true, t)
}

func doTestParallelEnumerationGetsTheRightFileInfo(parallelStat bool, t *testing.T) {
	a := assert.New(t)
	dir := "/usr"
	if runtime.GOOS == "windows" {
		dir = windowsSystemDirectory
		a.NotEqual("", dir)
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
	Walk(context.TODO(), dir, 64, parallelStat, func(path string, fi os.FileInfo, fileErr error) error {
		if fileErr == nil {
			parallelResults[path] = fi
		}
		return nil
	})

	// check results.
	a.True(len(stdResults) > 1000) // assert that we got a decent number of results
	// check all std results have the same file info in the parallel results
	for key := range stdResults {
		if pInfo, ok := parallelResults[key]; ok {
			stdInfo := stdResults[key]
			a.Equal(stdInfo.Name(), pInfo.Name(), key)
			a.Equal(stdInfo.Mode(), pInfo.Mode(), key)
			a.Equal(stdInfo.IsDir(), pInfo.IsDir(), key)
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
				a.True(pInfo.IsDir(), fmt.Sprintf("%s times are different, which is only OK on directories", key))
			}
			if !stdInfo.IsDir() {
				// std Enumeration gives 4096 as size of some directories on Windows, but parallel sizes them as zero
				// See here for what the std one is doing: https://stackoverflow.com/questions/33593335/size-of-directory-from-os-stat-lstat
				// Parallel one, on Windows, gets the info from the find result, so that's probably why its zero there for dirs
				a.Equal(stdInfo.Size(), pInfo.Size(), key)
			}
			// don't check the Sys() method.  It can be different, and we don't mind that
		} else {
			a.Fail("expected " + key)
		}
	}
}

func TestRootErrorsAreSignalled(t *testing.T) {
	a := assert.New(t)
	receivedError := false
	nonExistentDir := filepath.Join(os.TempDir(), "Big random-named directory that almost certainly doesn't exist 85784362628398473732827384")
	Walk(context.TODO(), nonExistentDir, 16, false, func(path string, _ os.FileInfo, fileErr error) error {
		if fileErr != nil && path == nonExistentDir {
			receivedError = true
		}
		return nil
	})
	a.True(receivedError)
}
