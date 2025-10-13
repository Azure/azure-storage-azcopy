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

package common

import (
	"os"
	"path"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRotatingWriter(t *testing.T) {
	convertToMap := func(entries []os.DirEntry) map[string]int {
		ret := make(map[string]int)
		for _, v := range entries {
			ret[v.Name()] += 1
		}

		return ret
	}

	a := assert.New(t)
	logFileNameWithoutSuffix := "logfile"
	data := "This string is one hundred bytes. Also has some junk to make actually make it one hundred bytes. Bye"

	tmpDir, err := os.MkdirTemp("", "")
	defer os.RemoveAll(tmpDir)

	a.NoError(err)
	logFile := path.Join(tmpDir, logFileNameWithoutSuffix)

	// 1. Create a rotating writer of size 100B
	w, err := NewRotatingWriter(logFile+".log", 100)
	a.NoError(err)

	// write 10 bytes and verify there is only one file in tmpDir
	w.Write([]byte(data[:10]))
	entries, err := os.ReadDir(tmpDir)
	a.NoError(err)
	a.Equal(1, len(entries))
	a.Equal(logFileNameWithoutSuffix+".log", entries[0].Name())

	// write 90 more bytes and verify there is still only one file
	w.Write([]byte(data[:90]))
	entries, err = os.ReadDir(tmpDir)
	a.NoError(err)
	a.Equal(1, len(entries))
	a.Equal(logFileNameWithoutSuffix+".log", entries[0].Name())

	// write 10 more bytes and verify a new log file is created
	n, err := w.Write([]byte(data[:10]))
	a.Equal(10, n)
	a.NoError(err)

	entries, err = os.ReadDir(tmpDir)
	a.NoError(err)
	a.Equal(2, len(entries))
	f := convertToMap(entries)
	a.Contains(f, logFileNameWithoutSuffix+".log")
	a.Contains(f, logFileNameWithoutSuffix+".0.log")

	// Write 80 bytes to prepare for next test
	w.Write([]byte(data[:80]))

	// parallelly write from 5 writers and verify log is rotated only once.
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			w.Write([]byte(data[:10]))
			n, err := w.Write([]byte(data[:10]))
			a.Equal(10, n)
			a.NoError(err)
			wg.Done()
		}()
	}

	wg.Wait()

	// verify only one new log file is created.
	entries, err = os.ReadDir(tmpDir)
	a.NoError(err)
	a.Equal(3, len(entries))
	f = convertToMap(entries)
	a.Contains(f, logFileNameWithoutSuffix+".log")
	a.Contains(f, logFileNameWithoutSuffix+".0.log")
	a.Contains(f, logFileNameWithoutSuffix+".1.log")

	// close and verify we've 3 log files
	w.Close()
	entries, err = os.ReadDir(tmpDir)
	a.NoError(err)
	a.Equal(3, len(entries))
	f = convertToMap(entries)
	a.Contains(f, logFileNameWithoutSuffix+".log")
	a.Contains(f, logFileNameWithoutSuffix+".0.log")
	a.Contains(f, logFileNameWithoutSuffix+".1.log")
}
