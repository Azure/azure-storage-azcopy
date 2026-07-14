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
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

// testCloseableReaderAt wraps a bytes.Reader to satisfy CloseableReaderAt for tests.
type testCloseableReaderAt struct {
	*bytes.Reader
}

func (testCloseableReaderAt) Close() error { return nil }

// testNoopLogger satisfies ILogger without doing anything.
type testNoopLogger struct{}

func (testNoopLogger) ShouldLog(LogLevel) bool { return false }
func (testNoopLogger) Log(LogLevel, string)    {}
func (testNoopLogger) Panic(err error)         { panic(err) }

// TestSingleChunkReader_ReadAfterCloseSucceedsOnRetry verifies the fix for the AFD-recycle
// upload failure: when the HTTP transport closes the request body (mid-flight because
// the server closed the connection), the azcore retry policy calls RewindBody+Read on
// the same singleChunkReader. Previously, a stale isClosed=true flag caused every
// retry to fail with "closed while reading", exhausting the retry budget without
// actually resending the chunk. After the fix, a Close() followed by Seek(0)+Read()
// must successfully re-fetch the chunk data from disk.
func TestSingleChunkReader_ReadAfterCloseSucceedsOnRetry(t *testing.T) {
	a := assert.New(t)

	// Prepare a synthetic 1 MiB chunk of file data.
	const chunkLen int64 = 1024 * 1024
	fileData := make([]byte, chunkLen)
	for i := range fileData {
		fileData[i] = byte(i)
	}

	sourceFactory := func() (CloseableReaderAt, error) {
		return testCloseableReaderAt{bytes.NewReader(fileData)}, nil
	}

	ctx := context.Background()
	cr := NewSingleChunkReader(
		ctx,
		sourceFactory,
		NewChunkID("test", 0, chunkLen),
		chunkLen,
		nil, // chunk status logger not required
		testNoopLogger{},
		NewMultiSizeSlicePool(chunkLen),
		NewCacheLimiter(chunkLen*4),
	)

	// First attempt: read the chunk fully (mimics the HTTP transport streaming the body).
	firstBuf := make([]byte, chunkLen)
	n, err := io.ReadFull(cr, firstBuf)
	a.NoError(err)
	a.Equal(int(chunkLen), n)
	a.Equal(fileData, firstBuf)

	// Simulate the HTTP transport closing the request body after the server closed the
	// connection.  Note: this happens even while the transfer's context is still active,
	// because it is triggered by a peer-side TCP close, not by AzCopy cancellation.
	a.NoError(cr.Close())

	// The azcore retry policy now calls RewindBody (Seek to 0) and re-drives the body.
	pos, err := cr.Seek(0, io.SeekStart)
	a.NoError(err)
	a.Equal(int64(0), pos)

	// Retry read: this MUST succeed and return the same bytes.  Before the fix, it
	// failed with errors.New("closed while reading") because the isClosed flag was
	// still set from the earlier Close() call, so every retry got the same failure and
	// the chunk was never actually re-sent to the service.
	retryBuf := make([]byte, chunkLen)
	n, err = io.ReadFull(cr, retryBuf)
	a.NoError(err)
	a.Equal(int(chunkLen), n)
	a.Equal(fileData, retryBuf)
}

// TestSingleChunkReader_ConcurrentCloseDuringReadStillFails verifies that the original
// safety check for a Close() that races with an in-flight disk read is preserved after
// the retry-reuse fix.
func TestSingleChunkReader_ConcurrentCloseDuringReadStillFails(t *testing.T) {
	a := assert.New(t)

	const chunkLen int64 = 4 * 1024

	// Custom source whose ReadAt blocks until we release it, so we can Close mid-read.
	release := make(chan struct{})
	sourceFactory := func() (CloseableReaderAt, error) {
		return &blockingReaderAt{data: make([]byte, chunkLen), release: release}, nil
	}

	cr := NewSingleChunkReader(
		context.Background(),
		sourceFactory,
		NewChunkID("test", 0, chunkLen),
		chunkLen,
		nil,
		testNoopLogger{},
		NewMultiSizeSlicePool(chunkLen),
		NewCacheLimiter(chunkLen*4),
	)

	// Start a read in a goroutine.
	readErr := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(cr, make([]byte, chunkLen))
		readErr <- err
	}()

	// Wait until the read is blocked inside ReadAt, then trigger Close.
	<-release // signals that ReadAt has been entered
	a.NoError(cr.Close())
	release <- struct{}{} // unblock ReadAt so it can return

	err := <-readErr
	a.Error(err)
	a.Contains(err.Error(), "closed while reading")
}

// blockingReaderAt signals when ReadAt is entered and blocks until told to proceed.
type blockingReaderAt struct {
	data    []byte
	release chan struct{}
}

func (b *blockingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	b.release <- struct{}{} // signal that we've entered ReadAt
	<-b.release             // block until told to proceed
	n := copy(p, b.data[off:])
	return n, nil
}

func (b *blockingReaderAt) Close() error { return nil }
