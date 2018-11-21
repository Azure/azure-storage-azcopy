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

package common

import (
	"context"
	"errors"
	"io"
)

// Reader of ONE chunk of a file. Maybe be used to re-read multiple times (e.g. if
// we must retry the sending of the chunk).
// May use implementation dependent pre-fetch, and implementation-dependent
// logic for when to discard any prefetched data (typically when it has read to the end
// for the first time, the prefected data will be discarded)
// Cannot be read by multiple threads (since it's Read/Seek are inherently stateful)
type FileChunkReader interface {
	io.ReadSeeker
	io.Closer
	TryPrefetch(fileReader CloseableReaderAt) bool
	ReadAndRetain(p []byte) (int, error)
}

// Simple aggregation of existing io interfaces
type CloseableReaderAt interface {
	io.ReaderAt
	io.Closer
}

// Factory method for data source for simpleFileChunkReader
type ChunkReaderSourceFactory func()(CloseableReaderAt, error)


type simpleFileChunkReader struct {
	// context used when waiting for permission to supply data
	ctx context.Context

	// Limits the number of concurrent active sends
	sendLimiter SendLimiter

	// A factory to get hold of the file, in case we need to re-read any of it
	sourceFactory ChunkReaderSourceFactory

	// start position in file
	offsetInFile int64

	// number of bytes in this chunk
	length int64

	// position for Seek/Read
	positionInChunk int64

	// buffer used by prefetch
	buffer []byte
	// TODO: pooling of buffers to reduce pressure on GC?

	// used to track how many unread bytes we have prefetched, so that
	// callers can prevent excessive prefetching (to control RAM usage)
	prefetchedByteTracker PrefetchedByteCounter

	// do we currently own/hold one of the active "slots" for sending
	sendSlotHeld bool
}

// TODO: consider support for prefetching only part of chunk. For the cases where chunks are relatively large (e.g. 100 MB)
// TODO: that might work by having it preftech the start, and then, when that part is being sent out to the network, use a
// separate goroutine to read the next.  OR, we can just say, if you want to use 100 MB chunk sizes, use lots of RAM.

func NewSimpleFileChunkReader(ctx context.Context, sourceFactory ChunkReaderSourceFactory, offset int64, length int64, sendLimiter SendLimiter, prefetchedByteCounter PrefetchedByteCounter) FileChunkReader {
	if length <= 0 {
		panic("length must be greater than zero")
	}
	return &simpleFileChunkReader{
		ctx:                   ctx,
		sendLimiter:           sendLimiter,
		sourceFactory:         sourceFactory,
		offsetInFile:          offset,
		length:                length,
		prefetchedByteTracker: prefetchedByteCounter}
}

// Prefetch, and ignore any errors (just leave in not-prefetch-yet state, if there was an error)
func (cr *simpleFileChunkReader) TryPrefetch(fileReader CloseableReaderAt) bool {
	err := cr.prefetch(fileReader)
	if err != nil {
		// if where was an error, be sure to put us back into a valid "not-yet-prefetched" state
		cr.buffer = nil
		return false
	}
	return true
}

// Prefetch the data in this chunk, using a file object that is provided to us (providing it to us supports sequential read, in the non-retry scenario)
// We use io.ReaderAt, rather than io.Reader, just for maintainablity/ensuring correctness. (Since just using Reader requires the caller to
// follow certain assumptions about positioning the file pointer at the right place before calling us, but using ReaderAt does not).
func (cr *simpleFileChunkReader) prefetch(fileReader CloseableReaderAt) error {
	if cr.buffer != nil {
		return nil // already prefetched
	}

	cr.buffer = make([]byte, cr.length)  // TODO: consider pooling these, depending on impact found in profiling

	totalBytesRead, err := fileReader.ReadAt(cr.buffer, cr.offsetInFile)
	if err != nil && err != io.EOF {
		return err
	}
	if int64(totalBytesRead) != cr.length {
		return errors.New("bytes read not equal to expected length. Chunk reader must be constructed so that it won't read past end of file")
	}

	// increase count of unused prefetched bytes
	cr.prefetchedByteTracker.Add(int64(totalBytesRead))
	return nil
}

func (cr *simpleFileChunkReader) redoPrefetchIfNecessary() error {
	if cr.buffer != nil {
		return nil // nothing to do
	}

	// create a new reader for the file (since anything that was passed to our Prefetch routine before is, deliberately, not kept)
	sourceFile, err := cr.sourceFactory()
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	// no need to seek first, because its a ReaderAt
	return cr.prefetch(sourceFile)
}

// Seeks within this chunk
// Seeking is used for retries, and also by some code to get length (by seeking to end)
func (cr *simpleFileChunkReader) Seek(offset int64, whence int) (int64, error) {

	newPosition := cr.positionInChunk

	switch whence {
	case io.SeekStart:
		newPosition = offset
	case io.SeekCurrent:
		newPosition += offset
	case io.SeekEnd:
		newPosition = cr.length - offset
	}

	if newPosition < 0 {
		return 0, errors.New("cannot seek to before beginning")
	}
	if newPosition > cr.length {
		newPosition = cr.length
	}

	cr.positionInChunk = newPosition
	return cr.positionInChunk, nil
}

// Reads from within this chunk
func (cr *simpleFileChunkReader) Read(p []byte) (n int, err error) {
	// This is a normal read, so free the prefetch buffer when hit EOF (i.e. end of this chunk).
	// We do so on the assumption that if we've read to the end we don't need the prefetched data any longer.
	// (If later, there's a retry that forces seek back to start and re-read, we'll automatically trigger a re-fetch at that time)
	return cr.doRead(p, true)
}

// Special version of Read, for cases where we don't want the usual automatic behaviour when the end of
func (cr *simpleFileChunkReader) ReadAndRetain(p []byte) (n int, err error){
	// Caller has asked us to retain the prefetch buffer no matter what, even if we hit EOF (end of this chunk)
	return cr.doRead(p,false)
}

func (cr *simpleFileChunkReader) doRead(p []byte, freeBufferOnEof bool) (n int, err error) {
	// check for EOF, BEFORE we ensure prefetch
	// (Otherwise, some readers can call as after EOF, and we end up re-pre-fetching)
	if cr.positionInChunk >= cr.length {
		return 0, io.EOF
	}

	// Always use the prefetch logic to read the data
	// This is simpler to maintain than using a different code path for the (rare) cases
	// where there has been no prefetch before this routine is called
	err = cr.redoPrefetchIfNecessary()
	if err != nil {
		return 0, err
	}

	if cr.positionInChunk == 0 && !cr.sendSlotHeld {
		// (Must check slotHeld in the if above, in case first part of file is probed for mime-type, in which case the
		// early bytes may be read TWICE)
		//
		// Wait until we are allowed to be one of the actively-sending goroutines
		// (The total count of active senders is limited to provide better network performance,
		// on a per-CPU-usage basis. Without this, CPU usage in the OS's network stack is much higher
		// in some tests.)
		// It would have been nice to do this restriction not here in the Read method but more in the structure
		// of the code, by explicitly separating HTTP "write body" from "read response" (and only applying this limit
		// to the former).  However, that's difficult/impossible in the architecture of net/http. So instead we apply
		// the restriction here, in code that is called at the time of STARTING to write the body
		err = cr.sendLimiter.AcquireSendSlot(cr.ctx)
		if err != nil {
			return 0, err
		}
		cr.sendSlotHeld = true
	}

	// Copy the data across
	bytesCopied := copy(p, cr.buffer[cr.positionInChunk:])
	cr.positionInChunk += int64(bytesCopied)

	// check for EOF
	isEof := cr.positionInChunk >= cr.length
	if isEof {
		if freeBufferOnEof {
			cr.deactivate()
		}
		return bytesCopied, io.EOF
	}

	return bytesCopied, nil
}

func (cr *simpleFileChunkReader) deactivate() {
	if cr.sendSlotHeld {
		cr.sendLimiter.ReleaseSendSlot() // important, otherwise other instances of this struct may be blocked from sending
		cr.sendSlotHeld = false
	}

	// free the buffer now, since we probably won't read it again
	// (and on the relatively rare occasions when we do (for retry/resend cases), we'll just take the hit
	// of re-reading it from disk, and the added hit that that read will be non-sequential)
	if cr.buffer == nil {
		return
	}
	cr.buffer = nil
	cr.prefetchedByteTracker.Add(-cr.length)
}

// Some code paths can call this, when cleaning up. (Even though in the normal, non error, code path, we don't NEED this
// because we close at the completion of a successful read of the whole prefetch buffer.
// We still want this though, to handle cases where for some reason the transfer stops before all the buffer has been read.)
// Without this close, if something failed part way through, we would keep counting this object's bytes in prefetchedByteTracker
// "for ever", even after the object is gone.
func (cr *simpleFileChunkReader) Close() error {
	cr.deactivate()
	return nil
}
