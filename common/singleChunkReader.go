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

// Reader of ONE chunk of a file. Maybe used to re-read multiple times (e.g. if
// we must retry the sending of the chunk).
// A instance of this type cannot be used by multiple threads (since it's Read/Seek are inherently stateful)
// The reader can throw away the data after each successful read, and then re-read it from disk if there
// is a need to retry the transmission of the chunk. That saves us the RAM cost of  from having to keep every
// transmitted chunk in RAM until acknowledged by the service.  We just re-read if the service says we need to retry.
// Although there's a time (performance) cost in the re-read, that's fine in a retry situation because the retry
// indicates we were going too fast for the service anyway.
type SingleChunkReader interface {

	// ReadSeeker is used to read the contents of the chunk, and because the sending pipeline seeks at various times
	io.ReadSeeker

	// Closer is needed to clean up resources
	io.Closer

	// BlockingPrefetch tries to read the full contents of the chunk into RAM.
	BlockingPrefetch(fileReader io.ReaderAt, isRetry bool) error

	// CaptureLeadingBytes is used to grab enough of the initial bytes to do MIME-type detection.  Expected to be called only
	// on the first chunk in each file (since there's no point in calling it on others)
	CaptureLeadingBytes() []byte

	// Length is the number of bytes in the chunk
	Length() int64

	// HasPrefectchedEntirelyZeros gives an indication of whether this chunk is entirely zeros.  If it returns true
	// then the chunk content has been prefetched AND it was all zeroes. For some remote destinations, that support "sparse file"
	// semantics, it is safe and correct to skip the upload of those chunks where this returns true.
	// In the rare edge case where this returns false due to the prefetch having failed (rather than the contents being non-zero),
	// we'll just treat it as a non-zero chunk. That's simpler (to code, to review and to test) than having this code force a prefetch.
	HasPrefetchedEntirelyZeros() bool
}

// Simple aggregation of existing io interfaces
type CloseableReaderAt interface {
	io.ReaderAt
	io.Closer
}

// Factory method for data source for singleChunkReader
type ChunkReaderSourceFactory func() (CloseableReaderAt, error)

type singleChunkReader struct {
	// context used to allow cancellation of blocking operations
	// (Yes, ideally contexts are not stored in structs, but we need it inside Read, and there's no way for it to be passed in there)
	ctx context.Context

	// pool of byte slices (to avoid constant GC)
	slicePool ByteSlicePooler

	// used to track the count of bytes that are (potentially) in RAM
	cacheLimiter CacheLimiter

	// for logging chunk state transitions
	chunkLogger ChunkStatusLogger

	// A factory to get hold of the file, in case we need to re-read any of it
	sourceFactory ChunkReaderSourceFactory

	// chunkId includes this chunk's start position (offset) in file
	chunkId ChunkID

	// number of bytes in this chunk
	length int64

	// position for Seek/Read
	positionInChunk int64

	// buffer used by prefetch
	buffer []byte
	// TODO: pooling of buffers to reduce pressure on GC?
}

func NewSingleChunkReader(ctx context.Context, sourceFactory ChunkReaderSourceFactory, chunkId ChunkID, length int64, chunkLogger ChunkStatusLogger, slicePool ByteSlicePooler, cacheLimiter CacheLimiter) SingleChunkReader {
	if length <= 0 {
		return &emptyChunkReader{}
	}
	return &singleChunkReader{
		ctx:           ctx,
		chunkLogger:   chunkLogger,
		slicePool:     slicePool,
		cacheLimiter:  cacheLimiter,
		sourceFactory: sourceFactory,
		chunkId:       chunkId,
		length:        length,
	}
}

func (cr *singleChunkReader) HasPrefetchedEntirelyZeros() bool {
	if cr.buffer == nil {
		return false // not prefetched  (and, to simply error handling in teh caller, we don't call retryBlockingPrefetchIfNecessary here)
	}

	for _, b := range cr.buffer {
		if b != 0 {
			return false // it's not all zeroes
		}
	}
	return true

	// note: we are not using this optimization: int64Slice := (*(*[]int64)(unsafe.Pointer(&rangeBytes)))[:len(rangeBytes)/8]
	//       Why?  Because (a) it only works when chunk size is divisible by 8, and that's not universally the case (e.g. last chunk in a file)
	//       and (b) some sources seem to imply that the middle of it should be &rangeBytes[0] instead of just &rangeBytes, so we'd want to
	//       check out the pros and cons of using the [0] before using it.
	//       and (c) we would want to check whether it really did offer meaningful real-world performance gain, before introducing use of unsafe.
}

// Prefetch the data in this chunk, using a file reader that is provided to us.
// (Allowing the caller to provide the reader to us allows a sequential read approach, since caller can control the order sequentially (in the initial, non-retry, scenario)
// We use io.ReaderAt, rather than io.Reader, just for maintainablity/ensuring correctness. (Since just using Reader requires the caller to
// follow certain assumptions about positioning the file pointer at the right place before calling us, but using ReaderAt does not).
func (cr *singleChunkReader) BlockingPrefetch(fileReader io.ReaderAt, isRetry bool) error {
	if cr.buffer != nil {
		return nil // already prefetched
	}

	// Block until we successfully add cr.length bytes to the app's current RAM allocation.
	// Must use "relaxed" RAM limit IFF this is a retry.  Else, we can, in theory, get deadlock with all active goroutines blocked
	// here doing retries, but no RAM _will_ become available because its
	// all used by queued chunkfuncs (that can't be processed because all goroutines are active).
	cr.chunkLogger.LogChunkStatus(cr.chunkId, EWaitReason.RAMToSchedule())
	err := cr.cacheLimiter.WaitUntilAddBytes(cr.ctx, cr.length, func() bool { return isRetry })
	if err != nil {
		return err
	}

	// get buffer from pool
	cr.buffer = cr.slicePool.RentSlice(uint32(cr.length))

	// read bytes into the buffer
	cr.chunkLogger.LogChunkStatus(cr.chunkId, EWaitReason.Disk())
	totalBytesRead, err := fileReader.ReadAt(cr.buffer, cr.chunkId.OffsetInFile)
	if err != nil && err != io.EOF {
		return err
	}
	if int64(totalBytesRead) != cr.length {
		return errors.New("bytes read not equal to expected length. Chunk reader must be constructed so that it won't read past end of file")
	}

	return nil
}

func (cr *singleChunkReader) retryBlockingPrefetchIfNecessary() error {
	if cr.buffer != nil {
		return nil // nothing to do
	}

	// create a new reader for the file (since anything that was passed to our Prefetch routine before was, deliberately, not kept)
	sourceFile, err := cr.sourceFactory()
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	// no need to seek first, because its a ReaderAt
	const isRetry = true // retries are the only time we need to redo the prefetch
	return cr.BlockingPrefetch(sourceFile, isRetry)
}

// Seeks within this chunk
// Seeking is used for retries, and also by some code to get length (by seeking to end)
func (cr *singleChunkReader) Seek(offset int64, whence int) (int64, error) {

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
func (cr *singleChunkReader) Read(p []byte) (n int, err error) {
	// This is a normal read, so free the prefetch buffer when hit EOF (i.e. end of this chunk).
	// We do so on the assumption that if we've read to the end we don't need the prefetched data any longer.
	// (If later, there's a retry that forces seek back to start and re-read, we'll automatically trigger a re-fetch at that time)
	return cr.doRead(p, true)
}

func (cr *singleChunkReader) doRead(p []byte, freeBufferOnEof bool) (n int, err error) {
	// check for EOF, BEFORE we ensure prefetch
	// (Otherwise, some readers can call us after EOF, and we end up re-pre-fetching unnecessarily)
	if cr.positionInChunk >= cr.length {
		return 0, io.EOF
	}

	// Always use the prefetch logic to read the data
	// This is simpler to maintain than using a different code path for the (rare) cases
	// where there has been no prefetch before this routine is called
	err = cr.retryBlockingPrefetchIfNecessary()
	if err != nil {
		return 0, err
	}

	// Copy the data across
	bytesCopied := copy(p, cr.buffer[cr.positionInChunk:])
	cr.positionInChunk += int64(bytesCopied)

	// check for EOF
	isEof := cr.positionInChunk >= cr.length
	if isEof {
		if freeBufferOnEof {
			cr.returnBuffer()
		}
		return bytesCopied, io.EOF
	}

	return bytesCopied, nil
}

func (cr *singleChunkReader) returnBuffer() {
	if cr.buffer == nil {
		return
	}
	cr.slicePool.ReturnSlice(cr.buffer)
	cr.cacheLimiter.RemoveBytes(int64(len(cr.buffer)))
	cr.buffer = nil
}

func (cr *singleChunkReader) Length() int64 {
	return cr.length
}

// Some code paths can call this, when cleaning up. (Even though in the normal, non error, code path, we don't NEED this
// because we close at the completion of a successful read of the whole prefetch buffer.
// We still want this though, to handle cases where for some reason the transfer stops before all the buffer has been read.)
// Without this close, if something failed part way through, we would keep counting this object's bytes in cacheLimiter
// "for ever", even after the object is gone.
func (cr *singleChunkReader) Close() error {
	cr.returnBuffer()
	return nil
}

// Grab the leading bytes, for later MIME type recognition
// (else we would have to re-read the start of the file later, and that breaks our rule to use sequential
// reads as much as possible)
func (cr *singleChunkReader) CaptureLeadingBytes() []byte {
	const mimeRecgonitionLen = 512
	leadingBytes := make([]byte, mimeRecgonitionLen)
	n, err := cr.doRead(leadingBytes, false) // do NOT free bufferOnEOF. So that if its a very small file, and we hit the end, we won't needlessly discard the prefetched data
	if err != nil && err != io.EOF {
		return nil // we just can't sniff the mime type
	}
	if n < len(leadingBytes) {
		// truncate if we read less than expected (very small file, so err was EOF above)
		leadingBytes = leadingBytes[:n]
	}
	// MUST re-wind, so that the bytes we read will get transferred too!
	cr.Seek(0, io.SeekStart)
	return leadingBytes
}
