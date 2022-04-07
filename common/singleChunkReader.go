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
	"hash"
	"io"
	"runtime"
	"sync"

	"github.com/Azure/azure-pipeline-go/pipeline"
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

	// GetPrologueState is used to grab enough of the initial bytes to do MIME-type detection.  Expected to be called only
	// on the first chunk in each file (since there's no point in calling it on others)
	// There is deliberately no error return value from the Prologue.
	// If it failed, the Prologue itself must call jptm.FailActiveSend.
	GetPrologueState() PrologueState

	// Length is the number of bytes in the chunk
	Length() int64

	// HasPrefectchedEntirelyZeros gives an indication of whether this chunk is entirely zeros.  If it returns true
	// then the chunk content has been prefetched AND it was all zeroes. For some remote destinations, that support "sparse file"
	// semantics, it is safe and correct to skip the upload of those chunks where this returns true.
	// In the rare edge case where this returns false due to the prefetch having failed (rather than the contents being non-zero),
	// we'll just treat it as a non-zero chunk. That's simpler (to code, to review and to test) than having this code force a prefetch.
	HasPrefetchedEntirelyZeros() bool

	// WriteBufferTo writes the entire contents of the prefetched buffer to h
	// Panics if the internal buffer has not been prefetched (or if its been discarded after a complete Read)
	WriteBufferTo(h hash.Hash)
}

// Simple aggregation of existing io interfaces
type CloseableReaderAt interface {
	io.ReaderAt
	io.Closer
}

// Factory method for data source for singleChunkReader
type ChunkReaderSourceFactory func() (CloseableReaderAt, error)

func DocumentationForDependencyOnChangeDetection() {
	// This function does nothing, except remind you to read the following, which is essential
	// to the correctness of AzCopy.

	// *** If the code that calls this singleChunkReader reads to the end of the buffer, then
	//     closeBuffer will automatically be called. If the calling subsequently Seeks back to
	//     the start, and Reads again, then singleChunkReader will re-retrieve the data from disk.
	//     That's exactly what happens if a chunk upload fails, and AzCopy's HTTP pipeline does a retry.
	//
	//     Here's the problem: There is no guarantee that the data obtained from the re-read matches the
	//     data that was retrieved the first time. It might be different. Specifically the data from the
	//     second disk read might be different from the first if some other process modified the file).
	//     And, importantly, AzCopy uses only the FIRST version when computing the MD5 hash of the file.
	//
	//	   So if the file changed, we'll upload the new version, but use the hash from the old one. That's
	//     clearly unacceptable because the hash will be invalid.
	//
	//     To solve that, we rely on our change detection logic in ste/xfer-anyToRemote-file.go/epilogueWithCleanupSendToRemote.
	//     If some other process has changed the file, then that change detection logic will kick in,
	//     and fail the transfer.  Therefore no _successful_ transfer will suffer from
	//     a re-read chunk being different from what was hashed.
	//
	//     Search for usages of this function DocumentationForDependencyOnChangeDetection(), to see all places that
	//     have been "commented" with a "call" to it.
	//
	//     Why do we re-read from disk like this? Because if we didn't we'd have to keep every chunk in RAM until the
	//     Storage Service acknowledged each request, and we assume that would substantially increase RAM usage.

	// Why do we have a function just for documentation? Because (a) the function gives us a compiler-checked
	// way to refer to the documentation from all relevant places, (b) IDEs can find usages of the function to
	// see where its referenced and (c) this is so important to the correctness of AzCopy, that it seemed sensible
	// to do something that was hard to ignore.
}

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

	// general-purpose logger
	generalLogger ILogger

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

	// muMaster locks everything for single-threaded use...
	muMaster *sync.Mutex

	// ... except muMaster doesn't lock Close(), which can be called at the same time as reads (pipeline.Do calls it in cases where the context has been cancelled)
	// It could be argued that we only need muClose (since that's the only case where we knowingly call two methods at the same time - Close while a Read is in progress -
	// but it seems cleaner to also lock overall with muMaster rather than making weird assumptions about how we are called - concurrently or not)
	muClose *sync.Mutex

	isClosed bool
}

func NewSingleChunkReader(ctx context.Context, sourceFactory ChunkReaderSourceFactory, chunkId ChunkID, length int64, chunkLogger ChunkStatusLogger, generalLogger ILogger, slicePool ByteSlicePooler, cacheLimiter CacheLimiter) SingleChunkReader {
	if length <= 0 {
		return &emptyChunkReader{}
	}
	return &singleChunkReader{
		muMaster:      &sync.Mutex{},
		muClose:       &sync.Mutex{},
		ctx:           ctx,
		chunkLogger:   chunkLogger,
		generalLogger: generalLogger,
		slicePool:     slicePool,
		cacheLimiter:  cacheLimiter,
		sourceFactory: sourceFactory,
		chunkId:       chunkId,
		length:        length,
	}
}

func (cr *singleChunkReader) use() {
	cr.muMaster.Lock()
	cr.muClose.Lock()
}

func (cr *singleChunkReader) unuse() {
	cr.muClose.Unlock()
	cr.muMaster.Unlock()
}

func (cr *singleChunkReader) HasPrefetchedEntirelyZeros() bool {
	cr.use()
	defer cr.unuse()

	if cr.buffer == nil {
		return false // not prefetched (and, to simply error handling in the caller, we don't call retryBlockingPrefetchIfNecessary here)
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

func (cr *singleChunkReader) BlockingPrefetch(fileReader io.ReaderAt, isRetry bool) error {
	cr.use()
	defer cr.unuse()

	return cr.blockingPrefetch(fileReader, isRetry)
}

// Prefetch the data in this chunk, using a file reader that is provided to us.
// (Allowing the caller to provide the reader to us allows a sequential read approach, since caller can control the order sequentially (in the initial, non-retry, scenario)
// We use io.ReaderAt, rather than io.Reader, just for maintainablity/ensuring correctness. (Since just using Reader requires the caller to
// follow certain assumptions about positioning the file pointer at the right place before calling us, but using ReaderAt does not).
func (cr *singleChunkReader) blockingPrefetch(fileReader io.ReaderAt, isRetry bool) error {
	if cr.buffer != nil {
		return nil // already prefetched
	}

	// Block until we successfully add cr.length bytes to the app's current RAM allocation.
	// Must use "relaxed" RAM limit IFF this is a retry.  Else, we can, in theory, get deadlock with all active goroutines blocked
	// here doing retries, but no RAM _will_ become available because its
	// all used by queued chunkfuncs (that can't be processed because all goroutines are active).
	if cr.chunkLogger != nil {
		cr.chunkLogger.LogChunkStatus(cr.chunkId, EWaitReason.RAMToSchedule())
	}

	err := cr.cacheLimiter.WaitUntilAdd(cr.ctx, cr.length, func() bool { return isRetry })
	if err != nil {
		return err
	}

	// prepare to read
	if cr.chunkLogger != nil {
		cr.chunkLogger.LogChunkStatus(cr.chunkId, EWaitReason.DiskIO())
	}

	targetBuffer := cr.slicePool.RentSlice(cr.length)

	// read WITHOUT holding the "close" lock.  While we don't have the lock, we mutate ONLY local variables, no instance state.
	// (Don't release the other lock, muMaster, since that's unnecessary would make it harder to reason about behaviour - e.g. is something other than Close happening?)
	cr.muClose.Unlock()
	n, readErr := fileReader.ReadAt(targetBuffer, cr.chunkId.OffsetInFile())
	cr.muClose.Lock()

	// now that we have the lock again, see if any error means we can't continue
	if readErr == nil {
		if cr.isClosed {
			readErr = errors.New("closed while reading")
		} else if cr.ctx.Err() != nil {
			readErr = cr.ctx.Err() // context cancelled
		} else if int64(n) != cr.length {
			readErr = errors.New("bytes read not equal to expected length. Chunk reader must be constructed so that it won't read past end of file")
		}
	}
	// return the revised error, if any
	if readErr != nil {
		cr.returnSlice(targetBuffer)
		return readErr
	}

	// We can continue, so use the data we have read
	cr.buffer = targetBuffer
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
	return cr.blockingPrefetch(sourceFile, isRetry)
}

// Seeks within this chunk
// Seeking is used for retries, and also by some code to get length (by seeking to end).
func (cr *singleChunkReader) Seek(offset int64, whence int) (int64, error) {
	DocumentationForDependencyOnChangeDetection() // <-- read the documentation here

	cr.use()
	defer cr.unuse()

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

// Reads from within this chunk.
func (cr *singleChunkReader) Read(p []byte) (n int, err error) {
	DocumentationForDependencyOnChangeDetection() // <-- read the documentation here

	cr.use()
	defer cr.unuse()

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

	// extra checks to be safe (originally for https://github.com/Azure/azure-storage-azcopy/issues/191)
	// No longer needed now that use/unuse lock with a mutex, but there's no harm in leaving them here
	if cr.buffer == nil {
		panic("unexpected nil buffer")
	}
	if cr.positionInChunk >= cr.length {
		panic("unexpected EOF")
	}
	if cr.length != int64(len(cr.buffer)) {
		panic("unexpected buffer length discrepancy")
	}

	// Copy the data across
	bytesCopied := copy(p, cr.buffer[cr.positionInChunk:])
	cr.positionInChunk += int64(bytesCopied)

	// check for EOF
	isEof := cr.positionInChunk >= cr.length
	if isEof {
		if freeBufferOnEof {
			cr.closeBuffer()
		}
		return bytesCopied, io.EOF
	}

	return bytesCopied, nil
}

// Disposes of the buffer to save RAM.
func (cr *singleChunkReader) closeBuffer() {
	DocumentationForDependencyOnChangeDetection() // <-- read the documentation here

	if cr.buffer == nil {
		return
	}
	cr.returnSlice(cr.buffer)
	cr.buffer = nil
}

func (cr *singleChunkReader) returnSlice(slice []byte) {
	cr.slicePool.ReturnSlice(slice)
	cr.cacheLimiter.Remove(int64(len(slice)))
}

func (cr *singleChunkReader) Length() int64 {
	cr.use()
	defer cr.unuse()

	return cr.length
}

// Some code paths can call this, when cleaning up. (Even though in the normal, non error, code path, we don't NEED this
// because we close at the completion of a successful read of the whole prefetch buffer.
// We still want this though, to handle cases where for some reason the transfer stops before all the buffer has been read.)
// Without this close, if something failed part way through, we would keep counting this object's bytes in cacheLimiter
// "for ever", even after the object is gone.
func (cr *singleChunkReader) Close() error {
	// First, check and log early closes
	// This check originates from issue 191. Even tho we think we've now resolved that issue,
	// we'll keep this code just to make sure.
	if cr.positionInChunk < cr.length && cr.ctx.Err() == nil {
		cr.generalLogger.Log(pipeline.LogInfo, "Early close of chunk in singleChunkReader with context still active")
		// cannot panic here, since this code path is NORMAL in the case of sparse files to Azure Files and Page Blobs
	}

	// Only acquire the Close mutex (it will be free if the prefetch method is in the middle of a disk read)
	// Don't acquire muMaster, which will not be free in that situation
	cr.muClose.Lock()
	defer cr.muClose.Unlock()

	// do the real work
	cr.closeBuffer()
	cr.isClosed = true

	/*
	 * Set chunkLogger to nil, so that chunkStatusLogger can be GC'ed.
	 *
	 * TODO: We should not need to explicitly set this to nil but today we have a yet-unknown ref on cr which
	 *       is leaking this "big" chunkStatusLogger memory, so we cause that to be freed by force dropping this ref.
	 *
	 * Note: We are force setting this to nil and we safe guard against this by checking chunklogger not nil at respective places.
	 *       At present this is called only from blockingPrefetch().
	 */
	cr.chunkLogger = nil

	return nil
}

// Grab the leading bytes, for later MIME type recognition
// (else we would have to re-read the start of the file later, and that breaks our rule to use sequential
// reads as much as possible)
func (cr *singleChunkReader) GetPrologueState() PrologueState {
	cr.use()
	// can't defer unuse here. See explicit calls (plural) below

	const mimeRecgonitionLen = 512
	leadingBytes := make([]byte, mimeRecgonitionLen)
	n, err := cr.doRead(leadingBytes, false) // do NOT free bufferOnEOF. So that if its a very small file, and we hit the end, we won't needlessly discard the prefetched data
	if err != nil && err != io.EOF {
		cr.unuse()
		return PrologueState{} // empty return value, because we just can't sniff the mime type
	}
	if n < len(leadingBytes) {
		// truncate if we read less than expected (very small file, so err was EOF above)
		leadingBytes = leadingBytes[:n]
	}
	// unuse before Seek, since Seek is public
	cr.unuse()
	// MUST re-wind, so that the bytes we read will get transferred too!
	_, err = cr.Seek(0, io.SeekStart)
	return PrologueState{LeadingBytes: leadingBytes}
}

// Writes the buffer to a hasher. Does not alter positionInChunk
func (cr *singleChunkReader) WriteBufferTo(h hash.Hash) {
	DocumentationForDependencyOnChangeDetection() // <-- read the documentation here

	cr.use()
	defer cr.unuse()

	if cr.buffer == nil {
		panic("invalid state. No prefetch buffer is present")
	}
	_, err := h.Write(cr.buffer)
	if err != nil {
		panic("documentation of hash.Hash.Write says it will never return an error")
	}
}

func stack() []byte {
	buf := make([]byte, 2048)
	for {
		n := runtime.Stack(buf, false)
		if n < len(buf) {
			return buf[:n]
		}
		buf = make([]byte, 2*len(buf))
	}
}
