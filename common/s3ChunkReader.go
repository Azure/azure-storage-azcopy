// Author : Parmananda Banakar
package common

import (
	"context"
	"errors"
	"fmt"
	"hash"
	"io"
	"sync"
	"time"
)

// IRemoteSourceInfoProvider interface is imported from ste package
// We'll use an interface{} for now to avoid circular imports, and cast it in the implementation
type S3ChunkReader struct {
	ctx             context.Context
	slicePool       ByteSlicePooler
	cacheLimiter    CacheLimiter
	chunkLogger     ChunkStatusLogger
	generalLogger   ILogger
	chunkId         ChunkID // <-- Add this for offset/length/logging
	length          int64
	positionInChunk int64
	buffer          []byte
	muMaster        *sync.Mutex
	muClose         *sync.Mutex
	isClosed        bool

	// S3-specific - source info provider that implements GetObjectRange
	sourceInfoProvider interface {
		GetObjectRange(offset, length int64) (io.ReadCloser, error)
	}
}

type S3ChunkReaderSourceFactory func() (interface{}, error)

func NewS3ChunkReader(
	ctx context.Context,
	sourceInfoProvider interface {
		GetObjectRange(offset, length int64) (io.ReadCloser, error)
	},
	chunkId ChunkID,
	length int64,
	chunkLogger ChunkStatusLogger,
	generalLogger ILogger,
	slicePool ByteSlicePooler,
	cacheLimiter CacheLimiter,
) SingleChunkReader {
	if length <= 0 {
		return &emptyChunkReader{}
	}
	return &S3ChunkReader{
		ctx:                ctx,
		sourceInfoProvider: sourceInfoProvider,
		chunkId:            chunkId,
		length:             length,
		chunkLogger:        chunkLogger,
		generalLogger:      generalLogger,
		slicePool:          slicePool,
		cacheLimiter:       cacheLimiter,
		muMaster:           &sync.Mutex{},
		muClose:            &sync.Mutex{},
	}
}

func (cr *S3ChunkReader) use() {
	cr.muMaster.Lock()
	cr.muClose.Lock()
}

func (cr *S3ChunkReader) unuse() {
	cr.muClose.Unlock()
	cr.muMaster.Unlock()
}

func (cr *S3ChunkReader) BlockingPrefetch(_ io.ReaderAt, isRetry bool) error {
	cr.use()
	defer cr.unuse()
	GetLifecycleMgr().Info(fmt.Sprintf("BlockingPrefetch called for chunkId: %v, length: %d, isRetry: %v", cr.chunkId, cr.length, isRetry))

	if cr.buffer != nil {
		GetLifecycleMgr().Info(fmt.Sprintf("BlockingPrefetch: Buffer already prefetched for chunkId: %v", cr.chunkId))
		return nil // already prefetched
	}

	if cr.chunkLogger != nil {
		cr.chunkLogger.LogChunkStatus(cr.chunkId, EWaitReason.RAMToSchedule())
	}
	err := cr.cacheLimiter.WaitUntilAdd(cr.ctx, cr.length, func() bool { return isRetry })
	if err != nil {
		GetLifecycleMgr().Info(fmt.Sprintf("s3 BlockingPrefetch: cacheLimiter.WaitUntilAdd failed for chunkId: %v, err: %v", cr.chunkId, err))
		return err
	}

	if cr.chunkLogger != nil {
		cr.chunkLogger.LogChunkStatus(cr.chunkId, EWaitReason.DiskIO())
	}

	targetBuffer := cr.slicePool.RentSlice(cr.length)
	GetLifecycleMgr().Info(fmt.Sprintf("S3ChunkReader: Allocated buffer of length: %d for chunkId: %v", cr.length, cr.chunkId))

	// release close lock for network call
	cr.muClose.Unlock()

	var n int
	var readErr error
	var body io.ReadCloser

	startGet := time.Now()
	if !isRetry {
		body, err = cr.sourceInfoProvider.GetObjectRange(cr.chunkId.offsetInFile, cr.length)
	} else {
		// Retryable path: use WithNetworkRetry to centralize backoff/retry semantics.
		body, err = WithNetworkRetry(
			cr.ctx,
			nil,
			fmt.Sprintf("BlockingPrefetch %v", cr.chunkId),
			func() (io.ReadCloser, error) {
				return cr.sourceInfoProvider.GetObjectRange(cr.chunkId.offsetInFile, cr.length)
			})
	}
	endGet := time.Now()
	GetLifecycleMgr().Info(fmt.Sprintf("S3ChunkReader:GetObjectRange chunk=%v start=%v end=%v duration=%v err=%v",
		cr.chunkId, startGet.Format(time.RFC3339Nano), endGet.Format(time.RFC3339Nano), endGet.Sub(startGet), err))

	if err == nil {
		func() {
			defer func() {
				if body != nil {
					body.Close()
				}
			}()
			n, readErr = io.ReadFull(body, targetBuffer)
		}()
	}

	cr.muClose.Lock()
	// cleanup
	cr.slicePool.ReturnSlice(targetBuffer)
	cr.cacheLimiter.Remove(cr.length)

	if err != nil {
		GetLifecycleMgr().Info(fmt.Sprintf("S3ChunkReader: GetObjectRange failed for chunkId: %v err %v", cr.chunkId, err))
		return err
	}
	if readErr != nil {
		GetLifecycleMgr().Info(fmt.Sprintf("S3ChunkReader: ReadFull failed for chunkId: %v readErr: %v", cr.chunkId, readErr))
		return readErr
	}
	if int64(n) != cr.length {
		err := fmt.Errorf("bytes read not equal to expected length: got %d expected %d", n, cr.length)
		GetLifecycleMgr().Info(fmt.Sprintf("S3ChunkReader: Read length mismatch for chunkId: %v err: %v", cr.chunkId, err))
		return err
	}
	// success
	cr.buffer = targetBuffer
	GetLifecycleMgr().Info(fmt.Sprintf("S3ChunkReader: Successfully fetched %d bytes from S3 and stored in buffer for chunkId: %v", n, cr.chunkId))
	return nil
}

func (cr *S3ChunkReader) retryBlockingPrefetchIfNecessary() error {
	if cr.buffer != nil {
		return nil // nothing to do
	}
	// For S3, just call BlockingPrefetch again with isRetry=true
	return cr.BlockingPrefetch(nil, true)
}

// Seeks within this chunk
// Seeking is used for retries, and also by some code to get length (by seeking to end).
func (cr *S3ChunkReader) Seek(offset int64, whence int) (int64, error) {
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
func (cr *S3ChunkReader) Read(p []byte) (n int, err error) {
	DocumentationForDependencyOnChangeDetection() // <-- read the documentation here

	cr.use()
	defer cr.unuse()

	// This is a normal read, so free the prefetch buffer when hit EOF (i.e. end of this chunk).
	// We do so on the assumption that if we've read to the end we don't need the prefetched data any longer.
	// (If later, there's a retry that forces seek back to start and re-read, we'll automatically trigger a re-fetch at that time)
	return cr.doRead(p, true)
}

func (cr *S3ChunkReader) doRead(p []byte, freeBufferOnEof bool) (n int, err error) {
	//GetLifecycleMgr().Info(fmt.Sprintf("S3ChunkReader: doRead called for chunkId: %v, positionInChunk: %d, length: %d", cr.chunkId, cr.positionInChunk, cr.length))
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
			//GetLifecycleMgr().Info(fmt.Sprintf("S3ChunkReader: closeBuffer is called for chunkId: %v", cr.chunkId))
			cr.closeBuffer()
		}
		return bytesCopied, io.EOF
	}

	return bytesCopied, nil
}

// Disposes of the buffer to save RAM.
func (cr *S3ChunkReader) closeBuffer() {
	DocumentationForDependencyOnChangeDetection() // <-- read the documentation here
	//GetLifecycleMgr().Info(fmt.Sprintf("S3ChunkReader: closeBuffer called for chunkId: %v", cr.chunkId))
	if cr.buffer == nil {
		return
	}
	cr.returnSlice(cr.buffer)
	cr.buffer = nil
}

func (cr *S3ChunkReader) returnSlice(slice []byte) {
	//GetLifecycleMgr().Info(fmt.Sprintf("S3ChunkReader: returnSlice called for chunkId: %v", cr.chunkId))
	cr.slicePool.ReturnSlice(slice)
	cr.cacheLimiter.Remove(int64(len(slice)))
}

func (r *S3ChunkReader) Length() int64 {
	return r.length
}

// Some code paths can call this, when cleaning up. (Even though in the normal, non error, code path, we don't NEED this
// because we close at the completion of a successful read of the whole prefetch buffer.
// We still want this though, to handle cases where for some reason the transfer stops before all the buffer has been read.)
// Without this close, if something failed part way through, we would keep counting this object's bytes in cacheLimiter
// "for ever", even after the object is gone.
func (cr *S3ChunkReader) Close() error {
	// First, check and log early closes
	// This check originates from issue 191. Even tho we think we've now resolved that issue,
	// we'll keep this code just to make sure.
	if cr.positionInChunk < cr.length && cr.ctx.Err() == nil {
		cr.generalLogger.Log(LogInfo, "Early close of chunk in S3ChunkReader with context still active")
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
func (cr *S3ChunkReader) GetPrologueState() PrologueState {
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
	_, _ = cr.Seek(0, io.SeekStart)
	return PrologueState{LeadingBytes: leadingBytes}
}

func (cr *S3ChunkReader) HasPrefetchedEntirelyZeros() bool {
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

// Writes the buffer to a hasher. Does not alter positionInChunk
func (cr *S3ChunkReader) WriteBufferTo(h hash.Hash) {
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
