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
	"crypto/md5"
	"errors"
	"hash"
	"io"
	"math"
	"sync/atomic"
	"time"
)

// Used to write all the chunks to a disk file
type ChunkedFileWriter interface {

	// WaitToScheduleChunk blocks until enough RAM is available to handle the given chunk, then it
	// "reserves" that amount of RAM in the CacheLimiter and returns.
	WaitToScheduleChunk(ctx context.Context, id ChunkID, chunkSize int64) error

	// EnqueueChunk hands the given chunkContents over to the ChunkedFileWriter, to be written to disk.
	// Because ChunkedFileWriter writes sequentially, the actual time of writing is not known to the caller.
	// All the caller knows, is that responsibility for writing the chunk has been passed to the ChunkedFileWriter.
	// While any error may be returned immediately, errors are more likely to be returned later, on either a subsequent
	// call to this routine or on the final return to Flush.
	// After the chunk is written to disk, its reserved memory byte allocation is automatically subtracted from the CacheLimiter.
	EnqueueChunk(ctx context.Context, id ChunkID, chunkSize int64, chunkContents io.Reader, retryable bool) error

	// Flush will block until all the chunks have been written to disk.  err will be non-nil if and only in any chunk failed to write.
	// Flush must be called exactly once, after all chunks have been enqueued with EnqueueChunk.
	Flush(ctx context.Context) (md5HashOfFileAsWritten []byte, err error)

	// MaxRetryPerDownloadBody returns the maximum number of retries that will be done for the download of a single chunk body
	MaxRetryPerDownloadBody() int
}

type chunkedFileWriter struct {
	// NOTE: for the 64 bit atomic functions to work on a 32 bit system, we have to guarantee the right 64-bit alignment
	// so the 64 bit integers are placed first in the struct to avoid future breaks
	// refer to: https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	// all time received count for this instance
	totalChunkReceiveMilliseconds int64
	totalReceivedChunkCount       int32

	// the file we are writing to (type as interface to somewhat abstract away io.File - e.g. for unit testing)
	file io.WriteCloser

	// pool of byte slices (to avoid constant GC)
	slicePool ByteSlicePooler

	// used to track the count of bytes that are (potentially) in RAM
	cacheLimiter CacheLimiter

	// for logging chunk state transitions
	chunkLogger ChunkStatusLogger

	// file chunks that have arrived and not been sorted yet
	newUnorderedChunks chan fileChunk

	// used to control scheduling of new chunks against this file,
	// to make sure we don't get too many sitting in RAM all waiting to be
	// saved at the same time
	activeChunkCount int32

	// used for completion
	successMd5   chan []byte
	failureError chan error

	// controls body-read retries. Public so value can be shared with retryReader
	maxRetryPerDownloadBody int

	// how will hashes be validated?
	md5ValidationOption HashValidationOption

	sourceMd5Exists bool
}

type fileChunk struct {
	id   ChunkID
	data []byte
}

func NewChunkedFileWriter(ctx context.Context, slicePool ByteSlicePooler, cacheLimiter CacheLimiter, chunkLogger ChunkStatusLogger, file io.WriteCloser, numChunks uint32, maxBodyRetries int, md5ValidationOption HashValidationOption, sourceMd5Exists bool) ChunkedFileWriter {
	// Set max size for buffered channel. The upper limit here is believed to be generous, given worker routine drains it constantly.
	// Use num chunks in file if lower than the upper limit, to prevent allocating RAM for lots of large channel buffers when dealing with
	// very large numbers of very small files.
	chanBufferSize := int(math.Min(float64(numChunks), 1000))

	w := &chunkedFileWriter{
		file:                    file,
		slicePool:               slicePool,
		cacheLimiter:            cacheLimiter,
		chunkLogger:             chunkLogger,
		successMd5:              make(chan []byte),
		failureError:            make(chan error, 1),
		newUnorderedChunks:      make(chan fileChunk, chanBufferSize),
		maxRetryPerDownloadBody: maxBodyRetries,
		md5ValidationOption:     md5ValidationOption,
		sourceMd5Exists:         sourceMd5Exists,
	}
	go w.workerRoutine(ctx)
	return w
}

var ChunkWriterAlreadyFailed = errors.New("chunk Writer already failed")

const maxDesirableActiveChunks = 20 // TODO: can we find a sensible way to remove the hard-coded count threshold here?

// Waits until we have enough RAM, within our pre-determined allocation, to accommodate the chunk.
// After any necessary wait, it updates the count of scheduled-but-unsaved bytes
// Note: we considered tracking only received-but-unsaved-bytes (i.e. increment the count at time of making the
// request to the remote data source) but decided it was simpler and no less effective to increment the count
// at the time of scheduling the chunk (which is when this routine should be called).
// Is here, as method of this struct, for symmetry with the point where we remove it's count
// from the cache limiter, which is also in this struct.
func (w *chunkedFileWriter) WaitToScheduleChunk(ctx context.Context, id ChunkID, chunkSize int64) error {
	w.chunkLogger.LogChunkStatus(id, EWaitReason.RAMToSchedule())
	err := w.cacheLimiter.WaitUntilAdd(ctx, chunkSize, w.shouldUseRelaxedRamThreshold)
	if err == nil {
		atomic.AddInt32(&w.activeChunkCount, 1)
	}
	return err
}

// Threadsafe method to enqueue a new chunk for processing
func (w *chunkedFileWriter) EnqueueChunk(ctx context.Context, id ChunkID, chunkSize int64, chunkContents io.Reader, retryable bool) error {

	readDone := make(chan struct{})
	if retryable {
		// if retryable == true, that tells us that closing the reader
		// is a safe way to force this particular reader to retry.
		// (Typically this means it forces the reader to make one iteration around its internal retry loop.
		// Going around that loop is hidden to the normal Read code (unless it exceeds the retry count threshold))
		closer := chunkContents.(io.Closer).Close // do the type assertion now, so get panic if it's not compatible.  If we left it to the last minute, then the type would only be verified on the rare occasions when retries are required
		retryForcer := func() { _ = closer() }
		w.setupProgressMonitoring(readDone, id, chunkSize, retryForcer)
	}

	// read into a buffer
	buffer := w.slicePool.RentSlice(chunkSize)
	readStart := time.Now()
	_, err := io.ReadFull(chunkContents, buffer)
	close(readDone)
	if err != nil {
		return err
	}

	// count it (since we fully "have" it now - just haven't sorted and saved it yet)
	atomic.AddInt32(&w.totalReceivedChunkCount, 1)
	atomic.AddInt64(&w.totalChunkReceiveMilliseconds, time.Since(readStart).Nanoseconds()/(1000*1000))

	// enqueue it
	w.chunkLogger.LogChunkStatus(id, EWaitReason.Sorting())
	select {
	case err = <-w.failureError:
		if err != nil {
			return err
		}
		return ChunkWriterAlreadyFailed // channel returned nil because it was closed and empty
	case <-ctx.Done():
		return ctx.Err()
	case w.newUnorderedChunks <- fileChunk{id: id, data: buffer}:
		return nil
	}
}

// Flush waits until all chunks have been flush to disk, then returns the MD5 has of the file's bytes-as-we-saved-them
func (w *chunkedFileWriter) Flush(ctx context.Context) ([]byte, error) {
	// let worker know that no more will be coming
	close(w.newUnorderedChunks)

	// wait until all written to disk
	select {
	case err := <-w.failureError:
		if err != nil {
			return nil, err
		}
		return nil, ChunkWriterAlreadyFailed // channel returned nil because it was closed and empty
	case <-ctx.Done():
		return nil, ctx.Err()
	case md5AtCompletion := <-w.successMd5:
		return md5AtCompletion, nil
	}
}

// Used so that callers can set their retry readers to the same retry count as what we are using here
func (w *chunkedFileWriter) MaxRetryPerDownloadBody() int {
	return w.maxRetryPerDownloadBody
}

// Each fileChunkWriter needs exactly one goroutine running this, to service the channel and save the data
// This routine orders the data sequentially, so that (a) we can get maximum performance without
// resorting to the likes of SetFileValidData (https://docs.microsoft.com/en-us/windows/desktop/api/fileapi/nf-fileapi-setfilevaliddata)
// and (b) we can compute MD5 hashes - which can only be computed when moving through the data sequentially
func (w *chunkedFileWriter) workerRoutine(ctx context.Context) {
	nextOffsetToSave := int64(0)
	unsavedChunksByFileOffset := make(map[int64]fileChunk)
	md5Hasher := md5.New()
	if w.md5ValidationOption == EHashValidationOption.NoCheck() || !w.sourceMd5Exists {
		// save CPU time by not even computing a hash, if we don't want to check it, or have nothing to check it against
		md5Hasher = &nullHasher{}
	}

	for {
		var newChunk fileChunk
		var channelIsOpen bool

		// await new chunk (or cancellation)
		select {
		case newChunk, channelIsOpen = <-w.newUnorderedChunks:
			if !channelIsOpen {
				// If channel is closed, we know that flush as been called and we have read everything
				// So we are finished
				// We know there was no error, because if there was an error we would have returned before now
				w.successMd5 <- md5Hasher.Sum(nil)
				return
			}
		case <-ctx.Done(): // If cancelled out in the middle of enqueuing chunks OR processing chunks, they will both cleanly cancel out and we'll get back to here.
			w.failureError <- ctx.Err()
			return
		}

		// index the new chunk
		unsavedChunksByFileOffset[newChunk.id.OffsetInFile()] = newChunk
		w.chunkLogger.LogChunkStatus(newChunk.id, EWaitReason.PriorChunk()) // may have to wait on prior chunks to arrive

		// Process all chunks that we can
		w.setStatusForContiguousAvailableChunks(unsavedChunksByFileOffset, nextOffsetToSave, ctx) // update states of those that have all their prior ones already here
		err := w.sequentiallyProcessAvailableChunks(unsavedChunksByFileOffset, &nextOffsetToSave, md5Hasher, ctx)
		if err != nil {
			w.failureError <- err
			close(w.failureError) // must close because many goroutines may be calling the public methods, and all need to be able to tell there's been an error, even tho only one will get the actual error
			return                // no point in processing any more after a failure
		}
	}
}

// Hashes and saves available chunks that are sequential from nextOffsetToSave. Stops and returns as soon as it hits
// a gap (i.e. the position of a chunk that hasn't arrived yet)
func (w *chunkedFileWriter) sequentiallyProcessAvailableChunks(unsavedChunksByFileOffset map[int64]fileChunk, nextOffsetToSave *int64, md5Hasher hash.Hash, ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil // Break out of the loop if cancelled. Done can be checked multiple times, so it's safe to not error out.
		default:
		}

		// Look for next chunk in sequence
		nextChunkInSequence, exists := unsavedChunksByFileOffset[*nextOffsetToSave]
		if !exists {
			return nil //its not there yet. That's OK.
		}
		delete(unsavedChunksByFileOffset, *nextOffsetToSave)      // remove it
		*nextOffsetToSave += int64(len(nextChunkInSequence.data)) // update immediately so we won't forget!

		// Save it (hashing exactly what we save)
		err := w.saveOneChunk(nextChunkInSequence, md5Hasher)
		if err != nil {
			return err
		}
	}
}

// Advances the status of chunks which are no longer waiting on missing predecessors, but are instead just waiting on
// us to get around to (sequentially) saving them
func (w *chunkedFileWriter) setStatusForContiguousAvailableChunks(unsavedChunksByFileOffset map[int64]fileChunk, nextOffsetToSave int64, ctx context.Context) {
	for {
		// Check for ctx.Done at the start of the loop, and cleanly return if it's done.
		select {
		case <-ctx.Done(): // Break out of the loop if cancelled. Done can be checked multiple times, so it's safe to not error out.
			return
		default: // do nothing if ctx.Done is empty
		}

		nextChunkInSequence, exists := unsavedChunksByFileOffset[nextOffsetToSave]
		if !exists {
			return //its not there yet, so no need to touch anything AFTER it. THEY are still waiting for prior chunk
		}
		nextOffsetToSave += int64(len(nextChunkInSequence.data))
		w.chunkLogger.LogChunkStatus(nextChunkInSequence.id, EWaitReason.QueueToWrite()) // we WILL write this. Just may have to write others before it
	}
}

// Saves one chunk to its destination
func (w *chunkedFileWriter) saveOneChunk(chunk fileChunk, md5Hasher hash.Hash) error {
	defer func() {
		w.cacheLimiter.Remove(int64(len(chunk.data))) // remove this from the tally of scheduled-but-unsaved bytes
		atomic.AddInt32(&w.activeChunkCount, -1)
		w.slicePool.ReturnSlice(chunk.data)
		w.chunkLogger.LogChunkStatus(chunk.id, EWaitReason.ChunkDone()) // this chunk is all finished
	}()

	const maxWriteSize = 1024 * 1024

	w.chunkLogger.LogChunkStatus(chunk.id, EWaitReason.DiskIO())

	// in some cases, e.g. Storage Spaces in Azure VMs, chopping up the writes helps perf. TODO: look into the reasons why it helps
	for i := 0; i < len(chunk.data); i += maxWriteSize {
		slice := chunk.data[i:]
		if len(slice) > maxWriteSize {
			slice = slice[:maxWriteSize]
		}

		// always hash exactly what we save
		md5Hasher.Write(slice)
		_, err := w.file.Write(slice) // unlike Read, Write must process ALL the data, or have an error.  It can't return "early".
		if err != nil {
			return err
		}
	}

	return nil
}

// We use a less strict cache limit
// if we have relatively few chunks in progress for THIS file. Why? To try to spread
// the work in progress across a larger number of files, instead of having it
// get concentrated in one. I.e. when we have a lot of in-flight chunks for this file,
// we'll tend to prefer allocating for other files, with fewer in-flight
func (w *chunkedFileWriter) shouldUseRelaxedRamThreshold() bool {
	return atomic.LoadInt32(&w.activeChunkCount) <= maxDesirableActiveChunks
}

// Are we currently in a memory-constrained situation?
func (w *chunkedFileWriter) haveMemoryPressure(chunkSize int64) bool {
	didAdd := w.cacheLimiter.TryAdd(chunkSize, w.shouldUseRelaxedRamThreshold())
	if didAdd {
		w.cacheLimiter.Remove(chunkSize) // remove immediately, since this was only a test
	}
	return !didAdd
}

func (w *chunkedFileWriter) averageDurationPerChunk() time.Duration {
	const minCountForAverage = 4 // to ignore major anomalies
	count := atomic.LoadInt32(&w.totalReceivedChunkCount)
	if count < minCountForAverage {
		return 1000 * time.Hour // "huge"
	}
	avgMilliseconds := atomic.LoadInt64(&w.totalChunkReceiveMilliseconds) / int64(count)
	return time.Duration(avgMilliseconds) * time.Millisecond
}

// Looks to see if read operation is slow, and forces retry of body read if certain conditions are met.
// This is to work around the rare cases when some body reads are much slower than usual therefore they (a) hold
// up the sequential saving of subsequent chunks of the same file and/or (b) hold up completion of the whole job.
// By retrying the slow chunk, we usually get a fast read.
func (w *chunkedFileWriter) setupProgressMonitoring(readDone chan struct{}, id ChunkID, chunkSize int64, retryForcer func()) {
	if retryForcer == nil {
		panic("retryForcer is nil")
	}
	start := time.Now()
	initialReceivedCount := atomic.LoadInt32(&w.totalReceivedChunkCount)

	// Create parameters for exponential backoff such that, in only a small number of tries,
	// our timeout here gets very big.  Why? Because, if things really _are_ slow, e.g. on the network,
	// we don't want to keep forcing very frequent retries. We want to do one early if needed, but if that doesn't
	// result in fast completion, we want to back our checking frequency off very quickly and basically leave it alone.
	// Also, we need it to be steep because we are always measuring from "start", not from end of last polling loop
	initialWaitSeconds := float64(15) // arbitrarily selected, to give minimal impression of waiting, to user (in testing, 30 seconds did occasionally show total throughput drops of a new 10's of percent)
	base := float64(4)                // a steep exponential backoff

	// set up a conservative timeout threshold based on average throughput so far, but being more aggressive if job is in its final stages
	speedTimeoutBackoff := 1
	speedTimeout, isJobAboutToFinish := w.calcSpeedTimeout(speedTimeoutBackoff)

	// Run a goroutine to monitor progress and force retries when necessary
	// Note that the retries are transparent to the main body Read call, due to use of retry reader. I.e.
	// our external caller's call to Read just keeps on running, and the external caller never even knows the retry happened
	go func() {
		maxConfiguredRetries := w.maxRetryPerDownloadBody
		maxForcedRetries := maxConfiguredRetries - 1 // leave one retry unused by us, to keep it available for non-forced, REAL, errors (handled by retryReader)

		for try := 0; try < maxForcedRetries; try++ {

			memoryTimeout := time.Second * time.Duration(initialWaitSeconds*math.Pow(base, float64(try)))

			pollDuration := 5 * time.Second // relatively short poll, so that we can update speedTimeout on each poll to reflect latest circumstances
			if isJobAboutToFinish {
				pollDuration = 1 * time.Second // poll more vigorously near the end
			}
			select {
			case <-readDone:
				// the read has finished
				return
			case <-time.After(pollDuration):
				// continue
			}

			if time.Since(start) > memoryTimeout {
				severalLaterChunksHaveArrived := atomic.LoadInt32(&w.totalReceivedChunkCount) > initialReceivedCount+1
				if severalLaterChunksHaveArrived && w.haveMemoryPressure(chunkSize) {
					// We know that later chunks are coming through fine AND we are getting tight on RAM, so force retry of this chunk
					// (even if still within conservativeTimeout)
					// This is the primary purpose of this routine: preventing 'stalls' due to too many unsaved chunks in RAM.
					// It's necessary because we write sequentially to the file.
					// It does not have to take into account average throughput, because later chucks arriving and RAM running out
					// is proof enough.
					w.chunkLogger.LogChunkStatus(id, EWaitReason.BodyReReadDueToMem())
					retryForcer()
				}
			} else {
				speedTimeout, isJobAboutToFinish = w.calcSpeedTimeout(speedTimeoutBackoff) // update with freshly-computed value (in case we have averages now, or proximity to end of job, that we didn't have before
				if time.Since(start) > speedTimeout {
					// This is the secondary purpose of this routine: preventing 'stalls' near the end of the transfer, where
					// RAM usage is no longer an issue, but slow chunks can cause a long tail in job progress.
					// Here we do have to take into account average throughput (in the form of conservativeTimeout) because
					// user may have a very slow network, so timeouts here must be relative to prior performance.
					w.chunkLogger.LogChunkStatus(id, EWaitReason.BodyReReadDueToSpeed())
					retryForcer()
					speedTimeoutBackoff = speedTimeoutBackoff * 5 // ramp this up really quickly, since the last thing we want to do is keep forcing retries on slow things that actually were making useful progress
				}
			}
		}
	}()
}

func (w *chunkedFileWriter) calcSpeedTimeout(speedTimeoutBackoffFactor int) (speedTimeout time.Duration, isJobAboutToFinish bool) {
	isJobAboutToFinish = w.chunkLogger.IsWaitingOnFinalBodyReads()
	var multiplier int
	if isJobAboutToFinish {
		multiplier = 3 // be more aggressive if we are near the end
	} else {
		multiplier = 10 // be more conservative, if we're not near the end
	}
	speedTimeout = w.averageDurationPerChunk() * time.Duration(multiplier) * time.Duration(speedTimeoutBackoffFactor)
	return
}
