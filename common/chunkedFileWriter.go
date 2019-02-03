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
	Flush(ctx context.Context) (md5Hash string, err error)

	// MaxRetryPerDownloadBody returns the maximum number of retries that will be done for the download of a single chunk body
	MaxRetryPerDownloadBody() int
}

type chunkedFileWriter struct {
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

	// all time received count for this instance
	totalReceivedChunkCount int32

	creationTime time.Time

	// used for completion
	successMd5   chan string // TODO: use this when we do MD5s
	failureError chan error

	// controls body-read retries. Public so value can be shared with retryReader
	maxRetryPerDownloadBody int
}

type fileChunk struct {
	id   ChunkID
	data []byte
}

func NewChunkedFileWriter(ctx context.Context, slicePool ByteSlicePooler, cacheLimiter CacheLimiter, chunkLogger ChunkStatusLogger, file io.WriteCloser, numChunks uint32, maxBodyRetries int) ChunkedFileWriter {
	// Set max size for buffered channel. The upper limit here is believed to be generous, given worker routine drains it constantly.
	// Use num chunks in file if lower than the upper limit, to prevent allocating RAM for lots of large channel buffers when dealing with
	// very large numbers of very small files.
	chanBufferSize := int(math.Min(float64(numChunks), 1000))

	w := &chunkedFileWriter{
		file:                    file,
		slicePool:               slicePool,
		cacheLimiter:            cacheLimiter,
		chunkLogger:             chunkLogger,
		successMd5:              make(chan string),
		failureError:            make(chan error, 1),
		newUnorderedChunks:      make(chan fileChunk, chanBufferSize),
		creationTime:            time.Now(),
		maxRetryPerDownloadBody: maxBodyRetries,
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
	err := w.cacheLimiter.WaitUntilAddBytes(ctx, chunkSize, w.shouldUseRelaxedRamThreshold)
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
	buffer := w.slicePool.RentSlice(uint32(chunkSize))
	_, err := io.ReadFull(chunkContents, buffer)
	close(readDone)
	if err != nil {
		return err
	}

	// enqueue it
	w.chunkLogger.LogChunkStatus(id, EWaitReason.WriterChannel())
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

// Waits until all chunks have been flush to disk, then returns
func (w *chunkedFileWriter) Flush(ctx context.Context) (string, error) {
	// let worker know that no more will be coming
	close(w.newUnorderedChunks)

	// wait until all written to disk
	select {
	case err := <-w.failureError:
		if err != nil {
			return "", err
		}
		return "", ChunkWriterAlreadyFailed // channel returned nil because it was closed and empty
	case <-ctx.Done():
		return "", ctx.Err()
	case hashAsAtCompletion := <-w.successMd5:
		return hashAsAtCompletion, nil
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

	for {
		var newChunk fileChunk
		var channelIsOpen bool

		// await new chunk (or cancellation)
		select {
		case newChunk, channelIsOpen = <-w.newUnorderedChunks:
			if !channelIsOpen {
				// If channel is closed, we know that flush as been called and we have read everything
				// So we are finished
				// TODO: add returning of MD5 hash in the next line
				w.successMd5 <- "" // everything is done. We know there was no error, because if there was an error we would have returned before now
				return
			}
		case <-ctx.Done():
			w.failureError <- ctx.Err()
			return
		}

		// index the new chunk
		unsavedChunksByFileOffset[newChunk.id.OffsetInFile] = newChunk
		atomic.AddInt32(&w.totalReceivedChunkCount, 1)
		w.chunkLogger.LogChunkStatus(newChunk.id, EWaitReason.PriorChunk()) // may have to wait on prior chunks to arrive

		// Process all chunks that we can
		err := w.saveAvailableChunks(unsavedChunksByFileOffset, &nextOffsetToSave)
		if err != nil {
			w.failureError <- err
			close(w.failureError) // must close because many goroutines may be calling the public methods, and all need to be able to tell there's been an error, even tho only one will get the actual error
			return                // no point in processing any more after a failure
		}
	}
}

// Saves available chunks that are sequential from nextOffsetToSave. Stops and returns as soon as it hits
// a gap (i.e. the position of a chunk that hasn't arrived yet)
func (w *chunkedFileWriter) saveAvailableChunks(unsavedChunksByFileOffset map[int64]fileChunk, nextOffsetToSave *int64) error {
	for {
		nextChunkInSequence, exists := unsavedChunksByFileOffset[*nextOffsetToSave]
		if !exists {
			return nil //its not there yet. That's OK.
		}
		*nextOffsetToSave += int64(len(nextChunkInSequence.data))

		err := w.saveOneChunk(nextChunkInSequence)
		if err != nil {
			return err
		}
	}
}

// Saves one chunk to its destination
func (w *chunkedFileWriter) saveOneChunk(chunk fileChunk) error {
	defer func() {
		w.cacheLimiter.RemoveBytes(int64(len(chunk.data))) // remove this from the tally of scheduled-but-unsaved bytes
		atomic.AddInt32(&w.activeChunkCount, -1)
		w.slicePool.ReturnSlice(chunk.data)
		w.chunkLogger.LogChunkStatus(chunk.id, EWaitReason.ChunkDone()) // this chunk is all finished
	}()

	w.chunkLogger.LogChunkStatus(chunk.id, EWaitReason.Disk())
	_, err := w.file.Write(chunk.data) // unlike Read, Write must process ALL the data, or have an error.  It can't return "early".
	if err != nil {
		return err
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
	didAdd := w.cacheLimiter.TryAddBytes(chunkSize, w.shouldUseRelaxedRamThreshold())
	if didAdd {
		w.cacheLimiter.RemoveBytes(chunkSize) // remove immediately, since this was only a test
	}
	return !didAdd
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
	initialWaitSeconds := 15 // arbitrarily selected, to give minimal impression of waiting, to user (in testing, 30 seconds did occasionally show total throughput drops of a new 10's of percent)
	base := float64(4)       // a steep exponential backoff

	// set up a conservative timeout threshold based on average throughput so far
	averageDurationPerChunkSoFar := time.Hour
	if initialReceivedCount > 0 {
		averageDurationPerChunkSoFar = time.Since(w.creationTime) / time.Duration(initialReceivedCount)
	}
	conservativeTimeout := averageDurationPerChunkSoFar * 10 // multiplier is small enough that in high-throughput test cases, conservativeTimeout has typically expired by end of first try, so we can force retry at that time if needed

	// Run a goroutine to monitor progress and force retries when necessary
	// Note that the retries are transparent to the main body Read call, due to use of retry reader. I.e.
	// our external caller's call to Read just keeps on running, and the external caller never even knows the retry happened
	go func() {
		maxConfiguredRetries := w.maxRetryPerDownloadBody
		maxForcedRetries := maxConfiguredRetries - 1 // leave one retry unused by us, to keep it available for non-forced, REAL, errors (handled by retryReader)
		for try := 0; try < maxForcedRetries; try++ {
			waitSeconds := int32(float64(initialWaitSeconds) * math.Pow(base, float64(try)))
			select {
			case <-readDone:
				// the read has finished
				return
			case <-time.After(time.Duration(waitSeconds) * time.Second):
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
				} else if time.Since(start) > conservativeTimeout {
					// This is the secondary purpose of this routine: preventing 'stalls' near the end of the transfer, where
					// RAM usage is no longer an issue, but slow chunks can cause a long tail in job progress.
					// Here we do have to take into account average throughput (in the form of conservativeTimeout) because
					// user may have a very slow network, so timeouts here must be relative to prior performance.
					w.chunkLogger.LogChunkStatus(id, EWaitReason.BodyReReadDueToSpeed())
					retryForcer()
					conservativeTimeout = conservativeTimeout * 5 // ramp this up really quickly, since the last thing we want to do is keep forcing retries on slow things that actually were making useful progress
				}
			}
		}
	}()
}
