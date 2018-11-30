
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
	"math/rand"
	"sync/atomic"
	"time"
)

// Used to write all the chunks to a disk file
type ChunkedFileWriter interface {
	WaitToScheduleChunk(ctx context.Context, chunkSize int64) error
	EnqueueChunk(ctx context.Context, chunkContents io.Reader, offsetInFile int64, chunkSize int64) error
	Flush(ctx context.Context) (md5Hash string, err error)
}

// TODO: move this pool to a better home
var tempPool = NewMultiSizeSlicePool(100 * 1024 * 1024)  // max size of 100 MB is based on max supported block size for block blobs

type chunkedFileWriter struct {
	// the file we are writing to (type as interface to somewhat abstract away io.File - e.g. for unit testing)
	file io.WriteCloser

	// used to track (potentially) in RAM bytes
	cacheLimiter CacheLimiter

	// how chunky should our file writes be?  Value might be tweaked for perf tuning
	writeSize int

	// file chunks that have arrived and not been sorted yet
	newUnorderedChunks chan fileChunk

	// used to control scheduling of new chunks against this file
	activeChunkCount int32

	// used for completion
	successMd5 chan string
	failureError chan error
}

type fileChunk struct {
	data []byte
	offsetInFile int64
}


func NewChunkedFileWriter(ctx context.Context, cacheLimiter CacheLimiter, file io.WriteCloser, writeSize int) ChunkedFileWriter {
	w := &chunkedFileWriter{
		file: file,
		cacheLimiter: cacheLimiter,
		writeSize: writeSize,
		successMd5: make(chan string),
		failureError: make(chan error, 1),
		newUnorderedChunks: make(chan fileChunk, 10000), // TODO: parameterize, OR make >= max expected number of chunks in any file
	}
	go w.workerRoutine(ctx)
	return w
}

var ChunkWriterAlreadyFailed = errors.New("chunk Writer already failed")

// Waits until we have enough RAM, within our pre-determined allocation, to accommodate the chunk.
// After any necessary wait, it updates the count of scheduled-but-unsaved bytes
// Note: we considered tracking only received-but-unsaved-bytes (i.e. increment the count at time of making the
// request to the remote data source) but decided it was simpler and no less effective to increment the count
// at the time of scheduling the chunk (which is when this routine should be called).
// Is here, as method of this struct, for symmetry with the point where we remove it's count
// from the cache limiter, which is also in this struct.
func (w *chunkedFileWriter) WaitToScheduleChunk(ctx context.Context, chunkSize int64) error{
	for {
		// Proceed if there's room in the cache, using a less strict cache limit
		// if we have relatively few chunks in progress for THIS file
		// We use the less strict limit if we have few in progress to try to spread
		// the work in progress across a larger number of files, instead of having it
		// get concentrated in one
		// TODO: can we find a sensible way to remove the hard-coded count threshold here?
		if w.cacheLimiter.AddIfBelowStrictLimit(chunkSize) ||
			(atomic.LoadInt32(&w.activeChunkCount) <= 10 && w.cacheLimiter.AddIfBelowRelaxedLimit(chunkSize)){
			atomic.AddInt32(&w.activeChunkCount, 1)
			return nil // the cache limited has accepted our addition
		}

		// else wait and repeat
		select {
		case <- ctx.Done():
			return ctx.Err()
		case <- time.After(time.Duration(2 * float32(time.Second) * rand.Float32())):
			// Nothing to do, just loop around again
			// The wait is randomized to prevent the establishment of repetitive oscillations in cache size
		}
	}
}

// Threadsafe method to enqueue a new chunk for processing
func (w *chunkedFileWriter) EnqueueChunk(ctx context.Context, chunkContents io.Reader, offsetInFile int64, chunkSize int64) error {
	// read into a buffer
	buffer := tempPool.RentSlice(uint32(chunkSize))
	_, err := io.ReadFull(chunkContents, buffer)
	if err != nil {
		return err
	}

	// enqueue it
	select {
	case err := <- w.failureError:
		if err != nil {
			return err
		}
		return ChunkWriterAlreadyFailed // channel returned nil because it was closed and empty
	case <-ctx.Done():
		return ctx.Err()
	case w.newUnorderedChunks <- fileChunk{data: buffer, offsetInFile: offsetInFile}:
		return nil
	}
}

// Waits until all chunks have been flush to disk, then returns
func (w *chunkedFileWriter) Flush(ctx context.Context) (string, error) {
	// let worker know that no more will be coming
	close(w.newUnorderedChunks)

	// wait until all written to disk
	select {
	case err := <- w.failureError:
		if err != nil {
			return "", err
		}
		return "", ChunkWriterAlreadyFailed  // channel returned nil because it was closed and empty
	case <-ctx.Done():
		return "", ctx.Err()
	case hashAsAtCompletion := <- w.successMd5:
		return hashAsAtCompletion, nil
	}
}

// Each fileChunkWriter needs exactly one goroutine running this, to service the channel and save the data
// This routine orders the data sequentially, so that (a) we can get maximum performance without
// resorting to the likes of SetFileValidData (https://docs.microsoft.com/en-us/windows/desktop/api/fileapi/nf-fileapi-setfilevaliddata)
// and (b) we can compute MD5 hashes - which can only be computed when moving through the data sequentially
func (w *chunkedFileWriter) workerRoutine(ctx context.Context){
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
		unsavedChunksByFileOffset[newChunk.offsetInFile] = newChunk

		// Process all chunks that we can
		err := w.saveAvailableChunks(unsavedChunksByFileOffset, &nextOffsetToSave)
		if err != nil{
			w.failureError <- err
			close(w.failureError)  // must close because many goroutines may be calling the public methods, and all need to be able to tell there's been an error, even tho only one will get the actual error
			return                 // no point in processing any more after a failure
		}
	}
}

// Saves available chunks that are sequential from nextOffsetToSave. Stops and returns as soon as it hits
// a gap (i.e. the position of a chunk that hasn't arrived yet)
func (w *chunkedFileWriter)saveAvailableChunks(unsavedChunksByFileOffset map[int64]fileChunk, nextOffsetToSave *int64) error {
	for {
		nextChunkInSequence, exists := unsavedChunksByFileOffset[*nextOffsetToSave]
		if !exists {
			return  nil   //its not there yet. That's OK.
		}
		*nextOffsetToSave += int64(len(nextChunkInSequence.data))

		err := w.saveOneChunk(nextChunkInSequence)
		if err != nil {
			return err
		}
	}
}

// Saves one chunk to its destination
func (w *chunkedFileWriter)saveOneChunk(chunk fileChunk) error{
	defer func() {
		w.cacheLimiter.RemoveBytes(int64(len(chunk.data))) // remove this from the tally of scheduled-but-unsaved bytes
		atomic.AddInt32(&w.activeChunkCount, -1)
		tempPool.ReturnSlice(chunk.data)
	}()

	bytesWritten := 0
	for {
		n, err := w.file.Write(chunk.data[bytesWritten:])
		if err != nil {
			return err
		}
		bytesWritten += n
		if bytesWritten == len(chunk.data) {
			return nil
		}
	}
}