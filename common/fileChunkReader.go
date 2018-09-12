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
	"errors"
	"io"
	"os"
)

// Reader of ONE chunk of a file. Maybe be used to re-read multiple times (e.g. if
// we must retry the sending of the chunk).
// May use implementation dependent pre-fetch, and implementation-dependent
// logic for when to discard any prefetched data (typically when it has read to the end
// for the first time, the prefected data will be discarded)
// Cannot be read by multiple threads (since Read/Seek are inherently stateful)
type FileChunkReader interface{
	io.ReadSeeker
}

// TODO: how does the file get closed?

type simpleFileChunkReader struct {
	file *os.File

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
	prefetchedByteTracker *SharedCounter
}

// TODO: consider support for prefetching only part of chunk. For the cases where chunks are relatively large (e.g. 100 MB)

func NewSimpleFileChunkReader(file *os.File, offset int64, length int64, prefetchedByteTracker *SharedCounter) (FileChunkReader, error) {
	if length <= 0 {
		return nil, errors.New("length must be greater than zero")
	}

	chunkReader := &simpleFileChunkReader{
		file:                  file,
		offsetInFile:          offset,
		length:                length,
		prefetchedByteTracker: prefetchedByteTracker}

	err := chunkReader.EnsurePrefetchCompleted()
	if err != nil {
		return nil, err
	}
	return chunkReader, nil
}


func (cr *simpleFileChunkReader) EnsurePrefetchCompleted() error {
	if cr.buffer != nil {
		return nil    // already prefetched
	}

	cr.buffer = make([]byte, cr.length)

	// read the data
	bytesRead, err := cr.file.ReadAt(cr.buffer, cr.offsetInFile)
	if err != nil {
		return err
	}
	if int64(bytesRead) != cr.length {
		return errors.New("bytes read not equal to expected length. Chunk reader must be constructed so that it won't read past end of file")
	}

	// increase count of unused prefetched bytes
	cr.prefetchedByteTracker.Add(int64(bytesRead))

	return nil
}

// Seeks within this chunk
func (cr *simpleFileChunkReader) Seek(offset int64, whence int) (int64, error){

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
func (cr *simpleFileChunkReader)  Read(p []byte) (n int, err error) {
	// check for EOF, BEFORE we ensure prefetch
	// (Otherwise, some readers can call as after EOF, and we end up re-pre-fetching)
	if cr.positionInChunk >= cr.length {
		return 0, io.EOF
	}

	// Always use the prefetch logic to read the data
	// This is simpler to maintain than using a different code path for the (rare) cases
	// where there has been no prefetch before this routine is called
	err = cr.EnsurePrefetchCompleted()
	if err != nil {
		return 0, err
	}

	// copy bytes across
	bytesCopied := copy(p, cr.buffer[cr.positionInChunk:])
	cr.positionInChunk += int64(bytesCopied)

	// check for EOF
	isEof := cr.positionInChunk >= cr.length
	if isEof {
		// free the buffer now, since we probably won't read it again
		// (and on the relatively rare occasions when we do, we'll just take the hit
		// of re-reading it from disk, and the added hit that that read will be non-sequential)
		cr.buffer = nil
		cr.prefetchedByteTracker.Add(-cr.length)
		return bytesCopied, io.EOF
	}

	return bytesCopied, nil
}