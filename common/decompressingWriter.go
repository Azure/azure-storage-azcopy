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
	"compress/gzip"
	"compress/zlib"
	"errors"
	"io"
	"time"
)

type decompressingWriter struct {
	pipeWriter  *io.PipeWriter
	workerError chan error
}

const decompressingWriterCopyBufferSize = 256 * 1024 // 1/4 the size that we usually write to disk with (elsewhere in codebase). 1/4 to try to keep mem usage a bit lower, without going so small as to compromize perf
var decompressingWriterBufferPool = NewMultiSizeSlicePool(decompressingWriterCopyBufferSize)

var ECompressionType = CompressionType(0)

type CompressionType uint8

func (CompressionType) None() CompressionType { return CompressionType(0) }
func (CompressionType) ZLib() CompressionType { return CompressionType(1) }
func (CompressionType) GZip() CompressionType { return CompressionType(2) }

// NewDecompressingWriter returns a WriteCloser which decompresses the data
// that is written to it, before passing the decompressed data on to a final destination
func NewDecompressingWriter(destination io.WriteCloser, ct CompressionType) io.WriteCloser {
	preader, pwriter := io.Pipe()

	d := &decompressingWriter{
		pipeWriter:  pwriter,
		workerError: make(chan error, 1),
	}

	// start the output processor worker
	go d.worker(ct, preader, destination, d.workerError)

	return d
}

func (d decompressingWriter) decompressorFactory(tp CompressionType, preader *io.PipeReader) (io.ReadCloser, error) {
	switch tp {
	case ECompressionType.ZLib():
		return zlib.NewReader(preader)
	case ECompressionType.GZip():
		return gzip.NewReader(preader)
	default:
		return nil, errors.New("unexpected compression type")
	}
}

func (d decompressingWriter) worker(tp CompressionType, preader *io.PipeReader, destination io.WriteCloser, workerError chan error) {

	defer func() {
		_ = destination.Close() // always close the destination file before we exit, since its a WriteCloser
		_ = preader.Close()
	}()

	// make the decompressor. Must be in the worker method because,
	// like the rest of read, this reads from the pipe.
	// (Factory reads from pipe to read the zip/gzip file header)
	dec, err := d.decompressorFactory(tp, preader)
	if err != nil {
		workerError <- err
		return
	}

	// Now read from the pipe, decompressing as we go, until
	// reach EOF on the pipe (or encounter an error)
	b := decompressingWriterBufferPool.RentSlice(decompressingWriterCopyBufferSize)
	_, err = io.CopyBuffer(destination, dec, b) // returns err==nil if hits EOF, as per docs
	decompressingWriterBufferPool.ReturnSlice(b)
	workerError <- err
	return
}

// Write, conceptually, takes a slice of compressed data, decompresses it, and writes it into the final destination.
// In actuality, all it really does is writes the compressed data to the pipe, and leaves
// it up to the worker to do the rest
func (d decompressingWriter) Write(p []byte) (n int, err error) {
	n, writeErr := d.pipeWriter.Write(p)

	// check for worker error, and report it in preference to the writeError,
	// since the worker error is likely to be more meaningful
	select {
	case workerErr := <-d.workerError:
		if workerErr == nil {
			return n, errors.New("decompression worker exited early") // we don't expect this
		}
		return n, errors.New("error in decompression worker when writing: " + workerErr.Error())
	default:
		// no worker error
	}

	return n, writeErr
}

func (d decompressingWriter) Close() error {
	// close pipe, so reader will get EOF
	closeError := d.pipeWriter.Close()
	if closeError != nil {
		return closeError
	}

	// check for worker completion and error state
	select {
	case workerErr := <-d.workerError:
		if workerErr == nil {
			return nil
		}
		return errors.New("error in decompression worker when closing: " + workerErr.Error())
	case <-time.After(time.Minute * 15): // should never take THIS long to flush final data to destination, but better to wait too long than too short and stop it before its closed
		return errors.New("timed out closing decompression worker")
	}
}
