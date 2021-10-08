package common

import (
	"compress/gzip"
	"errors"
	"io"
	"time"
)

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

type CompressingReader struct {
	rawDataSize int64
	workerError chan error
	pipeReader  *io.PipeReader
}

// TODO be smarter about this size
const compressingReaderCopyBufferSize = 8 * 1024 * 1024

var compressingReaderBufferPool = NewMultiSizeSlicePool(compressingReaderCopyBufferSize)

func NewCompressingReader(sourceFile CloseableReaderAt, rawDataSize int64) *CompressingReader {
	pipeReader, pipeWriter := io.Pipe()

	c := &CompressingReader{
		rawDataSize: rawDataSize,
		workerError: make(chan error, 1),
		pipeReader:  pipeReader,
	}

	// start the output processor worker
	go c.worker(sourceFile, pipeWriter, c.workerError)

	return c
}

func (c *CompressingReader) Read(p []byte) (n int, err error) {
	return c.pipeReader.Read(p)
}

func (c *CompressingReader) worker(sourceFile CloseableReaderAt, resultWriter *io.PipeWriter, workerError chan error) {
	compressor := gzip.NewWriter(resultWriter)
	buffer := compressingReaderBufferPool.RentSlice(compressingReaderCopyBufferSize)
	var err error

	defer func() {
		compressor.Close()
		resultWriter.Close()
		sourceFile.Close()
		compressingReaderBufferPool.ReturnSlice(buffer)
		workerError <- err
	}()

	for position := int64(0); position < c.rawDataSize; {
		n, readErr := sourceFile.ReadAt(buffer, position)

		if n != 0 {
			// write the buffered content completely, which requires calling write until the entire content is ingested
			writtenSoFar := 0
			for writtenSoFar != n {
				m, compressorErr := compressor.Write(buffer[writtenSoFar:n])
				if compressorErr != nil {
					// quit right away, something went wrong with compression
					err = compressorErr
					return
				}

				writtenSoFar += m
			}
		}

		if readErr != nil && readErr != io.EOF {
			err = readErr
			return
		}

		position += int64(n)
	}
}

func (c *CompressingReader) Close() error {
	// the worker will get ErrClosedPipe if it continues to write, resulting in it quitting
	closeErr := c.pipeReader.Close()
	if closeErr != nil {
		return closeErr
	}

	select {
	case workerErr := <-c.workerError:
		if workerErr == nil || workerErr == io.ErrClosedPipe {
			return nil
		}
		return errors.New("error in compression worker when closing: " + workerErr.Error())
	case <-time.After(time.Minute * 15): // should never take THIS long to flush, but better to wait too long than too short and stop it before it's closed
		return errors.New("timed out closing compression worker")
	}
}
