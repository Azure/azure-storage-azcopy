
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
	"io"
)

type FileChunkWriter interface {
	// Function that copies download data into destination file
	CopyAllToFile(src io.Reader, offsetInFile int64, expectedCount int64) error
}

type fileChunkWriter struct {
	// explicitly type the file as WriterAt, since WriterAt is safe to use with multiple threads hitting
	// the same file, but Write is not
	file io.WriterAt

	// how chunky should our file writes be?  Value might be tweaked for perf tuning
	writeSize int
}

func NewFileChunkWriter(file io.WriterAt, writeSize int) FileChunkWriter {
	return &fileChunkWriter{file: file, writeSize: writeSize}
}

func (w *fileChunkWriter) CopyAllToFile(src io.Reader, offsetInFile int64, expectedCount int64) error {
	buffer := make([]byte, w.writeSize)  // TODO: consider pooling if shown to necessary by perf testing
	bytesWrittenSoFar := int64(0)
	for {
		// fill the buffer, since we want to write w.writeSize bytes to disk at a time (whenever possible)
		n, err := io.ReadFull(src, buffer)
		wasEof := err == io.EOF || err == io.ErrUnexpectedEOF  // unexpected EOF just means there wasn't enough left to fill the buffer this time
		if err != nil && !wasEof {
			return err
		}

		// write out the buffer to the right point in the file
		_, err = w.file.WriteAt(buffer[:n], offsetInFile + int64(bytesWrittenSoFar))
		if err != nil {
			return err
		}
		bytesWrittenSoFar += int64(n)

		// have we finished, and if so, were we successful?
		if wasEof {
			if bytesWrittenSoFar == expectedCount {
				return nil
			} else {
				return io.ErrUnexpectedEOF
			}
		}
	}
}