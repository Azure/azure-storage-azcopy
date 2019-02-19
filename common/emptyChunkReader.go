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
	"hash"
	"io"
)

// emptyChunkReader satisfies the SingleChunkReader interface for empty chunks (i.e. the first and only chunk of an empty file)
type emptyChunkReader struct {
}

func (cr *emptyChunkReader) TryBlockingPrefetch(fileReader io.ReaderAt) bool {
	return true
}

func (cr *emptyChunkReader) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekEnd && offset > 0 || offset < 0 {
		return 0, errors.New("cannot seek to before beginning")
	}
	return 0, nil
}

func (cr *emptyChunkReader) Read(p []byte) (n int, err error) {
	return 0, io.EOF
}

func (cr *emptyChunkReader) Close() error {
	return nil
}

func (cr *emptyChunkReader) GetPrologueState() PrologueState {
	return PrologueState{}
}

func (cr *emptyChunkReader) HasPrefetchedEntirelyZeros() bool {
	return false // we don't have any zeros (or anything else for that matter)
}

func (cr *emptyChunkReader) Length() int64 {
	return 0
}

func (cr *emptyChunkReader) WriteBufferTo(h hash.Hash) {
	return // no content to write
}
