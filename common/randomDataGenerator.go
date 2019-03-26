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
	"math/rand"
	"strings"
	"sync"
)

// SendRandomDataExt is an optionally specified file suffix, used for generating test data. To use,
// generate a SPARSE disk file that has zero on-disk size, but a large logical size.
// Give it a distinctive extension. E.g. myBigFile.azCopySparseFill
// Then set the command-line parameter to put that extension into this variable.
// The result will be that AzCopy will upload the full logical size of the sparse file, with all its content as pseudo-random
// bytes.  So you can have, for example, something that occupies 0 bytes on your disk, but gets uploaded as 10 TB of random data.
var SendRandomDataExt string

func IsPlaceholderForRandomDataGenerator(filename string) bool {
	// TODO: Add OS calls to also check here that the on-disk size of the file is zero bytes, so we KNOW for sure
	//    that we are not throwing anything away by ignoring its content.
	return SendRandomDataExt != "" && strings.HasSuffix(filename, "."+SendRandomDataExt)
}

func NewRandomDataGenerator(sizeInBytes int64) CloseableReaderAt {
	return &randomDataGenerator{sizeInBytes,
		rand.New(rand.NewSource(rand.Int63())),
		&sync.Mutex{}}
}

type randomDataGenerator struct {
	size   int64
	rand   *rand.Rand
	randMu *sync.Mutex
}

func (r *randomDataGenerator) Close() error {
	return nil
}

func (r *randomDataGenerator) ReadAt(p []byte, off int64) (n int, err error) {
	if off+int64(len(p)) > r.size {
		return 0, errors.New("would read past end")
	}

	// lock, just in case we one day refactor to a design where concurrent reads on one randomDataGenerator may happen
	r.randMu.Lock()
	defer r.randMu.Unlock()

	return r.rand.Read(p)
}
