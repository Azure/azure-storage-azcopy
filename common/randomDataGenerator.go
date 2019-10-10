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
	"math"
	"math/rand"
	"sync"
)

const (
	randomSliceLength = 1024 * 1024
)

var randomDataBytePool = NewMultiSizeSlicePool(randomSliceLength)

func NewRandomDataGenerator(length int64) CloseableReaderAt {
	r := &randomDataGenerator{
		length:    length,
		randGen:   rand.New(rand.NewSource(rand.Int63())), // create new rand source, seeded from global one, so that after seeding we never lock the global one
		randBytes: randomDataBytePool.RentSlice(randomSliceLength),
		randMu:    &sync.Mutex{}}

	if r.couldBeNewSlice(r.randBytes) {
		r.randGen.Read(r.randBytes) // fill new arrays with random data
	}
	return r
}

type randomDataGenerator struct {
	length             int64
	randGen            *rand.Rand
	randBytes          []byte
	randMu             *sync.Mutex
	readIterationCount int
}

func (r *randomDataGenerator) couldBeNewSlice(s []byte) bool {
	return s[0] == 0
}

func (r *randomDataGenerator) Close() error {
	if r.couldBeNewSlice(r.randBytes) {
		r.randBytes[0] = 1 // so we know its not new when we get it back
	}
	randomDataBytePool.ReturnSlice(r.randBytes)
	r.randBytes = nil
	return nil
}

func (r *randomDataGenerator) ReadAt(p []byte, off int64) (n int, err error) {
	if off+int64(len(p)) > r.length {
		return 0, errors.New("would read past end")
	}

	min := func(a int, b int64) int {
		return int(math.Min(float64(a), float64(b)))
	}

	// lock, just in case we one day refactor to a design where concurrent reads on one randomDataGenerator may happen
	r.randMu.Lock()
	defer r.randMu.Unlock()

	n = 0
	for n < len(p) {
		remainingInFile := r.length - (off + int64(n))
		if remainingInFile == 0 {
			break
		}
		remainingThisRead := min(len(p)-n, remainingInFile)
		remainingThisIteration := min(remainingThisRead, int64(len(r.randBytes)))
		r.freshenRandomData(remainingThisIteration)
		copy(p[n:], r.randBytes[:remainingThisIteration])
		n += remainingThisIteration
	}

	if n != len(p) {
		panic("unexpected read length")
	}
	return n, nil
}

// The math.rand type is too slow for us to generate a completely fresh set of random data each time.
// (I.e. waiting for it to do so throttles our achievable send rate, below the rates we want to test at).
// So, don't refresh the whole buffer, just refresh a few parts of it - making sure to refresh different parts on each call.
// Using this approach improves speed from about 2.5 Gbps to about 8 or 9 Gbps (per "file"/randomDataGenerator).
// The refresh is to
// (a) prevent the (remote?) possibility of any DDOS protection device or similar becoming suspicious of many absolutely identical payloads, and
// (b) reduce (but not eliminate?) the risk of something such as a "TCP accelerator", somewhere in the network, compressing our data and thereby
// giving a misleading perf result.
func (r *randomDataGenerator) freshenRandomData(count int) {
	// completely freshen every xth element of the array
	const arbitraryMediumSizedNumber = 199 // seems sensible if this is relatively prime to the normal size of the payload section of a TCP segment, which is about 1400-and-something
	r.readIterationCount++
	for i := r.readIterationCount % arbitraryMediumSizedNumber; i < count; i += arbitraryMediumSizedNumber {
		r.randGen.Read(r.randBytes[i : i+1])
	}

	// ALSO flip random bits in every yth one (where y is much smaller than the x we used above)
	// This is not as random as what we do above, but its faster. And without it, the data is too compressible
	var skipSize = 2 // with skip-size = 3 its slightly faster, and still uncompressible with zip but it is
	// compressible (down to 30% of original size) with 7zip's compression
	bitFlipMask := byte(r.randGen.Int31n(128)) + 128
	for i := r.readIterationCount % skipSize; i < count; i += skipSize {
		r.randBytes[i] ^= bitFlipMask
	}

	// TODO: add unit tests to assert the lack of compressibility (since for now we are just going
	//   on tests of the .NET code from which randomDataGenerator was ported
}
