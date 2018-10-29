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

package ste

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
)

type ConcurrencyParams struct {
	// How many go-routines, at most, should be actively sending network traffic at any given time?
	// If loss rates or latency are very high, and target bandwith is in multi-Gbps range, the
	// value may need to be increased from the default.
	// If available bandwidth is low, value can safely be decreased from the default (to reduce GoShed load,
	// to reduce risk of self-imposed congestion, and and to reduce possibility of unfairness to other traffic).
	ConcurrentSendCount int

	// How may go-routines, at most, should be waiting for replies from the storage service?  This value
	// can, and should, be safely sent much higher than maxSenderCount.  While this is a max limit, the actual
	// number waiting at any given time will be a subset of this.  With the number actually waiting being
	// directly proportional to the current response time of the Storage Service,
	// multiplied by our current total throughput (as per standard queuing theory).
	ConcurrentWaitCount int

	// How many disk files should we read concurrently?
	// Value doesn't matter too much for SSD (something around 16 seems fine). For physical disks, a rule of thumb
	// is to start with a value equal to the number of disk spindles, and then try (progressively) doubling and halving from there
	// to find an optimum.  Maybe 1 for a single physical disk, or in the hundreds for SAN/NAS.
	ConcurrentFileReadCount int
}

const (
	DefaultConcurrentSendCount int = 96  // empirically derived, as being generally adequate for 10 Gbps over 60 ms latency
	DefaultConcurrentWaitCount int = 500 // about 250 is usually enough,even for 10 Gbps, but its safe to have more
	DefaultFileReadCount       int = 16  // default value suits simple SSD-based scenarios

	AZCOPY_CONCURRENT_SEND_COUNT_NAME      = "AZCOPY_CONCURRENT_SEND_COUNT"
	AZCOPY_CONCURRENT_WAIT_COUNT_NAME      = "AZCOPY_CONCURRENT_WAIT_COUNT"
	AZCOPY_CONCURRENT_FILE_READ_COUNT_NAME = "AZCOPY_CONCURRENT_FILE_READ_COUNT"
)

func (c *ConcurrencyParams) Dump(){
	fmt.Printf("%s=%d\n", AZCOPY_CONCURRENT_SEND_COUNT_NAME, c.ConcurrentSendCount)
	fmt.Printf("%s=%d\n", AZCOPY_CONCURRENT_WAIT_COUNT_NAME, c.ConcurrentWaitCount)
	fmt.Printf("%s=%d\n", AZCOPY_CONCURRENT_FILE_READ_COUNT_NAME, c.ConcurrentFileReadCount)
	fmt.Printf("Max prefetch GB = %.1f\n", float32(c.GetMaxPrefetchBytes()) / (1024 * 1024 * 1024))
}


/// Gets an estimate of a good number of bytes for us to prefetch
// (i.e. to have read of disk, and to have not yet finished sending yet)
func (c *ConcurrencyParams) GetMaxPrefetchBytes() int64 {
	// TODO: improve caching approach to deal with other block sizes
	return common.DefaultBlockBlobBlockSize *  // size of a block
		int64(c.ConcurrentSendCount) *         // number of senders
		2                                      // two blocks per sender (the one they're sending now, and one more that's ready for them)
}

