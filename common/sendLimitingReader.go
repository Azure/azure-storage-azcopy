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
)

type sendLimitingReader struct {
	// context used when waiting for permission to supply data
	ctx context.Context

	// Limits the number of concurrent active sends
	sendLimiter SendLimiter

	// the byte slice we are reading from
	chunkContents []byte

	// position for Seek/Read
	positionInChunk int64

	// do we currently own/hold one of the active "slots" for sending
	sendSlotHeld bool
}

func NewSendLimitingReader(ctx context.Context, contents []byte, sendLimiter SendLimiter) io.ReadSeeker {
	if len(contents) <= 0 {
		panic("length must be greater than zero")
	}
	return &sendLimitingReader{
		ctx:                   ctx,
		sendLimiter:           sendLimiter,
		chunkContents:         contents}
}

// Seeks within this chunk
// Seeking is used for retries, and also by some code to get length (by seeking to end)
func (cr *sendLimitingReader) Seek(offset int64, whence int) (int64, error) {

	newPosition := cr.positionInChunk

	switch whence {
	case io.SeekStart:
		newPosition = offset
	case io.SeekCurrent:
		newPosition += offset
	case io.SeekEnd:
		newPosition = int64(len(cr.chunkContents)) - offset
	}

	if newPosition < 0 {
		return 0, errors.New("cannot seek to before beginning")
	}
	if newPosition > int64(len(cr.chunkContents)) {
		newPosition = int64(len(cr.chunkContents))
	}

	cr.positionInChunk = newPosition
	return cr.positionInChunk, nil
}

// Reads from within this chunk
func (cr *sendLimitingReader) Read(p []byte) (n int, err error) {
	if cr.positionInChunk >= int64(len(cr.chunkContents)) {
		return 0, io.EOF
	}

	if cr.positionInChunk == 0 && !cr.sendSlotHeld {
		// Wait until we are allowed to be one of the actively-sending goroutines
		// (The total count of active senders is limited to provide better network performance,
		// on a per-CPU-usage basis. Without this, CPU usage in the OS's network stack is much higher
		// in some tests.)
		// It would have been nice to do this restriction not here in the Read method but more in the structure
		// of the code, by explicitly separating HTTP "write body" from "read response" (and only applying this limit
		// to the former).  However, that's difficult/impossible in the architecture of net/http. So instead we apply
		// the restriction here, in code that is called at the time of STARTING to write the body
		err = cr.sendLimiter.AcquireSendSlot(cr.ctx)
		if err != nil {
			return 0, err
		}
		cr.sendSlotHeld = true
	}

	// Copy the data across
	bytesCopied := copy(p, cr.chunkContents[cr.positionInChunk:])
	cr.positionInChunk += int64(bytesCopied)

	// check for EOF
	isEof := cr.positionInChunk >= int64(len(cr.chunkContents))
	if isEof {
		return bytesCopied, io.EOF
	}

	return bytesCopied, nil
}
