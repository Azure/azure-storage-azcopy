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
	"io"
	"sync/atomic"
)

// pacedReadSeeker implements read/seek/close with pacing. (Formerly in file pacer-lite)
type pacedReadSeeker struct {
	body io.Reader // Seeking is required to support retries
	p    *pacer
}

func newPacedRequestBody(requestBody io.ReadSeeker, p *pacer) io.ReadSeeker {
	if p == nil {
		panic("p must not be nil")
	}
	return &pacedReadSeeker{body: requestBody, p: p}
}

func newPacedResponseBody(responseBody io.ReadCloser, p *pacer) io.ReadCloser {
	if p == nil {
		panic("p must not be nil")
	}
	return &pacedReadSeeker{body: responseBody, p: p}
}

func (lbp *pacedReadSeeker) Read(p []byte) (int, error) {
	n, err := lbp.body.Read(p)
	atomic.AddInt64(&lbp.p.bytesTransferred, int64(n))
	return n, err
}

// Seeking is required to support retries
func (lbp *pacedReadSeeker) Seek(offset int64, whence int) (offsetFromStart int64, err error) {
	return lbp.body.(io.ReadSeeker).Seek(offset, whence)
}

// pacedReadSeeker supports Close but the underlying stream may not; if it does, Close will close it.
func (lbp *pacedReadSeeker) Close() error {
	if c, ok := lbp.body.(io.Closer); ok {
		return c.Close()
	}
	return nil
}
