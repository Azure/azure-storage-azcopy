// // Copyright © 2017 Microsoft <wastore@microsoft.com>
// //
// // Permission is hereby granted, free of charge, to any person obtaining a copy
// // of this software and associated documentation files (the "Software"), to deal
// // in the Software without restriction, including without limitation the rights
// // to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// // copies of the Software, and to permit persons to whom the Software is
// // furnished to do so, subject to the following conditions:
// //
// // The above copyright notice and this permission notice shall be included in
// // all copies or substantial portions of the Software.
// //
// // THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// // IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// // FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// // AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// // LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// // OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// // THE SOFTWARE.
package ste

//
//import (
//	"context"
//	"io"
//)
//
//// pacedReadSeeker implements read/seek/close with pacing. (Formerly in file pacer-lite)
//type pacedReadSeeker struct {
//
//	// Although storing ctx in a struct is generally considered an anti-patten, this particular
//	// struct happens to be fairly-short lived (from its users point of view). Basically just for
//	// as long as a takes to read or send a request body.  That's not terribly long, but it is
//	// long enough that we might need to cancel during it, hence the ctx.
//	ctx context.Context
//
//	body io.Reader // Seeking is required to support retries
//	p    pacer
//}
//
//func newPacedRequestBody(ctx context.Context, requestBody io.ReadSeeker, p pacer) io.ReadSeekCloser {
//	if p == nil {
//		panic("p must not be nil")
//	}
//	return &pacedReadSeeker{ctx: ctx, body: requestBody, p: p}
//}
//
//func newPacedResponseBody(ctx context.Context, responseBody io.ReadCloser, p pacer) io.ReadCloser {
//	if p == nil {
//		panic("p must not be nil")
//	}
//	return &pacedReadSeeker{ctx: ctx, body: responseBody, p: p}
//}
//
//func (prs *pacedReadSeeker) Read(p []byte) (int, error) {
//	requestedCount := len(p)
//
//	// blocks until we are allowed to process the bytes
//	err := prs.p.RequestTrafficAllocation(prs.ctx, int64(requestedCount))
//	if err != nil {
//		return 0, err
//	}
//
//	// process them
//	n, err := prs.body.Read(p)
//
//	// "return" any unused tokens to the pacer (e.g. if we hit eof before the end of our buffer p)
//	excess := requestedCount - n
//	prs.p.UndoRequest(int64(excess))
//
//	return n, err
//}
//
//// Seeking is required to support retries
//func (prs *pacedReadSeeker) Seek(offset int64, whence int) (offsetFromStart int64, err error) {
//	return prs.body.(io.ReadSeeker).Seek(offset, whence)
//}
//
//// pacedReadSeeker supports Close but the underlying stream may not; if it does, Close will close it.
//func (prs *pacedReadSeeker) Close() error {
//	if c, ok := prs.body.(io.Closer); ok {
//		return c.Close()
//	}
//	return nil
//}
