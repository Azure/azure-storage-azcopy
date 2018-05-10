package ste

import (
	"io"
	"sync/atomic"
)

// this struct wraps the ReadSeeker which contains the data to be sent over the network
type bytesOverTheWire struct {
	body             io.ReadSeeker // Seeking is required to support retries
	bytesTransferred int64
}

// get a ReadSeeker wrapper for the given request body, bound to the given pacer
func newBytesOverTheWire(requestBody io.ReadSeeker) io.ReadSeeker {
	return &bytesOverTheWire{body: requestBody, bytesTransferred:0}
}

// read blocks until tickets are obtained
func (bw *bytesOverTheWire) Read(p []byte) (int, error) {
	n , err := bw.body.Read(p)
	atomic.AddInt64(&bw.bytesTransferred, int64(n))
	return n , err
}

// no behavior change for seek
func (bw *bytesOverTheWire) Seek(offset int64, whence int) (offsetFromStart int64, err error) {
	return bw.body.Seek(offset, whence)
}

// bytesOverTheWire supports Close but the underlying stream may not; if it does, Close will close it.
func (bw *bytesOverTheWire) Close() error {
	if c, ok := bw.body.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

func (bw *bytesOverTheWire) Bandwidth() int64 {
	return atomic.LoadInt64(&bw.bytesTransferred)
}