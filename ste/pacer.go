package ste

import (
	"io"
	"runtime"
	"sync/atomic"
	"time"
)

type pacer struct {
	bytesAvailable int64
}

func newPacer(bytesPerSecond int64) (p *pacer) {
	p = &pacer{bytesAvailable: 0}
	timeToWaitInMs := 50
	availableBytesPerPeriod := bytesPerSecond * int64(timeToWaitInMs) / 1000

	go func() {
		for {
			for targetTime := time.Now().Add(time.Millisecond * 50); time.Now().Before(targetTime); {
				runtime.Gosched()
			}

			if atomic.AddInt64(&p.bytesAvailable, availableBytesPerPeriod) > 2*availableBytesPerPeriod {
				atomic.AddInt64(&p.bytesAvailable, -availableBytesPerPeriod)
			}
		}
	}()

	return
}

func (p *pacer) requestRightToSend(bytesToSend int64) {
	// attempt to get ticket
	for atomic.AddInt64(&p.bytesAvailable, -bytesToSend) < 0 {
		atomic.AddInt64(&p.bytesAvailable, bytesToSend)
		time.Sleep(time.Millisecond * 1)
	}
}

type requestBodyPacer struct {
	requestBody io.ReadSeeker // Seeking is required to support retries
	p           *pacer
}

func newRequestBodyPacer(requestBody io.ReadSeeker, p *pacer) io.ReadSeeker {
	if p == nil {
		panic("pr must not be nil")
	}
	return &requestBodyPacer{requestBody: requestBody, p: p}
}

// read blocks until ticket is obtained
func (rbp *requestBodyPacer) Read(p []byte) (int, error) {
	rbp.p.requestRightToSend(int64(len(p)))
	return rbp.requestBody.Read(p)
}

func (rbp *requestBodyPacer) Seek(offset int64, whence int) (offsetFromStart int64, err error) {
	return rbp.requestBody.Seek(offset, whence)
}

// requestBodyPacer supports Close but the underlying stream may not; if it does, Close will close it.
func (rbp *requestBodyPacer) Close() error {
	if c, ok := rbp.requestBody.(io.Closer); ok {
		return c.Close()
	}
	return nil
}
