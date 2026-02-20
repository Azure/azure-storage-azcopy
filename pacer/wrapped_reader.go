package pacer

import (
	"fmt"
	"io"
)

type wrappedRSC struct {
	seeker io.Seeker

	wrappedRC
}

func (w *wrappedRSC) Seek(offset int64, whence int) (newLoc int64, err error) {
	newLoc, err = w.seeker.Seek(offset, whence)

	if err != nil {
		return
	}

	w.parentReq.informSeek(newLoc)
	return
}

type wrappedRC struct {
	parentReq   Request
	childReader io.ReadCloser
}

func (w *wrappedRC) Read(p []byte) (n int, err error) {
	if w.parentReq.RemainingReads() <= 0 {
		return 0, nil
	}

	var allocated int
	allocated, err = w.parentReq.requestUse(len(p))

	if err != nil {
		return 0, fmt.Errorf("failed to get allocation to read: %w", err)
	}

	p = p[:allocated]

	n, err = w.childReader.Read(p)

	w.parentReq.confirmUse(n, true)
	return n, err
}

func (w *wrappedRC) Close() error {
	w.parentReq.Discard()
	return w.childReader.Close()
}
