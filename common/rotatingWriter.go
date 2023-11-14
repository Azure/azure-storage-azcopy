package common

import (
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

type rotatingWriter struct {
	filePath      string
	file          *os.File
	l             sync.RWMutex
	currentSuffix int32
	currentSize   uint64
	maxLogSize    uint64
}

func NewRotatingWriter(filePath string, size uint64) (io.WriteCloser, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, DEFAULT_FILE_PERM)
	if err != nil {
		return nil, err
	}

	return &rotatingWriter{
		file: file,
		filePath: filePath,
		maxLogSize: size,
	}, nil
}
// rotate() takes in a context inform of integer, and rotates log only
// if the context matches current suffix. 
// rotate() should be called with a RLock held. It'll return back with
// RLock held.
func (w *rotatingWriter) rotate(suffix int32) error {
	w.l.RUnlock()
	defer w.l.RLock()

	w.l.Lock()
	defer w.l.Unlock()

	if atomic.LoadInt32(&w.currentSuffix) > suffix {
		// This log has already been rotated.
		return nil
	}

	if err := w.file.Close(); err != nil {
		return err
	}

	suffixString := fmt.Sprintf(".%d.log", w.currentSuffix)
	if err := os.Rename(w.filePath, w.filePath + suffixString); err != nil {
		return err
	}
	
	atomic.AddInt32(&w.currentSuffix, 1)
	atomic.StoreUint64(&w.currentSize, 0)

	// create new one
	file, err := os.OpenFile(w.filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, DEFAULT_FILE_PERM)
	if err != nil {
		return err
	}

	w.file = file

	return nil
}

func (w *rotatingWriter) Close() error {
	return w.file.Close()
}

func (w *rotatingWriter) Write(p []byte) (n int, err error) {
	w.l.RLock()
	defer w.l.RUnlock()

	// We have to save curSuffix here so that if we rotate() the
	// same log file we checked here.
	currSuffix := atomic.LoadInt32(&w.currentSuffix)
	if atomic.AddUint64(&w.currentSize, uint64(len(p))) <= w.maxLogSize {
		// we've enough size
		return w.file.Write(p)
	}

	//1. Take out these bytes
	atomic.AddUint64(&w.currentSize, -uint64(len(p)))

	if err := w.rotate(currSuffix); err != nil {
			return 0, err
	}

	atomic.AddUint64(&w.currentSize, uint64(len(p)))
	return w.file.Write(p)
}