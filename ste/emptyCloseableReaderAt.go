package ste

type emptyCloseableReaderAt struct {
}

func (e emptyCloseableReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	return 0, nil
}

func (e emptyCloseableReaderAt) Close() error {
	// no-op
	return nil
}
