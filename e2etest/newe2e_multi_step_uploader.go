package e2etest

import (
	"bytes"
	"fmt"
	"io"
)

type MultiStepUploader struct { // GetTypeOrZero[T] will prove useful.
	Init        func(size int64) error
	UploadRange func(block io.ReadSeeker, state MultiStepUploaderState) error
	Finalize    func() error
	BlockSize   int64
	Parallel    bool
}

type MultiStepUploaderState struct {
	BlockSize  int64
	Offset     int64
	BlockIndex int64
	BlockCount int64
}

func (m *MultiStepUploader) GetBlockCount(size int64) int64 {
	quo := size / m.BlockSize
	rem := size % m.BlockSize

	if rem > 0 {
		quo++
	}

	return quo
}

func (m *MultiStepUploader) UploadContents(content ObjectContentContainer) error {
	if m.Init != nil {
		err := m.Init(content.Size())
		if err != nil {
			return fmt.Errorf("failed to initialize: %w", err)
		}
	}

	if content == nil {
		content = NewZeroContentContainerBuffer(0)
	}

	size := content.Size()
	reader := content.Reader()
	blockCount := m.GetBlockCount(size)

	offset := int64(0)
	blockIndex := int64(0)

	buf := make([]byte, m.BlockSize)
	for {
		n, err := reader.Read(buf)
		if err != nil && err != io.EOF {
			return fmt.Errorf("failed to read content (offset %d (block %d/%d), total %d): %w", offset, blockIndex, blockCount, size, err)
		} else if err == io.EOF {
			buf = buf[:n] // reduce buffer size for final block
		}

		if m.UploadRange != nil {
			err = m.UploadRange(
				bytes.NewReader(buf),
				MultiStepUploaderState{BlockSize: int64(n), Offset: offset, BlockIndex: blockIndex, BlockCount: blockCount})

			if err != nil {
				return fmt.Errorf("failed to upload content (offset %d (block %d/%d), total %d): %w", offset, blockIndex, blockCount, size, err)
			}
		}

		offset += int64(n)
		blockIndex++
		if offset >= size { // Offset will never be above size, but on the off chance it is, may as well handle it properly.
			break
		}
	}

	if m.Finalize != nil {
		err := m.Finalize()
		if err != nil {
			return fmt.Errorf("failed to finalize: %w", err)
		}
	}

	return nil
}
