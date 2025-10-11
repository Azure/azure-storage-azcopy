package e2etest

import (
	"bytes"
	"fmt"
	"io"
	"runtime"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type MultiStepUploader struct { // GetTypeOrZero[T] will prove useful.
	Init        func(size int64) error
	UploadRange func(block io.ReadSeekCloser, state MultiStepUploaderState) error
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
		content = NewZeroObjectContentContainer(0)
	}

	size := content.Size()
	reader := content.Reader()
	blockCount := m.GetBlockCount(size)

	offset := int64(0)
	blockIndex := int64(0)

	wg := &sync.WaitGroup{}
	pool := &sync.Pool{}
	threads := common.Iff(m.Parallel, runtime.NumCPU(), 1) // 1 thread if not parallel
	for i := 0; i < threads; i++ {
		pool.Put(&struct{ threadID int }{
			threadID: i,
		})
	}

	chunkErrors := make(map[int64]error)
	errMutex := &sync.Mutex{}

	for {
		buf := make([]byte, m.BlockSize)
		n, err := reader.Read(buf)
		if err != nil && err != io.EOF {
			return fmt.Errorf("failed to read content (offset %d (block %d/%d), total %d): %w", offset, blockIndex, blockCount, size, err)
		} else if err == io.EOF {
			buf = buf[:n] // reduce buffer size for final block
		}

		wg.Add(1)
		thread := pool.Get().(*struct{ threadID int })
		go func() {
			defer wg.Done()
			defer pool.Put(thread)

			if m.UploadRange != nil {
				err = m.UploadRange(
					streaming.NopCloser(bytes.NewReader(buf)),
					MultiStepUploaderState{BlockSize: int64(n), Offset: offset, BlockIndex: blockIndex, BlockCount: blockCount})

				if err != nil {
					errMutex.Lock()
					defer errMutex.Unlock()
					chunkErrors[blockIndex] = fmt.Errorf("failed to upload content (thread %d, offset %d (block %d/%d), total %d): %w", thread.threadID, offset, blockIndex, blockCount, size, err)
				}
			}
		}()

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
