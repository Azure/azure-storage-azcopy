package common

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCheckpoint6ReaderRetryReturnsExactBudget(t *testing.T) {
	data := bytes.Repeat([]byte("retry"), 64)
	limiter := &cacheLimiter{limit: int64(len(data) * 2)}
	reader := NewSingleChunkReader(context.Background(), func() (CloseableReaderAt, error) {
		return testCloseableReaderAt{bytes.NewReader(data)}, nil
	}, NewChunkID("local", 0, int64(len(data))), int64(len(data)), nil, nil,
		NewMultiSizeSlicePool(int64(len(data))), limiter)
	for range 10 {
		require.NoError(t, reader.BlockingPrefetch(bytes.NewReader(data), false))
		require.Equal(t, int64(len(data)), atomic.LoadInt64(&limiter.value))
		require.NoError(t, reader.Close(), "early close must support a nil logger")
		require.Zero(t, atomic.LoadInt64(&limiter.value))
		_, err := reader.Seek(0, io.SeekStart)
		require.NoError(t, err)
		actual, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.Equal(t, data, actual)
		require.Zero(t, atomic.LoadInt64(&limiter.value))
		_, err = reader.Seek(0, io.SeekStart)
		require.NoError(t, err)
	}
	require.NoError(t, reader.Close())
	require.Zero(t, atomic.LoadInt64(&limiter.value))
}

func TestCheckpoint6ReaderCancellationReturnsBuffer(t *testing.T) {
	const size = int64(4096)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	source := checkpoint6BlockingReader{entered: entered, release: release}
	limiter := &cacheLimiter{limit: size * 2}
	reader := NewSingleChunkReader(ctx, nil, NewChunkID("cancel", 0, size), size, nil, nil, NewMultiSizeSlicePool(size), limiter)
	done := make(chan error, 1)
	go func() { done <- reader.BlockingPrefetch(source, false) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("prefetch did not start")
	}
	cancel()
	unblock()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled prefetch did not return")
	}
	require.NoError(t, reader.Close())
	require.Zero(t, atomic.LoadInt64(&limiter.value))
}

type checkpoint6BlockingReader struct {
	entered chan struct{}
	release chan struct{}
}

func (r checkpoint6BlockingReader) ReadAt(buffer []byte, _ int64) (int, error) {
	close(r.entered)
	<-r.release
	clear(buffer)
	return len(buffer), nil
}

func TestCheckpoint6LegacyExtensionCompatibility(t *testing.T) {
	require.False(t, (HTTPResponseExtension{}).IsSuccessStatusCode(http.StatusOK))
	response := HTTPResponseExtension{Response: &http.Response{StatusCode: http.StatusCreated}}
	require.True(t, response.IsSuccessStatusCode(http.StatusOK, http.StatusCreated))
	require.False(t, response.IsSuccessStatusCode(http.StatusOK))
	require.Nil(t, (ByteSliceExtension{}).RemoveBOM())
	require.Equal(t, []byte("data"), (ByteSliceExtension{ByteSlice: []byte("\xef\xbb\xbfdata")}).RemoveBOM())
	require.Equal(t, []byte("data"), (ByteSliceExtension{ByteSlice: []byte("data")}).RemoveBOM())
}
