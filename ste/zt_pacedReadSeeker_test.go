package ste

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type pacedReadGate struct {
	*nullAutoPacer
	entered chan struct{}
	release chan struct{}
}

func (p *pacedReadGate) RequestTrafficAllocation(ctx context.Context, byteCount int64) error {
	select {
	case p.entered <- struct{}{}:
	default:
	}
	select {
	case <-p.release:
		return p.nullAutoPacer.RequestTrafficAllocation(ctx, byteCount)
	case <-ctx.Done():
		return ctx.Err()
	}
}

type pacedReadResult struct {
	count int
	err   error
}

func TestPacedReadSeekerPendingReadAcrossSeek(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		offset    int64
		seekFails bool
		nextBody  string
	}{
		{name: "rewind", offset: 0, nextBody: "abcdef"},
		{name: "seek forward", offset: 3, nextBody: "def"},
		{name: "failed seek", offset: -1, seekFails: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			pacing := &pacedReadGate{
				nullAutoPacer: NewNullAutoPacer(),
				entered:       make(chan struct{}, 1),
				release:       make(chan struct{}),
			}
			reader := newPacedRequestBody(ctx, bytes.NewReader([]byte("abcdef")), pacing)
			defer reader.Close()
			_, err := reader.Seek(2, io.SeekStart)
			require.NoError(t, err)

			buffer := make([]byte, 8)
			result := make(chan pacedReadResult, 1)
			go func() {
				count, readErr := reader.Read(buffer)
				result <- pacedReadResult{count: count, err: readErr}
			}()
			select {
			case <-pacing.entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}

			_, err = reader.Seek(testCase.offset, io.SeekStart)
			if testCase.seekFails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			close(pacing.release)

			select {
			case read := <-result:
				if testCase.seekFails {
					require.NoError(t, read.err)
					require.Equal(t, "cdef", string(buffer[:read.count]))
					require.Equal(t, int64(4), pacing.GetTotalTraffic())
					return
				}
				require.ErrorIs(t, read.err, io.ErrClosedPipe)
				require.Zero(t, read.count)
				require.Equal(t, make([]byte, len(buffer)), buffer)
				require.Zero(t, pacing.GetTotalTraffic(), "stale reads must refund their full allocation")
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}

			remaining, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.Equal(t, testCase.nextBody, string(remaining))
			require.Equal(t, int64(len(remaining)), pacing.GetTotalTraffic())
		})
	}
}

type pacedBlockingResponse struct {
	io.ReadCloser
	readStarted chan struct{}
}

func (body *pacedBlockingResponse) Read(buffer []byte) (int, error) {
	close(body.readStarted)
	return body.ReadCloser.Read(buffer)
}

func TestPacedReadSeekerCloseUnblocksResponseRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pipeReader, pipeWriter := io.Pipe()
	defer pipeReader.Close()
	defer pipeWriter.Close()
	source := &pacedBlockingResponse{ReadCloser: pipeReader, readStarted: make(chan struct{})}
	pacing := NewNullAutoPacer()
	reader := newPacedResponseBody(ctx, source, pacing)
	readResult := make(chan pacedReadResult, 1)
	go func() {
		count, err := reader.Read(make([]byte, 8))
		readResult <- pacedReadResult{count: count, err: err}
	}()
	select {
	case <-source.readStarted:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	closeResult := make(chan error, 1)
	go func() { closeResult <- reader.Close() }()
	select {
	case err := <-closeResult:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("Close must not wait for a blocked Read")
	}
	select {
	case read := <-readResult:
		require.ErrorIs(t, read.err, io.ErrClosedPipe)
		require.Zero(t, read.count)
		require.Zero(t, pacing.GetTotalTraffic())
	case <-ctx.Done():
		t.Fatal("Read must unblock after Close")
	}
}
