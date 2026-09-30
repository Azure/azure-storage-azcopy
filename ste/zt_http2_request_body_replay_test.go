package ste

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

type http2ReplayReaderAt struct {
	*bytes.Reader
}

func (http2ReplayReaderAt) Close() error { return nil }

type http2ReplayLogger struct{}

func (http2ReplayLogger) ShouldLog(common.LogLevel) bool { return false }
func (http2ReplayLogger) Log(common.LogLevel, string)    {}
func (http2ReplayLogger) Panic(err error)                { panic(err) }

type countingTransporter struct {
	client *http.Client
	calls  atomic.Int32
}

func (t *countingTransporter) Do(req *http.Request) (*http.Response, error) {
	t.calls.Add(1)
	return t.client.Do(req)
}

type http2ReplayResult struct {
	firstRequest  []byte
	secondRequest []byte
}

func runHTTP2ReplayPeer(listener net.Listener, result chan<- http2ReplayResult, blockSize, resetAfterBytes int64) error {
	connection, err := listener.Accept()
	if err != nil {
		return err
	}
	defer connection.Close()

	tlsConnection := connection.(*tls.Conn)
	if err = tlsConnection.Handshake(); err != nil {
		return err
	}
	if tlsConnection.ConnectionState().NegotiatedProtocol != http2.NextProtoTLS {
		return fmt.Errorf("negotiated %q instead of HTTP/2", tlsConnection.ConnectionState().NegotiatedProtocol)
	}

	preface := make([]byte, len(http2.ClientPreface))
	if _, err = io.ReadFull(connection, preface); err != nil {
		return err
	}
	if string(preface) != http2.ClientPreface {
		return fmt.Errorf("unexpected HTTP/2 client preface %q", preface)
	}

	framer := http2.NewFramer(connection, connection)
	if err = framer.WriteSettings(http2.Setting{
		ID:  http2.SettingInitialWindowSize,
		Val: uint32(blockSize),
	}); err != nil {
		return err
	}
	if err = framer.WriteWindowUpdate(0, uint32(blockSize*2)); err != nil {
		return err
	}

	requests := make(map[uint32][]byte)
	var firstStreamID uint32
	resetSent := false
	for {
		frame, readErr := framer.ReadFrame()
		if readErr != nil {
			return readErr
		}

		switch frame := frame.(type) {
		case *http2.SettingsFrame:
			if !frame.IsAck() {
				if err = framer.WriteSettingsAck(); err != nil {
					return err
				}
			}
		case *http2.PingFrame:
			if !frame.Flags.Has(http2.FlagPingAck) {
				if err = framer.WritePing(true, frame.Data); err != nil {
					return err
				}
			}
		case *http2.HeadersFrame:
			if firstStreamID == 0 {
				firstStreamID = frame.StreamID
			}
		case *http2.DataFrame:
			if frame.StreamID == firstStreamID && resetSent {
				continue
			}
			requests[frame.StreamID] = append(requests[frame.StreamID], frame.Data()...)
			if frame.StreamID == firstStreamID && !resetSent && int64(len(requests[frame.StreamID])) >= resetAfterBytes {
				requests[frame.StreamID] = requests[frame.StreamID][:resetAfterBytes]
				resetSent = true
				if err = framer.WriteRSTStream(frame.StreamID, http2.ErrCodeRefusedStream); err != nil {
					return err
				}
				continue
			}

			if frame.StreamID != firstStreamID && frame.StreamEnded() {
				var headerBlock bytes.Buffer
				encoder := hpack.NewEncoder(&headerBlock)
				if err = encoder.WriteField(hpack.HeaderField{Name: ":status", Value: "201"}); err != nil {
					return err
				}
				if err = encoder.WriteField(hpack.HeaderField{Name: "x-ms-request-id", Value: "http2-replay-test"}); err != nil {
					return err
				}
				if err = framer.WriteHeaders(http2.HeadersFrameParam{
					StreamID:      frame.StreamID,
					BlockFragment: headerBlock.Bytes(),
					EndHeaders:    true,
					EndStream:     true,
				}); err != nil {
					return err
				}
				result <- http2ReplayResult{
					firstRequest:  requests[firstStreamID],
					secondRequest: requests[frame.StreamID],
				}
				return nil
			}
		}
	}
}

func TestHTTP2ConnectionResetReplaysFullBlock(t *testing.T) {
	const (
		blockSize       = int64(8 * 1024 * 1024)
		resetAfterBytes = int64(512 * 1024)
	)

	body := make([]byte, blockSize)
	for index := range body {
		body[index] = byte(index / (64 * 1024))
	}

	certificateSource := httptest.NewTLSServer(http.NotFoundHandler())
	tlsCertificate := certificateSource.TLS.Certificates[0]
	clientTransport := certificateSource.Client().Transport.(*http.Transport).Clone()
	certificateSource.Close()
	clientTransport.ForceAttemptHTTP2 = true

	tcpListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	tlsListener := tls.NewListener(tcpListener, &tls.Config{
		Certificates: []tls.Certificate{tlsCertificate},
		NextProtos:   []string{http2.NextProtoTLS},
	})
	t.Cleanup(func() { _ = tlsListener.Close() })

	replayResult := make(chan http2ReplayResult, 1)
	peerError := make(chan error, 1)
	go func() {
		peerError <- runHTTP2ReplayPeer(tlsListener, replayResult, blockSize, resetAfterBytes)
	}()

	transport := &countingTransporter{client: &http.Client{Transport: clientTransport}}
	client, err := blockblob.NewClientWithNoCredential("https://"+tlsListener.Addr().String()+"/container/blob", &blockblob.ClientOptions{
		ClientOptions: NewClientOptions(
			policy.RetryOptions{
				MaxRetries:    1,
				RetryDelay:    time.Millisecond,
				MaxRetryDelay: time.Millisecond,
			},
			policy.TelemetryOptions{},
			transport,
			LogOptions{},
			nil,
			nil,
		),
	})
	require.NoError(t, err)

	sourceFactory := func() (common.CloseableReaderAt, error) {
		return http2ReplayReaderAt{bytes.NewReader(body)}, nil
	}
	reader := common.NewSingleChunkReader(
		context.Background(),
		sourceFactory,
		common.NewChunkID("http2-replay", 0, blockSize),
		blockSize,
		nil,
		http2ReplayLogger{},
		common.NewMultiSizeSlicePool(blockSize),
		common.NewCacheLimiter(blockSize*2),
	)

	blockID := base64.StdEncoding.EncodeToString([]byte("http2-replay-block"))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err = client.StageBlock(ctx, blockID, newPacedRequestBody(ctx, reader, NewNullAutoPacer()), nil)
	require.NoError(t, err)

	result := <-replayResult
	require.NoError(t, <-peerError)
	require.Equal(t, body[:resetAfterBytes], result.firstRequest)
	require.Equal(t, body, result.secondRequest, fmt.Sprintf(
		"replacement HTTP/2 request sent %d bytes; expected a complete %d-byte replay",
		len(result.secondRequest), blockSize))
	require.Equal(t, int32(1), transport.calls.Load(),
		"the replay must occur inside Go's HTTP/2 transport, not as an azcore retry")
}

type http2TimeoutPacer struct {
	pacer
	readCount           atomic.Int32
	completedReads      atomic.Int32
	blockedReadSize     atomic.Int64
	staleReadBytes      atomic.Int64
	retryReadStarted    chan struct{}
	blockedReadFinished chan struct{}
	releaseAfter        time.Duration
}

func (p *http2TimeoutPacer) RequestTrafficAllocation(ctx context.Context, byteCount int64) error {
	switch p.readCount.Add(1) {
	case 2:
		p.blockedReadSize.Store(byteCount)
		timer := time.NewTimer(p.releaseAfter)
		defer timer.Stop()
		select {
		case <-p.retryReadStarted:
		case <-timer.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	case 3:
		close(p.retryReadStarted)
		select {
		case <-p.blockedReadFinished:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (p *http2TimeoutPacer) UndoRequest(byteCount int64) {
	if p.completedReads.Add(1) == 2 {
		p.staleReadBytes.Store(p.blockedReadSize.Load() - byteCount)
		close(p.blockedReadFinished)
	}
}

type http2TimeoutTransporter struct {
	baseTransport     *http.Transport
	calls             atomic.Int32
	firstAttemptError error
}

func (transport *http2TimeoutTransporter) Do(request *http.Request) (*http.Response, error) {
	attempt := transport.calls.Add(1)
	clientTransport := transport.baseTransport.Clone()
	defer clientTransport.CloseIdleConnections()
	client := &http.Client{Transport: clientTransport}
	warmup, err := http.NewRequestWithContext(request.Context(), http.MethodHead,
		request.URL.Scheme+"://"+request.URL.Host+"/warmup", nil)
	if err != nil {
		return nil, err
	}
	response, err := client.Do(warmup)
	if err != nil {
		return nil, err
	}
	if err = response.Body.Close(); err != nil {
		return nil, err
	}
	response, err = client.Do(request)
	if attempt == 1 {
		transport.firstAttemptError = err
	}
	return response, err
}

func TestHTTP2TryTimeoutReplaysFullBlockWithPendingRead(t *testing.T) {
	const (
		blockSize  = int64(8 * 1024 * 1024)
		readSize   = int64(512 * 1024)
		tryTimeout = time.Second
	)

	body := make([]byte, blockSize)
	for index := range body {
		body[index] = byte(index / (64 * 1024))
	}

	type receivedRequest struct {
		attempt       int32
		body          []byte
		remoteAddress string
		protocol      int
		contentLength int64
		requestID     string
		uri           string
		readError     error
	}
	requests := make(chan receivedRequest, 2)
	var requestCount atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodHead && request.URL.Path == "/warmup" {
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		attempt := requestCount.Add(1)
		received, readErr := io.ReadAll(io.LimitReader(request.Body, blockSize+1))
		requests <- receivedRequest{
			attempt:       attempt,
			body:          received,
			remoteAddress: request.RemoteAddr,
			protocol:      request.ProtoMajor,
			contentLength: request.ContentLength,
			requestID:     request.Header.Get("x-ms-client-request-id"),
			uri:           request.RequestURI,
			readError:     readErr,
		}
		if readErr != nil || int64(len(received)) != blockSize {
			http.Error(writer, "incomplete block", http.StatusBadRequest)
			return
		}
		writer.Header().Set("x-ms-request-id", "http2-timeout-test")
		writer.WriteHeader(http.StatusCreated)
	}))
	server.EnableHTTP2 = true
	require.NoError(t, http2.ConfigureServer(server.Config, &http2.Server{MaxReadFrameSize: uint32(readSize)}))
	server.StartTLS()
	defer server.Close()

	clientTransport := server.Client().Transport.(*http.Transport).Clone()
	clientTransport.ForceAttemptHTTP2 = true
	defer clientTransport.CloseIdleConnections()
	transport := &http2TimeoutTransporter{baseTransport: clientTransport}
	client, err := blockblob.NewClientWithNoCredential(server.URL+"/container/blob", &blockblob.ClientOptions{
		ClientOptions: NewClientOptions(
			policy.RetryOptions{
				MaxRetries:    1,
				TryTimeout:    tryTimeout,
				RetryDelay:    time.Millisecond,
				MaxRetryDelay: time.Millisecond,
			},
			policy.TelemetryOptions{},
			transport,
			LogOptions{},
			nil,
			nil,
		),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	reader := common.NewSingleChunkReader(
		ctx,
		func() (common.CloseableReaderAt, error) {
			return http2ReplayReaderAt{bytes.NewReader(body)}, nil
		},
		common.NewChunkID("http2-timeout", 0, blockSize),
		blockSize,
		nil,
		http2ReplayLogger{},
		common.NewMultiSizeSlicePool(blockSize),
		common.NewCacheLimiter(blockSize*2),
	)
	defer reader.Close()
	pacing := &http2TimeoutPacer{
		pacer:               NewNullAutoPacer(),
		retryReadStarted:    make(chan struct{}),
		blockedReadFinished: make(chan struct{}),
		releaseAfter:        2 * tryTimeout,
	}
	blockID := base64.StdEncoding.EncodeToString([]byte("http2-timeout-block"))
	_, uploadErr := client.StageBlock(ctx, blockID, newPacedRequestBody(ctx, reader, pacing), nil)

	var received [2]receivedRequest
	for index := range received {
		select {
		case request := <-requests:
			require.Contains(t, []int32{1, 2}, request.attempt)
			received[request.attempt-1] = request
		case <-ctx.Done():
			t.Fatalf("received %d requests: %v; StageBlock: %v", index, ctx.Err(), uploadErr)
		}
	}
	first, second := received[0], received[1]
	retryIsSuffix := len(second.body) <= len(body) && bytes.Equal(body[len(body)-len(second.body):], second.body)
	t.Logf("first request: %d bytes (%v); retry: %d/%d bytes (%v); pending read consumed: %d bytes; retry matches block suffix: %t; StageBlock: %v",
		len(first.body), first.readError, len(second.body), second.contentLength, second.readError,
		pacing.staleReadBytes.Load(), retryIsSuffix, uploadErr)
	require.Equal(t, int32(2), transport.calls.Load(), "must exercise an SDK retry")
	require.True(t, errors.Is(transport.firstAttemptError, context.DeadlineExceeded),
		"first attempt must fail due to its per-try deadline: %v", transport.firstAttemptError)
	require.Equal(t, 2, first.protocol)
	require.Equal(t, 2, second.protocol)
	require.NotEqual(t, first.remoteAddress, second.remoteAddress, "retry must use a different TCP connection")
	require.NotEmpty(t, first.requestID)
	require.Equal(t, first.requestID, second.requestID)
	require.Equal(t, first.uri, second.uri)
	require.Equal(t, int(readSize), len(first.body))
	require.True(t, bytes.Equal(body[:readSize], first.body), "first attempt must send the block's prefix")
	require.Equal(t, blockSize, second.contentLength)
	require.Equal(t, int(blockSize), len(second.body), "retry must replay the entire block despite the timed-out attempt's pending read")
	require.True(t, bytes.Equal(body, second.body), "retry must start at byte zero and preserve every byte")
	require.Zero(t, pacing.staleReadBytes.Load(), "a read spanning a rewind must not consume retry bytes")
	require.NoError(t, second.readError)
	require.NoError(t, uploadErr)
}
