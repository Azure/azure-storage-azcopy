package ste

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

type http2DownloadResult struct {
	firstRange  string
	secondRange string
}

func writeHTTP2DownloadHeaders(framer *http2.Framer, streamID uint32, offset, count, blobSize int64) error {
	var headerBlock bytes.Buffer
	encoder := hpack.NewEncoder(&headerBlock)
	for _, field := range []hpack.HeaderField{
		{Name: ":status", Value: "206"},
		{Name: "content-length", Value: fmt.Sprint(count)},
		{Name: "content-range", Value: fmt.Sprintf("bytes %d-%d/%d", offset, offset+count-1, blobSize)},
		{Name: "etag", Value: `"http2-download-retry"`},
		{Name: "x-ms-request-id", Value: "http2-download-retry-test"},
	} {
		if err := encoder.WriteField(field); err != nil {
			return err
		}
	}
	return framer.WriteHeaders(http2.HeadersFrameParam{
		StreamID:      streamID,
		BlockFragment: headerBlock.Bytes(),
		EndHeaders:    true,
	})
}

func writeHTTP2Data(framer *http2.Framer, streamID uint32, body []byte, endStream bool) error {
	const maxFrameSize = 16 * 1024
	for len(body) > 0 {
		frameSize := min(len(body), maxFrameSize)
		lastFrame := frameSize == len(body)
		if err := framer.WriteData(streamID, endStream && lastFrame, body[:frameSize]); err != nil {
			return err
		}
		body = body[frameSize:]
	}
	return nil
}

func runHTTP2DownloadPeer(listener net.Listener, body []byte, resetAfterBytes int64, userAgent chan<- string, result chan<- http2DownloadResult) error {
	var firstRange string
	for requestIndex := 0; requestIndex < 2; requestIndex++ {
		connection, err := listener.Accept()
		if err != nil {
			return err
		}
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
		if err = framer.WriteSettings(); err != nil {
			return err
		}
		if err = framer.WriteWindowUpdate(0, uint32(len(body))); err != nil {
			return err
		}

		requestHeaders := make(map[string]string)
		decoder := hpack.NewDecoder(4096, func(field hpack.HeaderField) {
			requestHeaders[field.Name] = field.Value
		})
		requestComplete := false
		for !requestComplete {
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
				if _, err = decoder.Write(frame.HeaderBlockFragment()); err != nil {
					return err
				}
				if !frame.HeadersEnded() {
					return fmt.Errorf("continuation headers are not supported by this test peer")
				}

				requestRange := requestHeaders["x-ms-range"]
				if requestIndex == 0 {
					userAgent <- requestHeaders["user-agent"]
					firstRange = requestRange
					if err = writeHTTP2DownloadHeaders(framer, frame.StreamID, 0, int64(len(body)), int64(len(body))); err != nil {
						return err
					}
					if err = writeHTTP2Data(framer, frame.StreamID, body[:resetAfterBytes], false); err != nil {
						return err
					}
					if err = framer.WriteRSTStream(frame.StreamID, http2.ErrCodeProtocol); err != nil {
						return err
					}
				} else {
					remaining := body[resetAfterBytes:]
					if err = writeHTTP2DownloadHeaders(framer, frame.StreamID, resetAfterBytes, int64(len(remaining)), int64(len(body))); err != nil {
						return err
					}
					if err = writeHTTP2Data(framer, frame.StreamID, remaining, true); err != nil {
						return err
					}
					result <- http2DownloadResult{firstRange: firstRange, secondRange: requestRange}
				}
				requestComplete = true
			}
		}
		_ = connection.Close()
	}
	return nil
}

func TestHTTP2ProtocolErrorRetriesRemainingDownloadRange(t *testing.T) {
	const (
		blobSize        = int64(8 * 1024 * 1024)
		resetAfterBytes = int64(4 * 1024 * 1024)
	)

	body := make([]byte, blobSize)
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

	downloadResult := make(chan http2DownloadResult, 1)
	userAgent := make(chan string, 1)
	peerError := make(chan error, 1)
	go func() {
		peerError <- runHTTP2DownloadPeer(tlsListener, body, resetAfterBytes, userAgent, downloadResult)
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	response, err := client.DownloadStream(ctx, &blob.DownloadStreamOptions{
		Range: blob.HTTPRange{Offset: 0, Count: blobSize},
	})
	require.NoError(t, err)

	retryReader := response.NewRetryReader(ctx, &blob.RetryReaderOptions{MaxRetries: 1})
	defer retryReader.Close()
	downloaded, err := io.ReadAll(retryReader)
	if err != nil && strings.Contains(<-userAgent, "azsdk-go-azblob/v1.8.0") {
		t.Skip("azblob v1.8.0 does not retry HTTP/2 PROTOCOL_ERROR; this test runs after the SDK is updated")
	}
	require.NoError(t, err)
	require.Equal(t, body, downloaded)

	result := <-downloadResult
	require.NoError(t, <-peerError)
	require.Equal(t, "bytes=0-8388607", result.firstRange)
	require.Equal(t, "bytes=4194304-8388607", result.secondRange)
	require.Equal(t, int32(2), transport.calls.Load(),
		"the resumed range must be requested by azblob RetryReader")
}
