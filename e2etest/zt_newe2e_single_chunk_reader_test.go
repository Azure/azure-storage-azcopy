package e2etest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestSingleChunkReader_RetriesAfterProxyClosesReplayedBody reproduces a retry failure seen
// when a proxy resets a block upload while net/http is replaying its request body:
//
//  1. AzCopy uploads a 400 MiB sparse file in 8 MiB blocks with one active connection.
//  2. For the first block, the proxy returns HTTP 307. net/http follows the redirect by
//     calling Request.GetBody, whose replay body reaches the original singleChunkReader.
//  3. The proxy reads 64 KiB from that replay and aborts the TCP connection with an RST.
//     The transport consequently closes the replay body, setting the reader's closed state.
//  4. azcore retries the original StageBlock request. The fixed reader clears closed state
//     left by the prior attempt before re-reading the source, while still detecting a Close
//     that races with the current read. Before the fix, this retry failed locally with
//     "closed while reading" and sent no block data.
//  5. The proxy captures the retry body and requires all 8 MiB before forwarding it to a
//     fake Storage origin. The test also requires all 50 blocks and the final block-list
//     commit, proving that AzCopy recovered from the mid-body reset and completed the upload.
//
// The origin and proxy are entirely local; no production fault hook or Storage account is used.
func TestSingleChunkReader_RetriesAfterProxyClosesReplayedBody(t *testing.T) {
	a := assert.New(t)
	const (
		uploadSize = int64(400 * 1024 * 1024)
		blockSize  = int64(8 * 1024 * 1024)
	)

	tempDir := t.TempDir()
	sourcePath := filepath.Join(tempDir, "source.bin")
	source, err := os.Create(sourcePath)
	a.NoError(err)
	a.NoError(source.Truncate(uploadSize))
	a.NoError(source.Close())

	var mu sync.Mutex
	var serverErr error
	var targetBlockID string
	var targetOriginAttempts int
	var committed bool
	stagedBlocks := make(map[string]int)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Query().Get("restype") == "account" {
			w.Header().Set("x-ms-sku-name", "Standard_LRS")
			w.Header().Set("x-ms-account-kind", "StorageV2")
			w.WriteHeader(http.StatusOK)
			return
		}

		if req.Method == http.MethodHead {
			mu.Lock()
			isCommitted := committed
			mu.Unlock()
			if isCommitted {
				w.Header().Set("Content-Length", fmt.Sprint(uploadSize))
				w.Header().Set("ETag", `"single-chunk-reader-e2e"`)
				w.Header().Set("Last-Modified", "Mon, 17 Aug 2026 00:00:00 GMT")
				w.Header().Set("x-ms-blob-type", "BlockBlob")
				w.WriteHeader(http.StatusOK)
				return
			}
			w.Header().Set("x-ms-error-code", "BlobNotFound")
			w.WriteHeader(http.StatusNotFound)
			return
		}

		if req.Method == http.MethodPut && req.URL.Query().Get("comp") == "blocklist" {
			_, readErr := io.Copy(io.Discard, req.Body)
			mu.Lock()
			if readErr != nil {
				serverErr = fmt.Errorf("read block list request body: %w", readErr)
			}
			committed = true
			mu.Unlock()
			w.Header().Set("ETag", `"single-chunk-reader-e2e"`)
			w.Header().Set("x-ms-request-id", "single-chunk-reader-e2e")
			w.WriteHeader(http.StatusCreated)
			return
		}

		if req.Method != http.MethodPut || req.URL.Query().Get("comp") != "block" {
			w.Header().Set("x-ms-error-code", "BlobNotFound")
			w.WriteHeader(http.StatusNotFound)
			return
		}

		blockID := req.URL.Query().Get("blockid")
		bytesRead, readErr := io.Copy(io.Discard, req.Body)
		mu.Lock()
		defer mu.Unlock()
		if readErr != nil {
			serverErr = fmt.Errorf("read block %q request body: %w", blockID, readErr)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if bytesRead != blockSize {
			serverErr = fmt.Errorf("block %q body length was %d, expected %d", blockID, bytesRead, blockSize)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		stagedBlocks[blockID]++
		if blockID == targetBlockID {
			targetOriginAttempts++
		}

		w.Header().Set("ETag", `"single-chunk-reader-e2e"`)
		w.Header().Set("x-ms-request-id", "single-chunk-reader-e2e")
		w.WriteHeader(http.StatusCreated)
	}))
	defer origin.Close()

	originURL, err := url.Parse(origin.URL)
	a.NoError(err)
	reverseProxy := httputil.NewSingleHostReverseProxy(originURL)
	reverseProxy.ErrorHandler = func(w http.ResponseWriter, req *http.Request, proxyErr error) {
		mu.Lock()
		if serverErr == nil {
			serverErr = fmt.Errorf("reverse proxy: %w", proxyErr)
		}
		mu.Unlock()
		w.WriteHeader(http.StatusBadGateway)
	}

	var targetOriginalAttempts int
	var targetReplayAttempts int
	var targetRetryBodyBytes int64
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPut && req.URL.Query().Get("comp") == "block" {
			blockID := req.URL.Query().Get("blockid")
			isReplay := len(req.URL.Path) >= len("/replayed/") && req.URL.Path[:len("/replayed/")] == "/replayed/"
			mu.Lock()
			if targetBlockID == "" && !isReplay {
				targetBlockID = blockID
			}
			isTarget := blockID == targetBlockID
			if isTarget && isReplay {
				targetReplayAttempts++
			} else if isTarget {
				targetOriginalAttempts++
			}
			originalAttempt := targetOriginalAttempts
			mu.Unlock()

			if isTarget && !isReplay && originalAttempt == 1 {
				// A 307 makes net/http replay the request through GetBody, which exposes the original chunk reader.
				location := "/replayed" + req.URL.EscapedPath()
				if req.URL.RawQuery != "" {
					location += "?" + req.URL.RawQuery
				}
				w.Header().Set("Location", location)
				w.WriteHeader(http.StatusTemporaryRedirect)
				return
			}

			if isTarget && isReplay {
				// Reset while the replay body is in flight so the transport closes that request body.
				const partialBodySize = 64 * 1024
				if _, readErr := io.CopyN(io.Discard, req.Body, partialBodySize); readErr != nil {
					mu.Lock()
					serverErr = fmt.Errorf("read replayed request body: %w", readErr)
					mu.Unlock()
					return
				}
				resetProxyConnection(w, &mu, &serverErr)
				return
			}

			if isTarget && !isReplay && originalAttempt == 2 {
				// Inspect the retry before forwarding it; a stale reader sends zero bytes instead of a full block.
				responseController := http.NewResponseController(w)
				if deadlineErr := responseController.SetReadDeadline(time.Now().Add(5 * time.Second)); deadlineErr != nil {
					mu.Lock()
					serverErr = fmt.Errorf("set proxy read deadline: %w", deadlineErr)
					mu.Unlock()
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				var body bytes.Buffer
				bytesRead, readErr := io.Copy(&body, req.Body)
				_ = responseController.SetReadDeadline(time.Time{})
				mu.Lock()
				targetRetryBodyBytes = bytesRead
				mu.Unlock()
				if readErr != nil || bytesRead != blockSize {
					w.Header().Set("x-ms-error-code", "InvalidRequest")
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				req.Body = io.NopCloser(bytes.NewReader(body.Bytes()))
			}
		}

		reverseProxy.ServeHTTP(w, req)
	}))
	defer proxy.Close()

	destination := proxy.URL + "/container/destination.bin?sv=2021-12-02&sr=b&sp=rw&se=2099-01-01&sig=fake"
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	command := exec.CommandContext(
		ctx,
		GlobalInputManager{}.GetExecutablePath(),
		"copy", sourcePath, destination,
		"--from-to=LocalBlob",
		"--as-subdir=false",
		"--block-size-mb=8",
		"--put-blob-size-mb=8",
		"--output-type=json",
		"--log-level=DEBUG",
	)
	command.Env = append(os.Environ(),
		"AZCOPY_CONCURRENCY_VALUE=1",
		"AZCOPY_LOG_LOCATION="+filepath.Join(tempDir, "logs"),
		"AZCOPY_JOB_PLAN_LOCATION="+filepath.Join(tempDir, "plans"),
	)
	output, err := command.CombinedOutput()
	a.NoError(err, "AzCopy failed: %s", output)

	mu.Lock()
	defer mu.Unlock()
	a.NoError(serverErr)
	a.NotEmpty(targetBlockID)
	a.Equal(2, targetOriginalAttempts, "azcore should retry the target block after the redirected body is reset")
	a.Equal(1, targetReplayAttempts, "the proxy should reset one net/http GetBody replay")
	a.Equal(blockSize, targetRetryBodyBytes, "the retried target block body should be complete")
	a.Equal(1, targetOriginAttempts, "only the successful target-block retry should reach Storage")
	a.Len(stagedBlocks, int(uploadSize/blockSize), "every 8 MiB block should be staged")
	a.True(committed, "block list should be committed")
}

// resetProxyConnection performs an abortive close. Hijack removes the socket from net/http's
// control, and zero linger makes closing the TCP socket send an RST rather than a normal FIN.
func resetProxyConnection(w http.ResponseWriter, mu *sync.Mutex, serverErr *error) {
	connection, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		mu.Lock()
		*serverErr = fmt.Errorf("hijack proxy connection: %w", err)
		mu.Unlock()
		return
	}
	if tcpConnection, ok := connection.(*net.TCPConn); ok {
		_ = tcpConnection.SetLinger(0)
	}
	_ = connection.Close()
}
