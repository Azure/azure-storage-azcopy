// Copyright © Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package ste

import (
	"context"
	"errors"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/buildmode"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func Test_RetryNotificationPolicy_Timeout(t *testing.T) {
	a := assert.New(t)

	// generate a test server so we can capture and inspect the request
	srv := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.Header()["x-ms-error-code"] = []string{"OperationTimedOut"}
		res.WriteHeader(http.StatusInternalServerError)
	}))
	defer func() { srv.Close() }()

	p := newRetryNotificationPolicy()
	pl := runtime.NewPipeline("", "",
		runtime.PipelineOptions{PerRetry: []policy.Policy{p}},
		&policy.ClientOptions{Transport: http.DefaultClient},
	)
	var timeoutFromCtx bool
	ctx := withTimeoutNotification(context.Background(), &timeoutFromCtx)
	req, err := runtime.NewRequest(ctx, http.MethodPut, srv.URL)
	a.NoError(err)
	_, err = pl.Do(req)
	a.NoError(err)
	a.True(timeoutFromCtx)
}

func Test_RetryNotificationPolicy_TimeoutNegativeErrorCodeHeader(t *testing.T) {
	a := assert.New(t)

	// generate a test server so we can capture and inspect the request
	srv := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.Header()["x-ms-error-code"] = []string{"CannotVerifyCopySource"}
		res.WriteHeader(http.StatusInternalServerError)
	}))
	defer func() { srv.Close() }()

	p := newRetryNotificationPolicy()
	pl := runtime.NewPipeline("", "",
		runtime.PipelineOptions{PerRetry: []policy.Policy{p}},
		&policy.ClientOptions{Transport: http.DefaultClient},
	)
	var timeoutFromCtx bool
	ctx := withTimeoutNotification(context.Background(), &timeoutFromCtx)
	req, err := runtime.NewRequest(ctx, http.MethodPut, srv.URL)
	a.NoError(err)
	_, err = pl.Do(req)
	a.NoError(err)
	a.False(timeoutFromCtx)
}

func Test_RetryNotificationPolicy_TimeoutNegativeStatusCodeFail(t *testing.T) {
	a := assert.New(t)

	// generate a test server so we can capture and inspect the request
	srv := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(http.StatusNotFound)
	}))
	defer func() { srv.Close() }()

	p := newRetryNotificationPolicy()
	pl := runtime.NewPipeline("", "",
		runtime.PipelineOptions{PerRetry: []policy.Policy{p}},
		&policy.ClientOptions{Transport: http.DefaultClient},
	)
	var timeoutFromCtx bool
	ctx := withTimeoutNotification(context.Background(), &timeoutFromCtx)
	req, err := runtime.NewRequest(ctx, http.MethodPut, srv.URL)
	a.NoError(err)
	_, err = pl.Do(req)
	a.NoError(err)
	a.False(timeoutFromCtx)
}

func Test_RetryNotificationPolicy_TimeoutNegativeStatusCodeSuccess(t *testing.T) {
	a := assert.New(t)

	// generate a test server so we can capture and inspect the request
	srv := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(http.StatusOK)
	}))
	defer func() { srv.Close() }()

	p := newRetryNotificationPolicy()
	pl := runtime.NewPipeline("", "",
		runtime.PipelineOptions{PerRetry: []policy.Policy{p}},
		&policy.ClientOptions{Transport: http.DefaultClient},
	)
	var timeoutFromCtx bool
	ctx := withTimeoutNotification(context.Background(), &timeoutFromCtx)
	req, err := runtime.NewRequest(ctx, http.MethodPut, srv.URL)
	a.NoError(err)
	_, err = pl.Do(req)
	a.NoError(err)
	a.False(timeoutFromCtx)
}

type retryLogTransport func(*http.Request) (*http.Response, error)

func (f retryLogTransport) Do(req *http.Request) (*http.Response, error) {
	return f(req)
}

const retryLogBody = "<Error><Code>ServerBusy</Code><Message>Busy</Message></Error>"

func retryLogResponse(req *http.Request, status int) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Header:     http.Header{"X-Ms-Request-Id": []string{"test-request-id"}},
		Body:       io.NopCloser(strings.NewReader(retryLogBody)),
		Request:    req,
	}
}

func TestTransient503Logging(t *testing.T) {
	for _, tc := range []struct {
		name              string
		statuses          []int
		maxRetries        int32
		debug             bool
		stopRetry         bool
		retryAfter        bool
		overrideRetries   bool
		cancelDuringRetry bool
		slow              bool
		nonTransfer       bool
	}{
		{name: "recover on second try", statuses: []int{503, 200}, maxRetries: 8},
		{name: "recover after five 503s", statuses: []int{503, 503, 503, 503, 503, 200}, maxRetries: 8},
		{name: "sixth 503 also stays quiet", statuses: []int{503, 503, 503, 503, 503, 503, 200}, maxRetries: 8},
		{name: "exhaustion leaves logging to file failure", statuses: []int{503, 503, 503, 503, 503, 503, 503}, maxRetries: 6},
		{name: "retries disabled", statuses: []int{503}, maxRetries: -1},
		{name: "terminal fifth try", statuses: []int{503, 503, 503, 503, 503}, maxRetries: 4},
		{name: "SDK default retry count", statuses: []int{503, 503, 503, 503}},
		{name: "custom predicate stops retrying", statuses: []int{503}, maxRetries: 8, stopRetry: true},
		{name: "retry after exceeds cap", statuses: []int{503}, maxRetries: 8, retryAfter: true},
		{name: "per call retries disabled", statuses: []int{503}, maxRetries: 8, overrideRetries: true},
		{name: "cancel during retry delay", statuses: []int{503}, maxRetries: 8, cancelDuringRetry: true},
		{name: "slow transient 503 remains quiet", statuses: []int{503, 200}, maxRetries: 8, slow: true},
		{name: "500 unchanged", statuses: []int{500, 200}, maxRetries: 8},
		{name: "403 unchanged", statuses: []int{403}, maxRetries: 8},
		{name: "debug also suppresses retries", statuses: []int{503, 503, 503, 503, 503, 503, 200}, maxRetries: 8, debug: true},
		{name: "debug exhaustion is quiet", statuses: []int{503, 503, 503, 503, 503, 503, 503}, maxRetries: 6, debug: true},
		{name: "debug cancellation is quiet", statuses: []int{503}, maxRetries: 8, cancelDuringRetry: true, debug: true},
		{name: "scanning retry remains logged", statuses: []int{503, 200}, maxRetries: 8, nonTransfer: true},
		{name: "scanning exhaustion remains logged at debug", statuses: []int{503, 503}, maxRetries: 1, nonTransfer: true, debug: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			quiet503 := buildmode.HighPerf() && !tc.nonTransfer
			var errorMessages, allMessages []string
			log := LogOptions{
				RequestLogOptions: RequestLogOptions{SyslogDisabled: true, LogWarningIfTryOverThreshold: -1},
				ShouldLog:         func(level common.LogLevel) bool { return level <= common.LogInfo || tc.debug },
				Log: func(level common.LogLevel, message string) {
					allMessages = append(allMessages, message)
					require.NotContains(t, message, "secret-signature")
					require.NotContains(t, message, "secret-authorization")
					if level == common.LogError {
						errorMessages = append(errorMessages, message)
					}
				},
			}
			if tc.slow {
				log.RequestLogOptions.LogWarningIfTryOverThreshold = time.Nanosecond
			}
			attempts := 0
			transport := retryLogTransport(func(req *http.Request) (*http.Response, error) {
				require.Less(t, attempts, len(tc.statuses), "unexpected retry")
				require.Equal(t, "secret-signature", req.URL.Query().Get("sig"))
				require.Equal(t, "secret-authorization", req.Header.Get("Authorization"))
				if tc.debug {
					outgoing := fmt.Sprintf("==> OUTGOING REQUEST (Try=%d)", attempts+1)
					if quiet503 {
						require.NotContains(t, strings.Join(allMessages, "\n"), outgoing)
					} else {
						require.Contains(t, strings.Join(allMessages, "\n"), outgoing)
					}
				}
				response := retryLogResponse(req, tc.statuses[attempts])
				attempts++
				if tc.retryAfter {
					response.Header.Set("Retry-After", "3600")
				}
				return response, nil
			})
			options := NewClientOptions(policy.RetryOptions{
				MaxRetries: tc.maxRetries, RetryDelay: time.Nanosecond, MaxRetryDelay: time.Millisecond,
			}, policy.TelemetryOptions{}, transport, log, nil, nil)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if !tc.nonTransfer {
				ctx = context.WithValue(ctx, transferLogContextKey, true)
			}
			if tc.stopRetry {
				options.Retry.ShouldRetry = func(*http.Response, error) bool { return false }
			}
			if tc.cancelDuringRetry {
				options.Retry.RetryDelay = time.Hour
				options.Retry.MaxRetryDelay = time.Hour
				options.Retry.ShouldRetry = func(*http.Response, error) bool {
					cancel()
					return true
				}
			}
			if tc.overrideRetries {
				ctx = policy.WithRetryOptions(ctx, policy.RetryOptions{MaxRetries: -1})
			}
			stats := newPipelineNetworkStats(&NullConcurrencyTuner{})
			ctx = withPipelineNetworkStats(ctx, stats)
			pipeline := runtime.NewPipeline("", "", runtime.PipelineOptions{}, &options)
			req, err := runtime.NewRequest(ctx, http.MethodGet, "https://example.invalid/blob?sig=secret-signature")
			require.NoError(t, err)
			req.Raw().Header.Set("Authorization", "secret-authorization")
			response, err := pipeline.Do(req)
			if tc.cancelDuringRetry {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, len(tc.statuses), attempts)
			require.NotNil(t, response)
			require.Equal(t, tc.statuses[len(tc.statuses)-1], response.StatusCode)
			body, readErr := io.ReadAll(response.Body)
			require.NoError(t, readErr)
			require.NoError(t, response.Body.Close())
			if !tc.cancelDuringRetry {
				require.Equal(t, retryLogBody, string(body), "logging must preserve the response body")
			}
			var busy int64
			for _, status := range tc.statuses {
				if status == http.StatusServiceUnavailable {
					busy++
				}
			}
			require.Equal(t, busy, stats.GetTotalRetries(), "suppressed responses still contribute to throttling statistics")
			var errorTries []int
			for i, status := range tc.statuses {
				if status >= 400 && !(status == http.StatusServiceUnavailable && quiet503) {
					errorTries = append(errorTries, i+1)
				}
			}
			require.Len(t, errorMessages, len(errorTries))
			for i, attempt := range errorTries {
				require.Contains(t, errorMessages[i], fmt.Sprintf("(Try=%d/", attempt))
				require.Contains(t, errorMessages[i], "RESPONSE STATUS CODE ERROR")
				require.Contains(t, errorMessages[i], "test-request-id")
			}
			for i, status := range tc.statuses {
				if status != http.StatusServiceUnavailable || !quiet503 {
					continue
				}
				for _, message := range allMessages {
					require.NotContains(t, message, fmt.Sprintf("(Try=%d/", i+1))
					require.NotContains(t, message, fmt.Sprintf("(Try=%d)", i+1))
				}
			}
		})
	}
}

func TestFailedTransfer503Details(t *testing.T) {
	const requestBody = "transfer payload must remain untouched"
	req, err := http.NewRequest(http.MethodPut, "https://example.invalid/blob?sig=secret-signature", strings.NewReader(requestBody))
	require.NoError(t, err)
	req.Header.Set("Authorization", "secret-authorization")
	req.Header.Set("x-ms-copy-source", "https://source.invalid/blob?sig=source-secret")
	originalURL, originalHeaders := *req.URL, req.Header.Clone()
	response := retryLogResponse(req, http.StatusServiceUnavailable)
	response.Header.Set("x-ms-error-code", "ServerBusy")
	terminalErr := runtime.NewResponseError(response)
	details := formatFailedTransfer503(fmt.Errorf("upload failed: %w", terminalErr))
	require.Contains(t, details, "FINAL TRANSFER FAILURE: REQUEST/RESPONSE")
	require.Contains(t, details, "503 Service Unavailable")
	require.Contains(t, details, "ServerBusy")
	require.Contains(t, details, "test-request-id")
	for _, secret := range []string{"secret-signature", "secret-authorization", "source-secret"} {
		require.NotContains(t, details, secret)
	}
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, retryLogBody, string(body))
	require.NoError(t, response.Body.Close())
	require.Empty(t, formatFailedTransfer503(errors.New("network failure")))
	require.Empty(t, formatFailedTransfer503(&azcore.ResponseError{StatusCode: 500, RawResponse: response}))
	require.Empty(t, formatFailedTransfer503(&azcore.ResponseError{StatusCode: 503}))

	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), transferLogContextKey, true))
	defer cancel()
	part := &failed503TestPart{}
	transfer := &jobPartTransferMgr{
		ctx: ctx, cancel: cancel, jobPartMgr: part,
		jobPartPlanTransfer: &JobPartPlanTransfer{},
		transferInfo: &TransferInfo{
			Source: "https://source.invalid/blob", Destination: "https://example.invalid/blob",
		},
	}
	transfer.SetStatus(common.ETransferStatus.Started())
	transfer.FailActiveS2SCopy("copying range", terminalErr)
	require.Equal(t, common.ETransferStatus.Failed(), transfer.jobPartPlanTransfer.TransferStatus())
	require.EqualValues(t, 503, transfer.ErrorCode())
	wantMessages := 1
	if buildmode.HighPerf() {
		wantMessages++
	}
	require.Len(t, part.messages, wantMessages)
	require.Contains(t, part.messages[0], "COPYFAILED")
	if buildmode.HighPerf() {
		require.Contains(t, part.messages[1], "FINAL TRANSFER FAILURE: REQUEST/RESPONSE")
		require.Contains(t, part.messages[1], "test-request-id")
	}
	transfer.FailActiveS2SCopy("another canceled chunk", terminalErr)
	require.Len(t, part.messages, wantMessages)
	require.Equal(t, http.MethodPut, req.Method)
	require.Equal(t, originalURL, *req.URL)
	require.Equal(t, originalHeaders, req.Header)
	payload, err := io.ReadAll(req.Body)
	require.NoError(t, err)
	require.Equal(t, requestBody, string(payload))
	require.NoError(t, req.Body.Close())
}

type failed503TestPart struct {
	IJobPartMgr
	messages []string
}

func (*failed503TestPart) Plan() *JobPartPlanHeader { return &JobPartPlanHeader{PartNum: 100000} }
func (p *failed503TestPart) Log(level common.LogLevel, message string) {
	p.messages = append(p.messages, message)
	if p.IJobPartMgr != nil {
		p.IJobPartMgr.Log(level, message)
	}
}

func TestTransfer503LogFile(t *testing.T) {
	for _, tc := range []struct {
		name     string
		statuses []int
		transfer bool
	}{
		{"transfer recovers", []int{503, 200}, true},
		{"transfer fails", []int{503, 503}, true},
		{"scanning fails", []int{503, 503}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir, id := t.TempDir(), common.NewJobID()
			suffix := ""
			if !tc.transfer {
				suffix = "-scanning"
			}
			logger := common.NewJobLogger(id, common.LogError, dir, suffix)
			logger.OpenLog()
			var closeLog sync.Once
			t.Cleanup(func() { closeLog.Do(logger.CloseLog) })
			attempts := 0
			options := NewClientOptions(
				policy.RetryOptions{MaxRetries: 1, RetryDelay: time.Nanosecond},
				policy.TelemetryOptions{}, retryLogTransport(func(req *http.Request) (*http.Response, error) {
					require.Less(t, attempts, len(tc.statuses))
					response := retryLogResponse(req, tc.statuses[attempts])
					response.Header.Set("x-ms-error-code", "ServerBusy")
					attempts++
					return response, nil
				}),
				LogOptions{
					RequestLogOptions: RequestLogOptions{SyslogDisabled: true},
					ShouldLog:         logger.ShouldLog,
					Log:               logger.Log,
				}, nil, nil)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.transfer {
				ctx = context.WithValue(ctx, transferLogContextKey, true)
			}
			pipeline := runtime.NewPipeline("", "", runtime.PipelineOptions{}, &options)
			req, err := runtime.NewRequest(ctx, http.MethodGet, "https://example.invalid/blob?sig=secret-signature")
			require.NoError(t, err)
			req.Raw().Header.Set("Authorization", "secret-authorization")
			req.Raw().Header.Set("x-ms-copy-source", "https://source.invalid/blob?sig=source-secret")
			response, err := pipeline.Do(req)
			require.NoError(t, err)
			require.Equal(t, len(tc.statuses), attempts)
			require.NotNil(t, response)
			require.Equal(t, tc.statuses[len(tc.statuses)-1], response.StatusCode)
			failed := response.StatusCode == http.StatusServiceUnavailable
			if tc.transfer && failed {
				part := &failed503TestPart{IJobPartMgr: &jobPartMgr{jobMgr: &jobMgr{logger: logger}}}
				transfer := &jobPartTransferMgr{
					ctx: ctx, cancel: cancel, jobPartMgr: part,
					jobPartPlanTransfer: &JobPartPlanTransfer{},
					transferInfo: &TransferInfo{
						Source: "https://source.invalid/blob", Destination: "https://example.invalid/blob",
					},
				}
				transfer.SetStatus(common.ETransferStatus.Started())
				transfer.FailActiveS2SCopy("copying range", runtime.NewResponseError(response))
				require.Equal(t, common.ETransferStatus.Failed(), transfer.TransferStatusIgnoringCancellation())
				require.EqualValues(t, 503, transfer.ErrorCode())
			}
			require.NoError(t, response.Body.Close())
			closeLog.Do(logger.CloseLog)
			data, err := os.ReadFile(filepath.Join(dir, id.String()+suffix+".log"))
			require.NoError(t, err)
			text := string(data)
			if tc.transfer && failed {
				require.Contains(t, text, "COPYFAILED")
			} else {
				require.NotContains(t, text, "COPYFAILED")
			}
			if buildmode.HighPerf() && tc.transfer {
				require.NotContains(t, text, "REQUEST/RESPONSE (Try=")
				if failed {
					require.Contains(t, text, "FINAL TRANSFER FAILURE: REQUEST/RESPONSE")
					require.Contains(t, text, "GET https://example.invalid/blob?sig=-REDACTED-")
					require.Contains(t, text, "503 Service Unavailable")
					require.Contains(t, text, "ServerBusy")
					require.Contains(t, text, "test-request-id")
				} else {
					require.NotContains(t, text, "FINAL TRANSFER FAILURE")
					require.NotContains(t, text, "503 Service Unavailable")
				}
			} else {
				var expectedErrors int
				for _, status := range tc.statuses {
					if status == http.StatusServiceUnavailable {
						expectedErrors++
					}
				}
				require.Equal(t, expectedErrors, strings.Count(text, "RESPONSE STATUS CODE ERROR"))
				require.NotContains(t, text, "FINAL TRANSFER FAILURE")
			}
			require.NotContains(t, text, "OUTGOING REQUEST")
			for _, secret := range []string{"secret-signature", "secret-authorization", "source-secret"} {
				require.NotContains(t, text, secret)
			}
		})
	}
}

func TestTransient503LoggingFollowedByNetworkError(t *testing.T) {
	var messages []string
	log := LogOptions{
		RequestLogOptions: RequestLogOptions{SyslogDisabled: true},
		ShouldLog:         func(level common.LogLevel) bool { return level <= common.LogError },
		Log:               func(_ common.LogLevel, message string) { messages = append(messages, message) },
	}
	wantErr := errors.New("transport unavailable")
	attempts := 0
	options := NewClientOptions(policy.RetryOptions{MaxRetries: 1, RetryDelay: time.Nanosecond},
		policy.TelemetryOptions{}, retryLogTransport(func(req *http.Request) (*http.Response, error) {
			attempts++
			if attempts == 1 {
				return retryLogResponse(req, http.StatusServiceUnavailable), nil
			}
			return nil, wantErr
		}), log, nil, nil)
	pipeline := runtime.NewPipeline("", "", runtime.PipelineOptions{}, &options)
	ctx := context.WithValue(context.Background(), transferLogContextKey, true)
	req, err := runtime.NewRequest(ctx, http.MethodGet, "https://example.invalid/blob")
	require.NoError(t, err)
	_, err = pipeline.Do(req)
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, 2, attempts)
	if buildmode.HighPerf() {
		require.Len(t, messages, 1)
	} else {
		require.Len(t, messages, 2)
		require.Contains(t, messages[0], "(Try=1/")
	}
	require.Contains(t, messages[len(messages)-1], "(Try=2/")
	require.Contains(t, messages[len(messages)-1], "transport unavailable")
}

func TestTransient503LoggingConcurrentRequests(t *testing.T) {
	var mu sync.Mutex
	attempts := make(map[string]int)
	var messages []string
	log := LogOptions{
		RequestLogOptions: RequestLogOptions{SyslogDisabled: true},
		ShouldLog:         func(level common.LogLevel) bool { return level <= common.LogError },
		Log: func(_ common.LogLevel, message string) {
			mu.Lock()
			defer mu.Unlock()
			messages = append(messages, message)
		},
	}
	options := NewClientOptions(policy.RetryOptions{MaxRetries: 5, RetryDelay: time.Nanosecond},
		policy.TelemetryOptions{}, retryLogTransport(func(req *http.Request) (*http.Response, error) {
			mu.Lock()
			defer mu.Unlock()
			attempts[req.URL.Path]++
			status := http.StatusServiceUnavailable
			if attempts[req.URL.Path] == 6 {
				status = http.StatusOK
			}
			return retryLogResponse(req, status), nil
		}), log, nil, nil)
	pipeline := runtime.NewPipeline("", "", runtime.PipelineOptions{}, &options)
	for i := 0; i < 16; i++ {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			if i%2 == 0 {
				ctx = context.WithValue(ctx, transferLogContextKey, true)
			}
			req, err := runtime.NewRequest(ctx, http.MethodGet, fmt.Sprintf("https://example.invalid/blob%d", i))
			require.NoError(t, err)
			response, err := pipeline.Do(req)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, response.StatusCode)
			require.NoError(t, response.Body.Close())
		})
	}
	t.Cleanup(func() {
		wantMessages := 16 * 5
		if buildmode.HighPerf() {
			wantMessages = 8 * 5
			for _, message := range messages {
				for i := 0; i < 16; i += 2 {
					require.NotContains(t, message, fmt.Sprintf("https://example.invalid/blob%d\n", i))
				}
			}
		}
		require.Len(t, messages, wantMessages)
		require.Len(t, attempts, 16)
		for _, count := range attempts {
			require.Equal(t, 6, count)
		}
	})
}
