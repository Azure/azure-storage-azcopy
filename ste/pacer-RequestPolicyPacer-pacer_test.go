package ste

import (
	"bytes"
	"context"
	"errors"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	azruntime "github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func NewTestPolicyPacer(target uint64, manualTick bool) (pacer RequestPolicyPacer, tickFunc func()) {
	var ticker *time.Ticker
	if manualTick {
		t := make(chan time.Time, 1)
		ticker = &time.Ticker{
			C: t,
		}

		tickFunc = func() {
			t <- time.Now()
		}
	} else {
		ticker = time.NewTicker(PacerTickrate)
	}

	pacer = &requestPolicyPacer{
		processedBytes:          &atomic.Uint64{},
		maxBytesPerSecond:       &atomic.Uint64{},
		allocatedBytesPerSecond: &atomic.Uint64{},
		bytesPerTick:            &atomic.Uint64{},
		maxAvailableBytes:       &atomic.Uint64{},
		availableBytes:          &atomic.Uint64{},
		requestInitChannel:      make(chan *policyPacerBody, 300),
		respInitChannel:         make(chan *policyPacerBody, 300),
		pacerExitChannel:        make(chan *policyPacerBody, 300),
		allocationRequestchannel: make(chan struct {
			pacerID uuid.UUID
			size    uint64
		}, 300),
		shutdownCh:         make(chan bool, 300),
		liveBodies:         make(map[uuid.UUID]*policyPacerBody),
		allocationRequests: make(map[uuid.UUID]uint64),
		ticker:             ticker,
		manualTick:         manualTick,
	}

	pacer.UpdateTargetBytesPerSecond(target)
	go pacer.(*requestPolicyPacer).pacerBody()

	return
}

func NewRandomBytes(size uint64) []byte {
	buf := make([]byte, size)

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.Read(buf)

	return buf
}

func NewRandomBody(size uint64) io.ReadSeekCloser {
	return streaming.NopCloser(bytes.NewReader(NewRandomBytes(size)))
}

// Validate the natural lifecycle of a request and response.
func TestPolicyPacer_ReqRespSync(t *testing.T) {
	const (
		BodySize          = 1024 * 100
		PacerBytesPerTick = 1024
		// Work back to a rate that will give us the number of bytes we expect
		PacerRate = PacerBytesPerTick * uint64(time.Second/PacerTickrate)
	)

	// Stupid simple cheat function to validate we're actually reading bytes in pace
	validateRead := func(count uint64, body *policyPacerBody) {
		read := false
		var readBytes uint64
		for tries := 0; tries < 100; tries++ {
			readBytes = body.readHead.Load()
			assert.LessOrEqual(t, readBytes, count)
			if readBytes >= count {
				read = true
				break
			}

			// Give the reader a chance to finish, we're not sure where it really lives
			time.Sleep(time.Millisecond)
		}
		assert.True(t, read, "Failed to read %d bytes over 100ms of retries, successfully read %d", count, readBytes)
	}

	// Create a manually ticked policy pacer
	p, ticker := NewTestPolicyPacer(PacerRate, true)

	// Get random bodies
	reqBody := NewRandomBody(BodySize)
	var respBody io.Reader = NewRandomBody(BodySize)

	// Initialize our request body
	var err error
	reqBody, err = p.GetPacedRequestBody(streaming.NopCloser(reqBody))
	rawReqBody := reqBody.(*policyPacerRequestBody)
	assert.NoError(t, err, "Get Paced Request Body")

	// fire up our background reader
	go func() {
		buf := make([]byte, BodySize)
		n, err := reqBody.Read(buf)
		assert.NoError(t, err, "Read body")
		assert.Equal(t, n, BodySize, "Read expected amount from request")
	}()

	// Tick the pacer until completion, validating that each step is what we expect.
	for i := uint64(PacerBytesPerTick); i <= BodySize; i += PacerBytesPerTick {
		time.Sleep(time.Millisecond * 10) // Wait just a little for things to settle
		ticker()
		target := min(i, BodySize)
		validateRead(target, rawReqBody.policyPacerBody)
	}

	// Close the body so we tell the pacer to drop this body
	err = reqBody.Close()
	assert.NoError(t, err, "Close request body")

	// Give it a little bit for everything to settle
	time.Sleep(time.Millisecond)
	assert.Equal(t, int(BodySize), int(p.GetTotalTraffic()))

	// Fire up the new body
	respBody, err = p.GetPacedResponseBody(io.NopCloser(respBody), BodySize)
	rawRespBody := respBody.(*policyPacerBody)
	assert.NoError(t, err, "Get Paced Response Body")

	// fire up our background reader
	go func() {
		buf := make([]byte, BodySize)
		n, err := respBody.Read(buf)
		assert.NoError(t, err, "Read body")
		assert.Equal(t, n, BodySize, "Read expected amount from response")
	}()

	for i := uint64(PacerBytesPerTick); i <= BodySize; i += PacerBytesPerTick {
		ticker()
		target := min(i, BodySize)
		validateRead(target, rawRespBody)
	}

	err = rawRespBody.Close()
	assert.NoError(t, err, "Close resp body")

	assert.Equal(t, BodySize*2, int(p.GetTotalTraffic()))
}

type NullWriter struct{}

func (n2 NullWriter) Write(p []byte) (n int, err error) {
	_ = p // discard the buffer
	return len(p), nil
}

// Validate that multiple bodies receive bandwidth in parallel
func TestPolicyPacer_Multibody(t *testing.T) {
	const (
		BodyCount = 100
		BodySize  = common.MegaByte * 10
		PacerMbps = common.MegaByte * 10

		// We don't want this test to take 100 seconds,
		// so we send ticks much faster than normal to effectively speed through this test.
		// Normally, the tickrate is 10x per second, by dividing it by 100, we get 100x a second.
		// This test should run in about 1s on a decent processor.
		PacerTickrateDivisor = 100
	)

	// === Generate BodyCount separate BodySize bodies ===
	bodies := make([]io.ReadSeekCloser, BodyCount)
	wg := &sync.WaitGroup{}
	wg.Add(100)

	for bodyId := range bodies {
		go func() {
			bodies[bodyId] = NewRandomBody(BodySize)
			wg.Done()
		}()
	}

	wg.Wait()

	// === Initialize the pacer ===
	p, tickerFunc := NewTestPolicyPacer(PacerMbps, true)
	tickerRelease := make(chan bool)
	go func() {
		internalTickrate := PacerTickrate / PacerTickrateDivisor
		ticker := time.NewTicker(internalTickrate)
		for {
			select {
			case <-tickerRelease:
				ticker.Stop()
				close(tickerRelease)
				return
			case <-ticker.C:
				tickerFunc()
			}
		}
	}()

	// === Initialize the bodies and their readers ===
	wg.Add(100)
	for bodyId := range bodies {
		go func() {
			defer wg.Done()

			body, err := p.GetPacedRequestBody(bodies[bodyId])
			assert.NoError(t, err, "Get paced body request for body %d", bodyId)

			bytesRead, err := io.Copy(&NullWriter{}, body)
			assert.NoError(t, err, "Read body %d, successfully managed %d bytes", bodyId, bytesRead)
			assert.Equal(t, bytesRead, int64(BodySize), "Read whole body for body %d", bodyId)

			err = body.Close()
			assert.NoError(t, err)
		}()
	}

	wg.Wait()

	// === Wait a little bit, then check that we pushed as many bytes as we intended. ===
	time.Sleep(time.Millisecond * 20)
	assert.Equal(t, BodySize*BodyCount, int(p.GetTotalTraffic()))

	// Release the ticker
	tickerRelease <- true
}

// Validate that the pipeline policy works as expected
func TestPolicyPacer_Policy(t *testing.T) {
	const (
		BodySize  = common.MegaByte * 100
		PacerMbps = common.MegaByte * 10

		// We don't want this test to take 10 seconds,
		// so we send ticks much faster than normal to effectively speed through this test.
		// Normally, the tickrate is 10x per second, by dividing it by 10, we get 100x a second.
		// This test should run in about 1s on a decent processor.
		PacerTickrateDivisor = 10
	)

	// === Set up the pacer ===
	p, tickerFunc := NewTestPolicyPacer(PacerMbps, true)
	cleanupTicker := make(chan bool, 1)
	go func() {
		t := time.NewTicker(PacerTickrate / PacerTickrateDivisor)
		for {
			select {
			case <-cleanupTicker:
				t.Stop()
				close(cleanupTicker)
				return
			case <-t.C:
				tickerFunc()
			}
		}
	}()

	// === Set up a dummy client ===
	mockResp := &MockResponder{
		resp: http.Response{
			Status:        http.StatusText(http.StatusOK),
			StatusCode:    200,
			Header:        nil,
			ContentLength: BodySize,
			Close:         false,
			Uncompressed:  true,
		},
		body: NewRandomBytes(BodySize),
	}

	blobClient, err := blockblob.NewClientWithNoCredential(
		"https://asdf.blob.core.windows.net/notreal/blob",
		&blockblob.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				PerCallPolicies: []policy.Policy{
					azruntime.NewRequestIDPolicy(),
					NewVersionPolicy(),
					newFileUploadRangeFromURLFixPolicy(),
				},
				PerRetryPolicies: []policy.Policy{
					p.GetPolicy(),
					mockResp,
				},
			},
		},
	)
	assert.NoError(t, err, "Create blob Client")

	dsr, err := blobClient.DownloadStream(context.TODO(), nil)
	assert.NoError(t, err, "DownloadStream")

	_, ok := dsr.Body.(*policyPacerBody)
	assert.True(t, ok, "Body should be policyPacerBody")

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		n, err := io.Copy(&NullWriter{}, dsr.Body)
		assert.NoError(t, err, "Read body")
		assert.Equal(t, int(n), int(p.GetTotalTraffic()), "Expected n and total traffic to match")
		assert.Equal(t, int(BodySize), int(p.GetTotalTraffic()), "Expected total traffic to match body size")

		err = dsr.Body.Close()
		assert.NoError(t, err, "Close body")
		wg.Done()
	}()

	wg.Wait()
	cleanupTicker <- true
}

type nilReader struct {
	closed bool
	n, len int64
}

func (r *nilReader) Seek(offset int64, whence int) (int64, error) {
	if r.closed {
		return 0, io.ErrClosedPipe
	}

	switch whence {
	case io.SeekStart:
		r.n = 0 + offset
	case io.SeekCurrent:
		r.n += offset
	case io.SeekEnd:
		r.n = r.len + offset
	default:
		return 0, errors.New("invalid relativity")
	}

	return r.n, nil
}

func (r *nilReader) Close() error {
	r.closed = true
	return nil
}

func (r *nilReader) Read(p []byte) (n int, err error) {
	if r.closed {
		return 0, io.ErrClosedPipe
	}

	count := int64(len(p))
	if r.n+count > r.len {
		count = r.len - r.n
		err = io.EOF
	}

	r.n += count
	return int(count), err
}

// During testing, it was possible to trigger a panic by ticking the pacer with nothing going on.
// Attempt to reproduce that.
func TestPolicyPacer_FullIdle(t *testing.T) {
	if os.Getenv("PACER_FULLIDLE_TEST") == "" {
		t.SkipNow() // We don't want to run this test in CI.
	}

	const (
		PacerMbps    = common.MegaByte * 10
		TestDuration = time.Second * 60
	)

	p, _ := NewTestPolicyPacer(PacerMbps, false)

	body, _ := p.GetPacedRequestBody(&nilReader{len: common.MegaByte * 100})

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(&NullWriter{}, body)
	}()

	time.Sleep(TestDuration)
	wg.Wait()
}

// Validate that idle bodies don't bog down resources on live bodies.
// We want to make sure of this, because if somebody forgets to close a body, we don't want it hogging
// resources until it naturally exits.
func TestPolicyPacer_IdleBody(t *testing.T) {
	const (
		BodyCount     = 5
		BodySize      = common.MegaByte * 5
		FirstBodySize = BodySize * 5
		PacerMbps     = common.MegaByte
	)

	p, ticker := NewTestPolicyPacer(PacerMbps, true)

	// spin up 5 bodies
	bodies := make([]*policyPacerRequestBody, BodyCount)
	for k := range bodies {
		rootBody := NewRandomBody(common.Iff[uint64](k == 0, FirstBodySize, BodySize))
		b, err := p.GetPacedRequestBody(rootBody)
		assert.NoError(t, err)

		bodies[k] = b.(*policyPacerRequestBody)
	}

	// Start the first body reading
	go func() {
		b := bodies[0]
		w, err := io.Copy(&NullWriter{}, b)
		assert.NoError(t, err)
		assert.Equal(t, w, FirstBodySize)
	}()

	ticker() // Tick once

}
