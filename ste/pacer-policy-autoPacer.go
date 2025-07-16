package ste

import (
	"context"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"net/http"
	"sync/atomic"
	"time"
)

var PolicyAutoPacerKey = func() any {
	type PolicyAutoPacerKey struct{}

	return &PolicyAutoPacerKey{}
}()

const ( // values copied from old autoPacer code
	tuningIntervalDuration = time.Second

	deadBandDuration = 20 * time.Second // TODO: review this rather generous value.  Might not be needed if we can pace the internal retry efforts inside the retryPolices, because we (presumably) won't get such big flurries of 503s if we do that

	decreaseFactor = 0.65

	// These increase factors were prototyped in a Excel sheet, then tweaked after live testing.
	// In "steady state" we would expect one decrease every few minutes, with most of the time spent close to the optimal rate.
	// (And yes, the shape of the resulting speed curve is at least superficially similar to that used by modern TCP variants,
	// for the same reason, which is that we want slow change when near the last-known "best" level.)
	fastRecoveryFactor = 0.1
	probingFactor      = 0.015
	stableZoneFactor   = probingFactor / 10
	stableZoneStart    = 0.95
	stableZoneEnd      = 1.05

	pageBlobThroughputTunerString = "Page blob throughput tuner"

	maxPacerGbps           = 100
	maxPacerBytesPerSecond = maxPacerGbps * 1000 * 1000 * 1000 / 8
)

// RequestPolicyAutoPacer adapts the functionality of the old autoPacer onto
// the new RequestPolicyPacer architecture.
type RequestPolicyAutoPacer struct {
	RequestPolicyPacer

	cancelFunc             context.CancelFunc
	retriesInterval        common.AtomicNumeric[int32]
	lastPeakBytesPerSecond float32
	lastPeakTime           time.Time

	logger    common.ILogger
	logPrefix string
}

func NewRequestPolicyAutoPacer(targetBytesPerSecond uint64) RequestPolicyPacer {
	pacer := NewRequestPolicyPacer(targetBytesPerSecond)

	return &RequestPolicyAutoPacer{
		RequestPolicyPacer: pacer,

		cancelFunc:      func() {},
		retriesInterval: &atomic.Int32{},
	}
}

func (r *RequestPolicyAutoPacer) GetPolicy() policy.Policy {
	corePolicy := r.RequestPolicyPacer.GetPolicy()

	return &autoPacerPolicy{
		p:      corePolicy,
		parent: r,
	}
}

func (r *RequestPolicyAutoPacer) Cleanup() {
	r.cancelFunc()
	r.RequestPolicyPacer.Cleanup()
}

type autoPacerPolicy struct {
	p      policy.Policy
	parent *RequestPolicyAutoPacer
}

func (p *autoPacerPolicy) Do(req *policy.Request) (*http.Response, error) {
	resp, err := p.p.Do(req)

	if resp != nil && resp.StatusCode == http.StatusServiceUnavailable {
		p.parent.retriesInterval.Add(1)
	}

	return resp, err
}
