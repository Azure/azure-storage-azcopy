package ste

import (
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"time"
)

/*
In some cases, an extremely high number of threads with an extremely low throughput
can cause requests to remain live, but idle for an extended period of time.

This is sometimes long enough for the service, or AzCopy to simply outright drop the request.

RequestLifetimeTracker tracks requests over (count * lifetime), and keeps a running average.
*/

const (
	requestBucketCount    = 6 * 10 // 10 minutes of buckets
	requestBucketLifetime = time.Second * 10

	requestActionInitiate uint = iota - 2
	requestActionCancel
	requestActionFinalize
)

var _requestLifetimeTrackerSingleton *RequestLifetimeTracker

// GetRequestLifetimeTracker returns the singleton *RequestLifetimeTracker which stems off into RequestLifetimeTrackerPolicy.

func GetRequestLifetimeTracker() (tracker *RequestLifetimeTracker) {
	if _requestLifetimeTrackerSingleton == nil {

		_requestLifetimeTrackerSingleton = &RequestLifetimeTracker{
			requestActionQueue:         make(chan requestAction, 600), // 2x our max number of threads, that way the queue should never block up.
			liveRequestInitiationTimes: make(map[string]time.Time),
			requestLifetimeBuckets:     &requestLifetimeBucket{},
			bucketSwapTicker:           time.NewTicker(requestBucketLifetime),
		}

		go _requestLifetimeTrackerSingleton.queueWorker()
	}

	return _requestLifetimeTrackerSingleton
}

type requestLifetimeBucket = common.LinkedList[*common.LinkedList[time.Duration]]
type requestAction struct {
	ID     string
	Action uint

	Init     time.Time
	Duration time.Duration
}

type RequestLifetimeTracker struct {
	atomicSimultaneousLiveRequestCount int64

	// Keeping track of a total average, for debug purposes.
	atomicTotalRequestLifetime        int64
	atomicCompletedRequestCount       int64
	atomicTotalAverageRequestLifetime int64

	// This is the useful bit. Keep track of in-flight requests and recent historical data.
	requestActionQueue         chan requestAction
	liveRequestInitiationTimes map[string]time.Time
	requestLifetimeBuckets     *requestLifetimeBucket // Divided into n buckets of duration
	bucketSwapTicker           *time.Ticker

	// more atomic values for read-only
	atomicRunningRequestAverageLifetime int64
	atomicRunningRequestTotalLifetime   int64
	atomicRunningRequestCount           int64
}

// workers sit in the background and manage the running averages and buckets, ticking with the bucket ticker.

func (r *RequestLifetimeTracker) queueWorker() {
	for {
		select { // This thread should die with the program to prevent any hangups
		case <-r.bucketSwapTicker.C:
			// todo
			panic("swap")

			// Add the latest bucket on
			// Remove the oldest bucket
			// Average and find 99th percentile
		case act := <-r.requestActionQueue:
			switch act.Action {
			case requestActionInitiate:

			case requestActionFinalize:
				fallthrough
			case requestActionCancel:

				break
			default:
				panic(fmt.Sprintf("unrecognized action %d", act.Action))
			}
		}
	}
}

func (r *RequestLifetimeTracker) InitiateRequest(clientRequestID string) {
	r.requestActionQueue <- requestAction{
		ID:     clientRequestID,
		Action: requestActionInitiate,
	}
}

func (r *RequestLifetimeTracker) CancelRequest(clientRequestID string) {
	r.requestActionQueue <- requestAction{
		ID:     clientRequestID,
		Action: requestActionCancel,
	}
}

func (r *RequestLifetimeTracker) FinishRequest(clientRequestID string, duration time.Duration) {
	r.requestActionQueue <- requestAction{
		ID:       clientRequestID,
		Action:   requestActionFinalize,
		Duration: duration,
	}
}

func (r *RequestLifetimeTracker) GetPolicy() policy.Policy {
	panic("todo policy")
}
