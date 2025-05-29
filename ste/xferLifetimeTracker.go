package ste

import (
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/google/uuid"
	"net/http"
	"sync/atomic"
	"time"
)

/*
In some cases, an extremely high number of threads with an extremely low throughput
can cause requests to remain live, but idle for an extended period of time.

This is sometimes long enough for the service, or AzCopy to simply outright drop the request.

RequestLifetimeTracker tracks requests over (count * lifetime), and keeps a running average.

Below a certain length, say, a couple seconds, this isn't particularly useful.
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
		newAtomicInt := func() *int64 {
			var n int64
			return &n
		}

		_requestLifetimeTrackerSingleton = &RequestLifetimeTracker{
			requestActionQueue:         make(chan requestAction, 600), // 2x our max number of threads, that way the queue should never block up.
			liveRequestInitiationTimes: make(map[uuid.UUID]time.Time),
			requestLifetimeBuckets:     &requestLifetimeBucket{},
			bucketSwapTicker:           time.NewTicker(requestBucketLifetime),

			atomicSimultaneousLiveRequestCount: newAtomicInt(),
			atomicUnexpectedRequestLifetime:    newAtomicInt(),

			atomicRequestLifetimeSum:     newAtomicInt(),
			atomicRequestCompletionCount: newAtomicInt(),
			atomicRequestMaxLifetime:     newAtomicInt(),
			atomicRequestAvgLifetime:     newAtomicInt(),

			atomicRunningRequestAverageLifetime: newAtomicInt(),
			atomicRunningRequestCount:           newAtomicInt(),
			atomicRunningRequestMaxLifetime:     newAtomicInt(),
		}

		_requestLifetimeTrackerSingleton.requestLifetimeBuckets.Insert(&common.LinkedList[time.Duration]{})

		go _requestLifetimeTrackerSingleton.queueWorker()
	}

	return _requestLifetimeTrackerSingleton
}

type requestLifetimeBucket = common.LinkedList[*common.LinkedList[time.Duration]]
type requestAction struct {
	ID     uuid.UUID
	Action uint

	SubmitTime time.Time
}

type RequestLifetimeTracker struct {
	atomicSimultaneousLiveRequestCount *int64 // how many requests are currently live
	atomicUnexpectedRequestLifetime    *int64 // Are our longest-living requests alive too long?

	// Keeping track of a total average, for debug purposes.
	atomicRequestLifetimeSum     *int64 // the sum of all requests' lifetime
	atomicRequestMaxLifetime     *int64 // The highest lifetime we've *ever* seen.
	atomicRequestCompletionCount *int64 // how many requests have been completed
	atomicRequestAvgLifetime     *int64 // the average of all requests' lifetime

	// This is the useful bit. Keep track of in-flight requests and recent historical data.
	requestActionQueue         chan requestAction
	liveRequestInitiationTimes map[uuid.UUID]time.Time
	requestLifetimeBuckets     *requestLifetimeBucket // Divided into n buckets of duration
	bucketSwapTicker           *time.Ticker

	// more atomic values for read-only
	atomicRunningRequestAverageLifetime *int64
	atomicRunningRequestMaxLifetime     *int64
	atomicRunningRequestCount           *int64
}

// workers sit in the background and manage the running averages and buckets, ticking with the bucket ticker.

func (r *RequestLifetimeTracker) queueWorker() {
	for {
		select { // This thread should die with the program to prevent any hangups
		case <-r.bucketSwapTicker.C:
			var rCount int64
			var rSum, rAvg, rMax time.Duration

			// Average and find peak
			for bucketEnum := r.requestLifetimeBuckets.Enum(); bucketEnum.HasData(); bucketEnum.Next() {
				bucket := bucketEnum.Data()
				rCount += bucket.Len()

				for durationEnum := bucket.Enum(); durationEnum.HasData(); durationEnum.Next() {
					duration := durationEnum.Data()

					rSum += duration
					if rMax < duration {
						rMax = duration
					}
				}
			}
			rAvg = rSum / time.Duration(rCount)

			atomic.StoreInt64(r.atomicRunningRequestAverageLifetime, int64(rAvg))
			atomic.StoreInt64(r.atomicRunningRequestCount, rCount)
			atomic.StoreInt64(r.atomicRunningRequestMaxLifetime, int64(rMax))

			// Add the latest bucket on
			r.requestLifetimeBuckets.Insert(&common.LinkedList[time.Duration]{})

			// Remove the oldest bucket
			r.requestLifetimeBuckets.PopRear()

			// Log the current statistic
			if common.AzcopyCurrentJobLogger != nil {
				common.AzcopyCurrentJobLogger.Log(common.ELogLevel.Debug(), fmt.Sprintf(
					"Request lifetime statistics: Job lifetime: (avg: %v max: %v requests: %v) Recent window: (Avg: %v Max: %v Requests: %v"))
			}

			// Indicate that we're seeing routine exhaustion
			if rAvg < time.Second*40 { // For requests only say, a few seconds long, this isn't going to be a useful measure.
				rAvg = time.Second * 40 // At higher avg lifetimes though, we do start being interested.
			}
			maxDiff := float64(rMax) / float64(rAvg) // If the max request lifetime significantly exceeds the average,
			if maxDiff > 3 {                         // We should probably start to slow down a little.
				atomic.StoreInt64(r.atomicUnexpectedRequestLifetime, 1)
			} else {
				atomic.StoreInt64(r.atomicUnexpectedRequestLifetime, 0)
			}
		case act := <-r.requestActionQueue:
			switch act.Action {
			case requestActionInitiate:
				r.liveRequestInitiationTimes[act.ID] = act.SubmitTime
			case requestActionFinalize:
				startTime := r.liveRequestInitiationTimes[act.ID]
				duration := act.SubmitTime.Sub(startTime)

				sum := atomic.AddInt64(r.atomicRequestLifetimeSum, int64(duration))
				count := atomic.AddInt64(r.atomicRequestCompletionCount, 1)
				atomic.StoreInt64(r.atomicRequestAvgLifetime, sum/count) // we're the only writer here so this is OK
				atomic.AddInt64(r.atomicSimultaneousLiveRequestCount, -1)

				r.requestLifetimeBuckets.Front().Insert(duration)

				fallthrough
			case requestActionCancel:
				delete(r.liveRequestInitiationTimes, act.ID)
				break
			default:
				panic(fmt.Sprintf("unrecognized action %d", act.Action))
			}
		}
	}
}

func (r *RequestLifetimeTracker) InitiateRequest(clientRequestID uuid.UUID) {
	r.requestActionQueue <- requestAction{
		ID:         clientRequestID,
		Action:     requestActionInitiate,
		SubmitTime: time.Now(),
	}
}

func (r *RequestLifetimeTracker) FinishRequest(clientRequestID uuid.UUID) {
	r.requestActionQueue <- requestAction{
		ID:         clientRequestID,
		Action:     requestActionFinalize,
		SubmitTime: time.Now(),
	}
}

func (r *RequestLifetimeTracker) SimultaneousLiveRequests() int64 {
	return atomic.LoadInt64(r.atomicSimultaneousLiveRequestCount)
}

func (r *RequestLifetimeTracker) LifetimeTotalRequests() int64 {
	return atomic.LoadInt64(r.atomicRequestCompletionCount)
}

func (r *RequestLifetimeTracker) LifetimeAverageRequestLifetime() time.Duration {
	return time.Duration(atomic.LoadInt64(r.atomicRequestAvgLifetime))
}

func (r *RequestLifetimeTracker) WindowTotalRequests() int64 {
	return atomic.LoadInt64(r.atomicRunningRequestCount)
}

func (r *RequestLifetimeTracker) WindowAverageRequestLifetime() time.Duration {
	return time.Duration(atomic.LoadInt64(r.atomicRunningRequestAverageLifetime))
}

func (r *RequestLifetimeTracker) WindowMaxRequestLifetime() time.Duration {
	return time.Duration(atomic.LoadInt64(r.atomicRunningRequestMaxLifetime))
}

func (r *RequestLifetimeTracker) UnexpectedMaxRequestLifetime() bool {
	return atomic.LoadInt64(r.atomicUnexpectedRequestLifetime) == 1
}

func (r *RequestLifetimeTracker) GetPolicy() policy.Policy {
	return &RequestLifetimeTrackerPolicy{
		parent: r,
	}
}

type RequestLifetimeTrackerPolicy struct {
	parent *RequestLifetimeTracker
}

func (r *RequestLifetimeTrackerPolicy) Do(req *policy.Request) (*http.Response, error) {
	reqId := uuid.New()

	r.parent.InitiateRequest(reqId)
	resp, err := req.Next()
	r.parent.FinishRequest(reqId)

	return resp, err
}
