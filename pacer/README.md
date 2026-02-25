# pacer

The `pacer` package aims to solve two problems...

1) How do we prevent timing out requests when we have high routines and low bandwidth
2) What if the bandwidth is intentionally, artificially low?
3) What if it's a mix? The user has requested a target bandwidth, but they cannot hit it.

The answer is simple at face value, but pretty complex in reality.

## Bandwidth detection

This is relevant for #1, and #3. Until we _know_ what our bandwidth looks like, we should fire as many requests as our routines will allow, and try to pick up the bandwidth by recording the throughput and averaging it over the last block of time (i.e. average 1s chunks over 30s), then scale down in [Bandwidth Allocation](#bandwidth-allocation).

To do this, our pacer must implement a throughput recorder which will report the average (or target throughput). This is done by `interface.go`'s `ThroughputRecorder` interface:

```go
type ThroughputRecorder interface {
	// RecordBytes records the number of bytes that have made it through the pipe.
	// We don't care if the request ends in success or failure,
	// because we're trying to observe the available bandwidth.
	RecordBytes(count uint64)

	// SetObservationPeriod sets the observation period that RecordBytes averages over.
	SetObservationPeriod(seconds uint)
	// ObservationPeriod gets the observation period that RecordBytes averages over.
	ObservationPeriod() (seconds uint)

	// Bandwidth returns the observed (or requested) bandwidth.
	Bandwidth() (bytesPerSecond uint64)
	// HardLimit indicates that the user has requested a hard limit.
	HardLimit() (requested bool, bytesPerSecond uint64)
	
	// RequestHardLimit indicates that the user is requesting a hard limit.
	RequestHardLimit(bytesPerSecond uint64)
	// ClearHardLimit indicates that the user has cancelled the hard limit request.
	ClearHardLimit()
}
```

## Bandwidth allocation

Consider the following few scenarios under the above situations.

1) We are processing abnormally large requests, i.e. chunks of several gigabytes
2) We are processing abnormally small requests, i.e. tiny chunks of a handful of kilobytes.
3) We are processing average sized requests, i.e. a handful of megabytes

In case 1, we can probably just allocate everyone a standard division of the hard cap, or as much as they like if we're in detection-only mode. For flighting requests, we should check if allocating a new request would drop everyone below Azure's minimum throughput (with some healthy overage), henceforth `azureMinimumSpeed` based upon the hard cap, or the observed cap (if it is lower).

In case 2, these requests may complete in one iteration, and won't consume the entire `azureMinimumSpeed` on their own. We should watch out for this, because if we don't, we may wind up with much lower real throughput than we intend.

In case 3, it's something in between 1 and 2. It could bounce between. Ergo, we have to handle them like case 2.

To achieve this, keeping our observed throughput and hard-cap in mind, we should, for requests larger than `azureMinimumSpeed`, allocate them as consuming `azureMinimumSpeed`, and for requests smaller, allocate them as consuming their real size.

If the new request's allocated throughput would go over either our hard cap or our observed throughput, we typically shouldn't allocate it.

### On the subject of S2S requests...

These are a headache. We technically _have_ to support them, because previous pacer implementations did, but with a huge bug due to the caveat of trying to pace them. It's, fundamentally, not our bandwidth. We can't control the speed at which an S2S request moves, just the fact that it, in it's entirety, moves at all.

This means the most granularity we get, is the full chunk. We have to allocate them as the full chunk, and subtract the desired throughput from the chunk size until it is satisfied. This in effect, averages out to the desired speed, but not really.

Unfortunately, these have to be handled fundamentally differently than upload or download requests.

## Pauses in transfers and other delays that may present sub-optimal bandwidth observations...

1) Many small files are known to cause issues with bandwidth, as they absorb mostly IOPS, _not_ actual bandwidth.
2) Certain methods of enumeration can be increasingly slow, causing delays between blocks of 10k files. This leads to pauses in any real throughput, and consequently, sends observed bandwidth to 0.
3) What if the user's internet disconnects temporarily or something?
4) What if we get a really large string of retries? That'll send bandwidth to near 0.

Unfortunately, situations like this are where our strategy starts to fall apart a little bit more. We _could_ keep track of average request timing, or we could just fall back to 1MB/s throughput as a theoretical "minimum" to prevent getting totally locked out.

