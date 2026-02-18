package pacer

import (
	"math"
)

func (i *impl) tick() {
	/*
		1) Check hard limit and bandwidth
		2) Allocate new requests
		3) Feed all existing requests
	*/

	limitRequested, hardLimit := i.HardLimit()
	observedBps, _ := i.Bandwidth()
	observedBps = max(observedBps, PretendBps)

	currentlyAllocated := len(i.live) * AzureMinimumBandwidth
	targetBandwidth := int(float64(observedBps) * BandwidthSaturationTarget)
	if limitRequested && observedBps > hardLimit {
		targetBandwidth = int(float64(hardLimit) * BandwidthSaturationTarget)
	}

	for currentlyAllocated < targetBandwidth {
		var newRequest requestQueueEntry
		queueEmpty := false
		select {
		case newRequest = <-i.queue:
		case newRequest = <-i.reliveQueue:
		default:
			queueEmpty = true
		}

		if !queueEmpty {
			newRequest.readyCh <- nil
			currentlyAllocated += AzureMinimumBandwidth

			i.live[newRequest.req.ID()] = newRequest.req
		} else {
			break
		}
	}

	if !limitRequested || len(i.live) == 0 {
		// if there's no limit requested, everyone gets everything they want-- the requests will check and see that a hard limit isn't enforced.
		// if there's nobody to give bandwidth to, this escapes a div by 0.
		return
	}

	// If there's a limit requested, we need to distribute it. In phase 1 of distribution, we'll see if any requests are lower than our "average".
	// We don't bother looping this, because our average is only likely to be lower if we did any distributions during this phase.
	averageAllocationSize := hardLimit / int64(len(i.live))
	for k, v := range i.live {
		if remNeeded := v.RemainingAllocations(); int64(remNeeded) <= averageAllocationSize {
			//fmt.Println(v.ID(), "receiving", remNeeded, "bytes (early clear)")

			v.issueBytes(remNeeded)
			hardLimit -= int64(remNeeded)
			delete(i.live, k)
		}
	}

	// recalculate our average, then redistribute it to all live requests
	averageAllocationSize = hardLimit / int64(len(i.live))
	for k, v := range i.live {
		remainingBytes := v.issueBytes(int(min(averageAllocationSize, math.MaxInt)))
		//fmt.Println(v.ID(), "receiving", averageAllocationSize, "bytes (regular distribution", remainingBytes, "remain)")

		if remainingBytes == 0 {
			delete(i.live, k)
		}
	}
}
