package pacer

import (
	"fmt"
	"math"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func (i *impl) tick() {
	/*
		1) Check hard limit and bandwidth
		2) Allocate new requests
		3) Feed all existing requests
	*/

	limitRequested, hardLimit := i.HardLimit()
	observedBps, _ := i.Bandwidth()
	observedBps = max(observedBps, int64(pretendBps))

	currentlyAllocated := len(i.live) * azureMinimumBandwidth
	//targetBandwidth := int(float64(observedBps) * bandwidthSaturationTarget)
	//if limitRequested && observedBps > hardLimit {
	//	targetBandwidth = int(float64(hardLimit) * bandwidthSaturationTarget)
	//}

	for len(i.live) < 2 {
		var newRequest requestQueueEntry
		queueEmpty := false
		select {
		case newRequest = <-i.reliveQueue: // reanimated requests receive higher priority, as they may still be on the wire. this helps avoid starvation.
		case newRequest = <-i.queue:
		default:
			queueEmpty = true
		}

		if !queueEmpty {
			go func() {
				newRequest.readyCh <- nil
			}()

			// there's no point in accepting a request needing nothing, this is just going to waste our time.
			if newRequest.req.RemainingAllocations() == 0 {
				continue
			}

			// keep track of our allocations
			currentlyAllocated += azureMinimumBandwidth

			// record it as live
			i.live[newRequest.req.ID()] = newRequest.req

			common.GetLifecycleMgr().Info(fmt.Sprintf("accepting request %s with %d bytes needed", newRequest.req.ID(), newRequest.req.RemainingAllocations()))
		} else {
			break
		}
	}

	if !limitRequested || len(i.live) == 0 {
		// if there's no limit requested, everyone gets everything they want-- the requests will check and see that a hard limit isn't enforced.
		// if there's nobody to give bandwidth to, this escapes a div by 0.
		return
	}

	requestsPopped := 0
	// If there's a limit requested, we need to distribute it. In phase 1 of distribution, we'll see if any requests are lower than our "average".
	// We don't bother looping this, because our average is only likely to be lower if we did any distributions during this phase.
	averageAllocationSize := hardLimit / int64(len(i.live))
	for k, v := range i.live {
		if remNeeded := v.RemainingAllocations(); int64(remNeeded) <= averageAllocationSize {
			common.GetLifecycleMgr().Info(fmt.Sprintf("%s receiving %d bytes (early clear)", v.ID(), remNeeded))

			v.issueBytes(remNeeded)
			hardLimit -= int64(remNeeded)
			delete(i.live, k)
			v.Discard()
			requestsPopped++
		}
	}

	if len(i.live) == 0 {
		return
	}

	// recalculate our average, then redistribute it to all live requests
	averageAllocationSize = hardLimit / int64(len(i.live))
	for k, v := range i.live {
		remainingBytes := v.issueBytes(int(min(averageAllocationSize, math.MaxInt)))
		common.GetLifecycleMgr().Info(fmt.Sprintf("%s receiving %d bytes (%d remain, %d bytes to read left)", v.ID(), averageAllocationSize, remainingBytes, v.RemainingReads()))

		if remainingBytes == 0 {
			delete(i.live, k)
			v.Discard()
			requestsPopped++
		}
	}

	limitRequested, hardLimit = i.HardLimit()
	observedBps, _ = i.Bandwidth()
	common.GetLifecycleMgr().Info(fmt.Sprintf("average alloc: %d (%d requests live, %d before averaging, %d popped this cycle, %d bytes observed throughput, %d hard cap requested)", averageAllocationSize, len(i.live), hardLimit, requestsPopped, observedBps, hardLimit))
}
