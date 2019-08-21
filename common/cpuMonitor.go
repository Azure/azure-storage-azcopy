// Copyright Microsoft <wastore@microsoft.com>
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

package common

import (
	"runtime"
	"sync/atomic"
	"time"
)

type CPUMonitor interface {
	CPUContentionExists() bool
}

// TODO: consider replacing this file with %age CPU usage measurement from gopsutil (bearing in mind that it would be a little harder to test,
//    since this approach here can be tested by using CPU affinity to constrain a process to a smaller number of CPUs (to make it CPU-
//    constrained, but direct total measurement of CPU usage %age wouldn't support that approach to testing)

type nullCpuMonitor struct{}

func NewNullCpuMonitor() CPUMonitor {
	return &nullCpuMonitor{}
}

func (n *nullCpuMonitor) CPUContentionExists() bool {
	return false
}

// cpuUsageMonitor is loosely adapted from concepts described (for a different purpose) here https://mattwarren.org/2014/06/18/measuring-the-impact-of-the-net-garbage-collector/
type cpuUsageMonitor struct {
	atomicContentionExistsIndicator int32
}

// NewCalibratedCpuUsageMonitor should be called early in the app's life cycle, before we are creating any significant CPU load
// so that it's self-calibration will be accurate
func NewCalibratedCpuUsageMonitor() CPUMonitor {
	c := &cpuUsageMonitor{}

	// start it running and wait until it has self-calibrated
	calibration := make(chan struct{})
	go c.computationWorker(calibration)
	_ = <-calibration

	return c
}

func (c *cpuUsageMonitor) setContentionExists(exists bool) {
	if exists {
		atomic.StoreInt32(&c.atomicContentionExistsIndicator, 1)
	} else {
		atomic.StoreInt32(&c.atomicContentionExistsIndicator, 0)
	}
}

// CPUContentionExists returns true if demand for CPU capacity is affecting
// the ability of our GoRoutines to run when they want to run
func (c *cpuUsageMonitor) CPUContentionExists() bool {
	return atomic.LoadInt32(&c.atomicContentionExistsIndicator) == 1
}

// computationWorker does the computations. It runs forever.
func (c *cpuUsageMonitor) computationWorker(calibrationDone chan struct{}) {
	oldIsSlow := false
	durations := make(chan time.Duration, 1000)
	valuesInWindow := make(chan bool, 100)
	rollingCount := 0
	waitTime := 333 * time.Millisecond            // sleep for this long each time we probe for busy-ness of CPU
	windowSize := int(5 * time.Second / waitTime) // keep track of this many recent samples
	thresholdMultiplier := float64(50)            // a sample is considered slow if its this many times greater than the baseline (or more)
	if runtime.GOOS == "windows" {
		thresholdMultiplier = 10 // testing indicates we need a lower threshold here, but not on Linux (since the lower number gives too many false alarms there)
	}

	// run a separate loop to do the probes/measurements
	go c.monitoringWorker(waitTime, durations)

	_ = <-durations // discard first value, it doesn't seem very reliable

	// get the next 3 and average them, as our baseline. We chose 3 somewhat arbitrarily
	x := <-durations
	y := <-durations
	z := <-durations
	baselineAverageNs := float64((x + y + z).Nanoseconds()) / 3
	close(calibrationDone)

	// loop and monitor
	for i := int64(0); ; i++ {
		d := <-durations
		nano := d.Nanoseconds()

		latestWasSlow := float64(nano) > thresholdMultiplier*baselineAverageNs // it took us over x times longer than normal to wake from sleep

		// maintain count of slow ones in the last "windowSize" intervals
		valuesInWindow <- latestWasSlow
		if latestWasSlow {
			rollingCount++
		} else if len(valuesInWindow) > windowSize {
			oldWasSlow := <-valuesInWindow // "pop" the oldest one
			if oldWasSlow {
				rollingCount--
			}
		}

		// If lots of our recent samples passed the threshold for "slow", then assume that we are indeed generally slow
		isGenerallySlow := rollingCount >= (windowSize / 2)

		if isGenerallySlow != oldIsSlow {
			c.setContentionExists(isGenerallySlow)
			oldIsSlow = isGenerallySlow
		}
	}
}

// monitorCpuUsage, does repeated tests to monitor CPU contention.
// Because its mostly sleeping, it does not itself create significant CPU usage.
// Works by sleeping, and seeing how much longer than expected it takes us to wake back up
// (e.g. did something else have the CPU at the time we wanted to wake up?)
// Note that this times a "sleep", with time.After, because timing a runtime.GoSched didn't always seem to work,
// at least not in the presence of external load from other processes using CPU)
func (c *cpuUsageMonitor) monitoringWorker(waitTime time.Duration, d chan time.Duration) {
	runtime.LockOSThread() // make ourselves as difficult/demanding to schedule as possible
	for {
		start := time.Now()

		select {
		case <-time.After(waitTime):
			// noop
		}

		duration := time.Since(start)
		// how much longer than expected did it take for us to wake up?
		// This is assumed to be time in which we were ready to run, but we couldn't run, because something else was busy on the
		// CPU.
		excessDuration := duration - waitTime

		d <- excessDuration
	}
}
