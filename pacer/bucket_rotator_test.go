package pacer

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestBucketRotator tests the facilities of the bucket rotator to
// 1) accurately maintain whether or not the average is ready through the start, inflation, and deflation steps
// 2) accept "overflowing" values and maintain a correct average
// 3) correctly handle a state where averaging isn't possible
func TestBucketRotator(t *testing.T) {
	const (
		ROTATOR_SIZE = 30
	)

	a := assert.New(t)

	rot := newBucketRotator(ROTATOR_SIZE)

	// test averaging
	var sum uint64
	for idx := range uint64(ROTATOR_SIZE) {
		a.Equal(false, rot.AverageReady(), fmt.Sprintf("average ready on idx %d", idx)) // average should not be ready yet.
		if idx > 0 {
			a.Equal(int(sum/idx), int(rot.GetAverage()), "early average should match logic") // idx, not idx+1, because we haven't added the current value yet.
		} else {
			a.Equal(0, int(rot.GetAverage()), "0th element avg should be 0")
		}

		sum += idx + 1
		rot.AddToCurrentValue(idx + 1)
		rot.Rotate()
	}

	a.Equal(ROTATOR_SIZE, int(rot.Size()))
	a.Equal(ROTATOR_SIZE, int(rot.availableValues))
	a.Equal(true, rot.AverageReady(), "Average is expected to be ready after ROTATOR_SIZE rotations")
	a.Equal(int(sum/uint64(ROTATOR_SIZE)), int(rot.GetAverage()))

	// test deflation
	rot.SetSize(ROTATOR_SIZE / 2)
	sum = 0
	for idx := range uint64(ROTATOR_SIZE / 2) {
		sum += idx + (ROTATOR_SIZE / 2) + 1 // re-adjust our results, since it's going to be the more recent half
	}

	a.Equal(ROTATOR_SIZE/2, int(rot.Size()))
	a.Equal(ROTATOR_SIZE/2, int(rot.availableValues))
	a.Equal(true, rot.AverageReady())
	a.Equal(int(sum/(ROTATOR_SIZE/2)), int(rot.GetAverage()))

	// test inflation
	rot.SetSize(ROTATOR_SIZE)

	// we shouldn't be ready to provide an average, but, if we pull one anyway the result should be the same.
	a.Equal(ROTATOR_SIZE, int(rot.Size()))
	a.Equal(false, rot.AverageReady())
	a.Equal(ROTATOR_SIZE/2, int(rot.availableValues))
	a.Equal(int(sum/(ROTATOR_SIZE/2)), int(rot.GetAverage()))

	// if we count back up, then we should get our original average.
	for idx := range uint64(ROTATOR_SIZE / 2) {
		a.Equal(int(sum/((ROTATOR_SIZE/2)+idx)), int(rot.GetAverage()), "early average should match logic") // idx, not idx+1, because we haven't added the current value yet.

		sum += idx + 1
		rot.AddToCurrentValue(idx + 1)
		rot.Rotate()
	}

	// we should be ready to average again.
	a.Equal(ROTATOR_SIZE, int(rot.Size()))
	a.Equal(true, rot.AverageReady())
	a.Equal(ROTATOR_SIZE, int(rot.availableValues))
	a.Equal(int(sum/uint64(ROTATOR_SIZE)), int(rot.GetAverage()))

	// if we push over by 1, it should delete the first value ever placed in the table.
	rot.AddToCurrentValue(ROTATOR_SIZE + 1)
	sum -= (ROTATOR_SIZE / 2) + 1 // remove the proceeding value, which should be the first value after the halfway mark.
	sum += ROTATOR_SIZE + 1       // add the new value, which is 1 over our cap.
	rot.Rotate()

	// validate our new average matches our expectations.
	a.Equal(ROTATOR_SIZE, int(rot.Size()))
	a.Equal(true, rot.AverageReady())
	a.Equal(ROTATOR_SIZE, int(rot.availableValues))
	a.Equal(int(sum/uint64(ROTATOR_SIZE)), int(rot.GetAverage()))

	// deflate to 0.
	rot.SetSize(0)

	a.Equal(0, int(rot.Size()))
	a.Equal(0, int(rot.availableValues))
	a.Equal(false, rot.AverageReady())
	a.Equal(0, int(rot.GetAverage()))

	// if we try to push a value and rotate the only value will be 0.
	rot.AddToCurrentValue(5)
	a.Equal(5, int(rot.buckets[0]))
	rot.Rotate()
	a.Equal(0, int(rot.buckets[0]))
}
