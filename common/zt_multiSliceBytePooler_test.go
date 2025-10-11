// Copyright © 2017 Microsoft <wastore@microsoft.com>
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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMultiSliceSlotInfo(t *testing.T) {
	a := assert.New(t)
	eightMB := 8 * 1024 * 1024

	cases := []struct {
		size                 int
		expectedSlotIndex    int
		expectedMaxCapInSlot int
	}{
		{1, 0, 1},
		{2, 1, 2},
		{3, 2, 4},
		{4, 2, 4},
		{5, 3, 8},
		{8, 3, 8},
		{9, 4, 16},
		{eightMB - 1, 23, eightMB},
		{eightMB, 23, eightMB},
		{eightMB + 1, 24, eightMB * 2},
		{100 * 1024 * 1024, 27, 128 * 1024 * 1024},
	}

	for _, x := range cases {
		logBase2 := math.Log2(float64(x.size))
		roundedLogBase2 := int(math.Round(logBase2 + 0.49999999999999)) // rounds up unless already exact(ish)

		// now lets see if the pooler is working as we expect
		slotIndex, maxCap := getSlotInfo(int64(x.size))

		a.Equal(roundedLogBase2, slotIndex)     // this what, mathematically, we expect
		a.Equal(x.expectedSlotIndex, slotIndex) // this what our test case said (should be same)

		a.Equal(x.expectedMaxCapInSlot, maxCap)
	}

}
