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

import "github.com/Azure/azure-storage-azcopy/v10/common/buildmode"

// ChannelPressureProfile defines the thresholds and delays for different channel pressure levels
type ChannelPressureProfile struct {
	Enabled        bool  // If false, backpressure is completely disabled (always returns 0 delay)
	MinChannelSize int   // Minimum channel size to apply backpressure (avoid sleeping on small channels)
	Thresholds     []int // Empty percentage thresholds (ascending order)
	Delays         []int // Corresponding delay values in milliseconds
}

// Predefined profiles for different types of channels
var (
	// Profile for chunk channels (more conservative delays)
	DefaultProfile = ChannelPressureProfile{
		Enabled:        buildmode.IsMover,
		MinChannelSize: 1000,
		Thresholds:     []int{10, 20},    // Empty % thresholds
		Delays:         []int{20, 10, 0}, // Delays: <10%->20ms, 10-20%->10ms, >20%->0ms
	}

	// Profile with backpressure completely disabled
	DisabledProfile = ChannelPressureProfile{
		Enabled:        false,
		MinChannelSize: 0,
		Thresholds:     []int{},
		Delays:         []int{},
	}

	// Profile for chunk channels (more conservative delays)
	TransferChannelProfile = ChannelPressureProfile{
		Enabled:        buildmode.IsMover,
		MinChannelSize: 1000,
		Thresholds:     []int{10, 20},    // Empty % thresholds
		Delays:         []int{20, 10, 0}, // Delays: <10%->20ms, 10-20%->10ms, >20%->0ms
	}

	// Profile for chunk channels (more conservative delays)
	ChunkTransferProfile = ChannelPressureProfile{
		Enabled:        buildmode.IsMover,
		MinChannelSize: 1000,
		Thresholds:     []int{5, 10, 20},     // Empty % thresholds
		Delays:         []int{50, 20, 10, 0}, // Delays: <5%->50ms, 5-10%->20ms, 10-20%->10ms, >20%->0ms
	}
)

// CalculateChannelBackPressureDelay calculates the delay based on channel fullness using the specified profile
func CalculateChannelBackPressureDelay(capacity, used int, profile ChannelPressureProfile) int {
	// If backpressure is not enabled, always return 0
	if !profile.Enabled {
		return 0
	}

	// It's risky to sleep if channel size is too small
	if capacity <= profile.MinChannelSize {
		return 0
	}

	emptyPercent := ((capacity - used) * 100) / capacity

	// Find the appropriate delay based on empty percentage
	for i, threshold := range profile.Thresholds {
		if emptyPercent < threshold {
			return profile.Delays[i]
		}
	}

	// If we exceed all thresholds, return the last delay value
	return profile.Delays[len(profile.Delays)-1]
}
