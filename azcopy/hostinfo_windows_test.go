//go:build windows

package azcopy

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sys/windows"
)

func TestBestWindowsNICSpeedMbps(t *testing.T) {
	adapters := []windowsNICAdapter{
		{interfaceType: 6, operStatus: windows.IfOperStatusDown, transmitBitsPerS: 100_000_000_000},
		{interfaceType: windows.IF_TYPE_SOFTWARE_LOOPBACK, operStatus: windows.IfOperStatusUp, transmitBitsPerS: 100_000_000_000},
		{interfaceType: windows.IF_TYPE_TUNNEL, operStatus: windows.IfOperStatusUp, transmitBitsPerS: 100_000_000_000},
		{interfaceType: 6, operStatus: windows.IfOperStatusUp},
		{interfaceType: 6, operStatus: windows.IfOperStatusUp, transmitBitsPerS: 1_000_000_000, receiveBitsPerS: 2_500_000_000},
		{interfaceType: 71, operStatus: windows.IfOperStatusUp, transmitBitsPerS: 10_000_000_000, receiveBitsPerS: 5_000_000_000},
	}

	assert.Equal(t, 10000, bestWindowsNICSpeedMbps(adapters))
	assert.Equal(t, -1, bestWindowsNICSpeedMbps(nil))
	assert.Equal(t, -1, bestWindowsNICSpeedMbps(adapters[:4]))
	assert.Equal(t, 0, bestWindowsNICSpeedMbps([]windowsNICAdapter{{
		interfaceType:    6,
		operStatus:       windows.IfOperStatusUp,
		transmitBitsPerS: 999_999,
	}}))
}

func TestNICSpeedMbpsWindows(t *testing.T) {
	speedMbps := nicSpeedMbps()
	assert.GreaterOrEqual(t, speedMbps, -1)
	t.Logf("detected Windows NIC speed: %d Mbps", speedMbps)
}
