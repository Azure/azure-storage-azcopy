package azcopy

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAzurePublicCloudFromFirmware(t *testing.T) {
	raw := smbiosFixture(append([]byte{3, 9, 0, 0, 0, 0, 0, 0, 1}, []byte(azurePublicCloudAssetTag+"\x00\x00")...))
	for _, test := range []struct {
		name    string
		size    uint32
		written uint32
		want    bool
	}{
		{"success", uint32(len(raw)), uint32(len(raw)), true},
		{"size query failed", 0, 0, false},
		{"short size", 7, 0, false},
		{"oversized", maxSMBIOSTableBytes + 1, 0, false},
		{"read failed", uint32(len(raw)), 0, false},
		{"grew between calls", uint32(len(raw)), uint32(len(raw) + 1), false},
		{"truncated read", uint32(len(raw)), uint32(len(raw) - 1), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			got := azurePublicCloudFromFirmware(func(buffer []byte) uint32 {
				calls++
				if buffer == nil {
					return test.size
				}
				require.EqualValues(t, test.size, len(buffer))
				copy(buffer, raw)
				return test.written
			})
			require.Equal(t, test.want, got)
			require.LessOrEqual(t, calls, 2)
		})
	}
}

func TestProbeAzureVMWindows(t *testing.T) {
	t.Logf("Azure public cloud detected by local firmware: %t", probeAzureVM())
}
