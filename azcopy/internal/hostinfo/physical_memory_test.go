package hostinfo

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHostHardwarePhysicalMemoryGB(t *testing.T) {
	const gib uint64 = 1 << 30
	maxUint64GB := uint64(1) << 34
	maxUint64Expected := -1
	if strconv.IntSize == 64 {
		maxUint64Expected = int(maxUint64GB)
	}
	tests := []struct {
		name     string
		bytes    uint64
		expected int
	}{
		{"zero bytes", 0, 0},
		{"below half GiB", gib/2 - 1, 0},
		{"half GiB", gib / 2, 1},
		{"one GiB", gib, 1},
		{"below rounding boundary", gib + gib/2 - 1, 1},
		{"round half up", gib + gib/2, 2},
		{"eight GiB", 8 * gib, 8},
		{"large host", 4 * 1024 * gib, 4096},
		{"maximum uint64", ^uint64(0), maxUint64Expected},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, PhysicalMemoryGB(test.bytes))
		})
	}
}

func TestHostHardwarePhysicalMemoryGB32BitBoundary(t *testing.T) {
	if strconv.IntSize != 32 {
		t.Skip("32-bit int boundary")
	}

	const (
		gib      uint64 = 1 << 30
		maxInt32 uint64 = 1<<31 - 1
	)
	assert.Equal(t, int(maxInt32), PhysicalMemoryGB(maxInt32*gib+gib/2-1))
	assert.Equal(t, -1, PhysicalMemoryGB(maxInt32*gib+gib/2))
}
