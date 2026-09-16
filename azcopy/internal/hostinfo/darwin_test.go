//go:build hostinfoe2e

package hostinfo

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHostHardwareDarwinOSVersion(t *testing.T) {
	assert.Equal(t, "macOS 14.5", DarwinOSVersion("14.5", nil))
	assert.Equal(t, "macOS 15.6.1", DarwinOSVersion(" 15.6.1\n", nil))
	assert.Empty(t, DarwinOSVersion("", nil))
	assert.Empty(t, DarwinOSVersion(" \n", nil))
	assert.Empty(t, DarwinOSVersion("14.5", errors.New("sysctl unavailable")))
}

func TestHostHardwareDarwinCPUModel(t *testing.T) {
	assert.Equal(t, "Intel Core i7", DarwinCPUModel(" Intel Core i7 \n", nil))
	assert.Equal(t, "Apple M4", DarwinCPUModel("Apple M4", nil))
	assert.Empty(t, DarwinCPUModel("", nil))
	assert.Empty(t, DarwinCPUModel(" \n", nil))
	assert.Empty(t, DarwinCPUModel("Apple M4", errors.New("sysctl unavailable")))
}

func TestHostHardwareDarwinMemoryGB(t *testing.T) {
	assert.Equal(t, 16, DarwinMemoryGB(16<<30, nil))
	assert.Equal(t, 1, DarwinMemoryGB((1<<30)+(1<<29)-1, nil))
	assert.Equal(t, 2, DarwinMemoryGB((1<<30)+(1<<29), nil))
	assert.Equal(t, -1, DarwinMemoryGB(0, nil))
	assert.Equal(t, -1, DarwinMemoryGB(16<<30, errors.New("sysctl unavailable")))
}

func TestHostHardwareDarwinClassifyFSType(t *testing.T) {
	tests := []struct {
		fsType   string
		expected string
	}{
		{"nfs", "nas-nfs"},
		{"nfs4", "nas-nfs"},
		{"smbfs", "nas-smb"},
		{"smb3", "nas-smb"},
		{"apfs", "local-disk"},
		{"hfs", "local-disk"},
		{"exfat", "local-disk"},
		{"", ""},
	}
	for _, test := range tests {
		t.Run(test.fsType, func(t *testing.T) {
			assert.Equal(t, test.expected, DarwinFSType(test.fsType))
		})
	}
}
