package hostinfo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHostHardwareNormalizeArchitecture(t *testing.T) {
	for machine, expected := range map[string]string{
		"x86_64":  "amd64",
		"AMD64":   "amd64",
		"aarch64": "arm64",
		"arm64":   "arm64",
		"i686":    "386",
		"i386":    "386",
		"armv7l":  "arm",
		"armv8l":  "arm",
		"s390x":   "s390x",
		"ppc64le": "ppc64le",
		" x86_64": "amd64",
		"":        "",
	} {
		assert.Equal(t, expected, NormalizeArchitecture(machine), machine)
	}
}

func TestHostHardwareWindowsArchitecture(t *testing.T) {
	assert.Equal(t, "amd64", WindowsArchitecture(0x8664))
	assert.Equal(t, "arm64", WindowsArchitecture(0xAA64))
	assert.Equal(t, "386", WindowsArchitecture(0x014C))
	assert.Equal(t, "arm", WindowsArchitecture(0x01C4))
	assert.Empty(t, WindowsArchitecture(0))
	assert.Empty(t, WindowsArchitecture(0x0200))
}
