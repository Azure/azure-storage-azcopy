package hostinfo

import (
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
)

func TestHostHardwareLinuxClassifyFSType(t *testing.T) {
	assert.Equal(t, "nas-nfs", classifyLinuxFSType("nfs"))
	assert.Equal(t, "nas-nfs", classifyLinuxFSType("nfs4"))
	assert.Equal(t, "nas-smb", classifyLinuxFSType("cifs"))
	assert.Equal(t, "nas-smb", classifyLinuxFSType("smb3"))
	assert.Equal(t, "nas-smb", classifyLinuxFSType("smbfs"))
	assert.Equal(t, "local-disk", classifyLinuxFSType("ext4"))
	assert.Equal(t, "local-disk", classifyLinuxFSType("xfs"))
	assert.Empty(t, classifyLinuxFSType(""))
}

func TestHostHardwareLinuxParseMountinfoLine(t *testing.T) {
	mountPoint, fsType, ok := parseMountinfoLine("36 35 98:0 / /mnt/nas rw,noatime - nfs4 1.2.3.4:/export rw")
	assert.True(t, ok)
	assert.Equal(t, "/mnt/nas", mountPoint)
	assert.Equal(t, "nfs4", fsType)

	mountPoint, fsType, ok = parseMountinfoLine(`36 35 98:0 / /mnt/nas\040share rw,noatime - nfs4 server:/export rw`)
	assert.True(t, ok)
	assert.Equal(t, "/mnt/nas share", mountPoint)
	assert.Equal(t, "nfs4", fsType)

	mountPoint, _, ok = parseMountinfoLine(`36 35 98:0 / /mnt/nas\134040share rw,noatime - nfs4 server:/export rw`)
	assert.True(t, ok)
	assert.Equal(t, `/mnt/nas\040share`, mountPoint)

	mountPoint, _, ok = parseMountinfoLine(`36 35 98:0 / /mnt/nas\011tab\012line rw,noatime - nfs4 server:/export rw`)
	assert.True(t, ok)
	assert.Equal(t, "/mnt/nas\ttab\nline", mountPoint)

	mountPoint, fsType, ok = parseMountinfoLine("22 30 0:21 / / rw,relatime shared:1 - ext4 /dev/root rw")
	assert.True(t, ok)
	assert.Equal(t, "/", mountPoint)
	assert.Equal(t, "ext4", fsType)

	_, _, ok = parseMountinfoLine("garbage line without separator")
	assert.False(t, ok)
}

func TestHostHardwareLinuxPathHasMountPrefix(t *testing.T) {
	assert.True(t, pathHasMountPrefix("/mnt/nas/data", "/mnt/nas"))
	assert.True(t, pathHasMountPrefix("/mnt/nas", "/mnt/nas"))
	assert.True(t, pathHasMountPrefix("/anything", "/"))
	assert.False(t, pathHasMountPrefix("/mnt/nasextra", "/mnt/nas"))
	assert.False(t, pathHasMountPrefix("/home/user", "/mnt/nas"))
}

func TestHostHardwareLinuxOSVersion(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"quoted", "NAME=Ubuntu\nPRETTY_NAME=\"Ubuntu 24.04.3 LTS\"\nVERSION_ID=24.04\n", "Ubuntu 24.04.3 LTS"},
		{"unquoted", "PRETTY_NAME=  Alpine Linux v3.22  \n", "Alpine Linux v3.22"},
		{"exact key", "# PRETTY_NAME=comment\nOTHER_PRETTY_NAME=wrong\nPRETTY_NAME=Debian GNU/Linux\n", "Debian GNU/Linux"},
		{"first match", "PRETTY_NAME=first\nPRETTY_NAME=second\n", "first"},
		{"missing", "NAME=Ubuntu\nVERSION_ID=24.04\n", ""},
		{"empty", "PRETTY_NAME=\"\"\n", ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, ParseLinuxOSVersion(strings.NewReader(test.input)))
		})
	}
	assert.Empty(t, ParseLinuxOSVersion(iotest.ErrReader(errors.New("read failure"))))
}

func TestHostHardwareLinuxCPUModel(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"trim whitespace", "processor\t: 0\nmodel name\t:  Intel Xeon CPU  \n", "Intel Xeon CPU"},
		{"first processor", "model name: first CPU\nprocessor: 1\nmodel name: second CPU\n", "first CPU"},
		{"exact key", "model: 85\nmodel name extra: wrong\nmodel name: AMD EPYC\n", "AMD EPYC"},
		{"embedded colon", "model name: Example: CPU\n", "Example: CPU"},
		{"missing separator", "model name invalid\nmodel name: valid CPU\n", "valid CPU"},
		{"no model name", "processor: 0\nCPU architecture: 8\nHardware: ARM\n", ""},
		{"empty", "model name:\n", ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, ParseLinuxCPUModel(strings.NewReader(test.input)))
		})
	}
	assert.Empty(t, ParseLinuxCPUModel(iotest.ErrReader(errors.New("read failure"))))
}

func TestHostHardwareLinuxMemoryGB(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int
	}{
		{"physical not free", "MemFree: 1048576 kB\nMemTotal: 16777216 kB\n", 16},
		{"below rounding boundary", "MemTotal: 1572863 kB\n", 1},
		{"round half up", "MemTotal: 1572864 kB\n", 2},
		{"sub GiB", "MemTotal: 524288 kB\n", 1},
		{"below half GiB", "MemTotal: 524287 kB\n", 0},
		{"first match", "MemTotal: 8388608 kB\nMemTotal: 16777216 kB\n", 8},
		{"missing units", "MemTotal: 2097152\n", 2},
		{"zero", "MemTotal: 0 kB\n", -1},
		{"negative", "MemTotal: -1 kB\n", -1},
		{"invalid number", "MemTotal: invalid kB\nMemTotal: 2097152 kB\n", -1},
		{"overflow", "MemTotal: 9223372036854775808 kB\n", -1},
		{"missing value", "MemTotal:\n", -1},
		{"missing key", "MemAvailable: 16777216 kB\n", -1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, ParseLinuxMemoryGB(strings.NewReader(test.input)))
		})
	}
	assert.Equal(t, -1, ParseLinuxMemoryGB(iotest.ErrReader(errors.New("read failure"))))
}

func TestHostHardwareLinuxMountSelection(t *testing.T) {
	root := "22 30 0:21 / / rw,relatime shared:1 - ext4 /dev/root rw\n"
	nfs := "36 35 98:0 / /mnt/nas rw,noatime - nfs4 server:/export rw\n"
	smb := "37 35 98:1 / /mnt/nas/team rw - cifs //server/share rw\n"
	escapedNFS := "38 35 98:2 / /mnt/nas\\040share rw - nfs4 server:/export rw\n"
	tests := []struct {
		name     string
		path     string
		mounts   string
		expected string
	}{
		{"longest prefix", "/mnt/nas/team/file", root + nfs + smb, "nas-smb"},
		{"reversed table", "/mnt/nas/team/file", smb + nfs + root, "nas-smb"},
		{"exact mount", "/mnt/nas", root + nfs + smb, "nas-nfs"},
		{"nested file", "/mnt/nas/file", root + nfs, "nas-nfs"},
		{"escaped mount point", "/mnt/nas share/file", root + escapedNFS, "nas-nfs"},
		{"segment boundary", "/mnt/nasextra/file", root + nfs, "local-disk"},
		{"root", "/", root + nfs, "local-disk"},
		{"malformed record", "/mnt/nas/file", "bad - nfs\n" + root + nfs, "nas-nfs"},
		{"long record", "/mnt/nas/file", "36 35 98:0 / /mnt/nas rw " + strings.Repeat("optional ", 9000) + "- nfs4 server:/export rw\n", "nas-nfs"},
		{"no covering mount", "/home/file", nfs, ""},
		{"empty table", "/mnt/nas/file", "", ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, LinuxMountTypeFromReader(test.path, strings.NewReader(test.mounts)))
		})
	}
	assert.Empty(t, LinuxMountTypeFromReader("/", iotest.ErrReader(errors.New("read failure"))))
	partialTable := io.MultiReader(strings.NewReader(root+nfs), iotest.ErrReader(errors.New("read failure")))
	assert.Equal(t, "nas-nfs", LinuxMountTypeFromReader("/mnt/nas/file", partialTable))
}
