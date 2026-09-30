//go:build hostinfoe2e

package hostinfo

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

type registryValues map[string]string

func (values registryValues) GetStringValue(name string) (string, uint32, error) {
	value, exists := values[name]
	if !exists {
		return "", 0, errors.New("registry value unavailable")
	}
	return value, 1, nil
}

func TestHostHardwareWindowsOSVersion(t *testing.T) {
	tests := []struct {
		name     string
		values   registryValues
		expected string
	}{
		{"display preferred", registryValues{"ProductName": "Windows 11 Pro", "DisplayVersion": "23H2", "ReleaseId": "2009", "CurrentBuild": "22631"}, "Windows 11 Pro 23H2 (22631)"},
		{"release fallback", registryValues{"ProductName": "Windows 10 Pro", "ReleaseId": "1909", "CurrentBuild": "18363"}, "Windows 10 Pro 1909 (18363)"},
		{"empty display fallback", registryValues{"ProductName": "Windows", "DisplayVersion": "", "ReleaseId": "2009"}, "Windows 2009"},
		{"trim fields", registryValues{"ProductName": " Windows Server ", "DisplayVersion": " 24H2 ", "CurrentBuild": " 26100 "}, "Windows Server 24H2 (26100)"},
		{"product only", registryValues{"ProductName": "Windows Server"}, "Windows Server"},
		{"build only", registryValues{"CurrentBuild": "26100"}, "(26100)"},
		{"unavailable", nil, ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, WindowsOSVersion(test.values))
		})
	}
}

func TestHostHardwareWindowsCPUModel(t *testing.T) {
	assert.Equal(t, "Intel Xeon CPU", WindowsCPUModel(registryValues{"ProcessorNameString": "  Intel Xeon CPU  "}))
	assert.Equal(t, "AMD EPYC", WindowsCPUModel(registryValues{"ProcessorNameString": "AMD EPYC", "Identifier": "not the model"}))
	assert.Empty(t, WindowsCPUModel(registryValues{"ProcessorNameString": "   "}))
	assert.Empty(t, WindowsCPUModel(registryValues{"Identifier": "not the model"}))
}

func TestHostHardwareWindowsDriveClassification(t *testing.T) {
	tests := []struct {
		name     string
		kind     int
		expected string
	}{
		{"remote", driveRemote, "nas-smb"},
		{"fixed", driveFixed, "local-disk"},
		{"removable", driveRemovable, "local-disk"},
		{"optical", driveCDROM, "local-disk"},
		{"ram disk", driveRAMDisk, "local-disk"},
		{"unknown", driveUnknown, ""},
		{"missing root", driveNoRootDir, ""},
		{"unrecognized", 99, ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, classifyWindowsDriveType(test.kind))
		})
	}
}

func TestHostHardwareWindowsPaths(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		stripped string
		root     string
		unc      bool
	}{
		{"drive", `C:\data\file`, `C:\data\file`, `C:\`, false},
		{"extended drive", `\\?\C:\data\file`, `C:\data\file`, `C:\`, false},
		{"lowercase drive", `d:/data/file`, `d:/data/file`, `d:\`, false},
		{"drive relative", `Z:data`, `Z:data`, `Z:\`, false},
		{"unc", `\\server.invalid\share\file`, `\\server.invalid\share\file`, "", true},
		{"extended unc", `\\?\UNC\server.invalid\share\file`, `\\server.invalid\share\file`, "", true},
		{"forward unc", "//server.invalid/share/file", "//server.invalid/share/file", "", true},
		{"relative", `data\file`, `data\file`, "", false},
		{"invalid drive", `1:\data`, `1:\data`, "", false},
		{"empty", "", "", "", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stripped := stripExtendedLengthPrefix(test.path)
			assert.Equal(t, test.stripped, stripped)
			assert.Equal(t, test.root, volumeRoot(stripped))
			assert.Equal(t, test.unc, isUNCPath(stripped))

			resolverCalled := false
			mountType := WindowsMountType(test.path, func(root string) int {
				resolverCalled = true
				assert.Equal(t, test.root, root)
				return driveFixed
			})
			if test.unc {
				assert.Equal(t, "nas-smb", mountType)
			} else if test.root == "" {
				assert.Empty(t, mountType)
			} else {
				assert.Equal(t, "local-disk", mountType)
			}
			assert.Equal(t, test.root != "", resolverCalled)
		})
	}
}
