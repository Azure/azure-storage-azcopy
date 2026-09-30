package hostinfo

import (
	"strings"

	"github.com/Azure/azure-storage-azcopy/v10/telemetry"
)

func DarwinOSVersion(version string, err error) string {
	if err != nil {
		return ""
	}
	version = strings.TrimSpace(version)
	if version == "" {
		return ""
	}
	return "macOS " + version
}

func DarwinCPUModel(model string, err error) string {
	if err != nil {
		return ""
	}
	return strings.TrimSpace(model)
}

func DarwinMemoryGB(bytes uint64, err error) int {
	if err != nil || bytes == 0 {
		return -1
	}
	return PhysicalMemoryGB(bytes)
}

// DarwinMountType classifies a statfs result; local reports whether MNT_LOCAL is set.
func DarwinMountType(fsType string, local bool) telemetry.MountType {
	switch {
	case strings.HasPrefix(fsType, "nfs"):
		return telemetry.MountTypeNASNFS
	case strings.HasPrefix(fsType, "smb"):
		return telemetry.MountTypeNASSMB
	case fsType == "":
		return ""
	case !local:
		return telemetry.MountTypeNASOther
	default:
		return telemetry.MountTypeLocalDisk
	}
}
