package hostinfo

import "strings"

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

func DarwinFSType(fsType string) string {
	switch {
	case strings.HasPrefix(fsType, "nfs"):
		return "nas-nfs"
	case strings.HasPrefix(fsType, "smb"):
		return "nas-smb"
	case fsType == "":
		return ""
	default:
		return "local-disk"
	}
}
