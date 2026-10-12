package hostinfo

import "strings"

// NormalizeArchitecture maps uname(2) and sysctl machine names to Go's GOARCH names.
func NormalizeArchitecture(machine string) string {
	machine = strings.ToLower(strings.TrimSpace(machine))
	switch machine {
	case "":
		return ""
	case "x86_64", "amd64":
		return "amd64"
	case "aarch64", "arm64":
		return "arm64"
	case "x86", "i386", "i486", "i586", "i686":
		return "386"
	}
	if strings.HasPrefix(machine, "arm") {
		return "arm"
	}
	return machine
}

// WindowsArchitecture maps an IMAGE_FILE_MACHINE value to Go's GOARCH name.
// See https://learn.microsoft.com/en-us/windows/win32/sysinfo/image-file-machine-constants
func WindowsArchitecture(machine uint16) string {
	switch machine {
	case 0x8664:
		return "amd64"
	case 0xAA64:
		return "arm64"
	case 0x014C:
		return "386"
	case 0x01C4:
		return "arm"
	default:
		return ""
	}
}
