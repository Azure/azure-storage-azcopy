package hostinfo

import (
	"fmt"
	"strings"
)

type WindowsRegistryReader interface {
	GetStringValue(name string) (string, uint32, error)
}

func WindowsOSVersion(reader WindowsRegistryReader) string {
	product, _, _ := reader.GetStringValue("ProductName")
	display, _, _ := reader.GetStringValue("DisplayVersion")
	if display == "" {
		display, _, _ = reader.GetStringValue("ReleaseId")
	}
	build, _, _ := reader.GetStringValue("CurrentBuild")

	parts := make([]string, 0, 3)
	if product != "" {
		parts = append(parts, strings.TrimSpace(product))
	}
	if display != "" {
		parts = append(parts, strings.TrimSpace(display))
	}
	out := strings.Join(parts, " ")
	if build != "" {
		out = strings.TrimSpace(fmt.Sprintf("%s (%s)", out, strings.TrimSpace(build)))
	}
	return strings.TrimSpace(out)
}

func WindowsCPUModel(reader WindowsRegistryReader) string {
	name, _, err := reader.GetStringValue("ProcessorNameString")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(name)
}

func WindowsMountType(path string, driveType func(string) int) string {
	path = stripExtendedLengthPrefix(path)
	if isUNCPath(path) {
		return "nas-smb"
	}
	root := volumeRoot(path)
	if root == "" {
		return ""
	}
	return classifyWindowsDriveType(driveType(root))
}

func classifyWindowsDriveType(kind int) string {
	switch kind {
	case driveRemote:
		return "nas-smb"
	case driveFixed, driveRemovable, driveCDROM, driveRAMDisk:
		return "local-disk"
	default:
		return ""
	}
}

const (
	driveUnknown   = 0
	driveNoRootDir = 1
	driveRemovable = 2
	driveFixed     = 3
	driveRemote    = 4
	driveCDROM     = 5
	driveRAMDisk   = 6
)

func stripExtendedLengthPrefix(path string) string {
	if rest, ok := strings.CutPrefix(path, `\\?\UNC\`); ok {
		return `\\` + rest
	}
	if rest, ok := strings.CutPrefix(path, `\\?\`); ok {
		return rest
	}
	return path
}

func isUNCPath(path string) bool {
	return strings.HasPrefix(path, `\\`) || strings.HasPrefix(path, `//`)
}

func volumeRoot(path string) string {
	if len(path) >= 2 && path[1] == ':' {
		letter := path[0]
		if (letter >= 'A' && letter <= 'Z') || (letter >= 'a' && letter <= 'z') {
			return string(letter) + `:\`
		}
	}
	return ""
}
