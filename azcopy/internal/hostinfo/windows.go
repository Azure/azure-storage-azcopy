package hostinfo

import (
	"fmt"
	"strings"

	"github.com/Azure/azure-storage-azcopy/v10/telemetry"
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

// WindowsMountType classifies a local path. mappedTarget returns the UNC path a
// mapped drive such as "Z:" points to, and fileSystemName is only called for
// network paths; both may return "" when the value is unavailable.
func WindowsMountType(path string, driveType func(string) int, mappedTarget, fileSystemName func(string) string) telemetry.MountType {
	path = stripExtendedLengthPrefix(path)
	if isUNCPath(path) {
		host, share := uncHostAndShare(path)
		if mountType := redirectedShareType(host, share); mountType != "" {
			return mountType
		}
		if host == "" || share == "" {
			return telemetry.MountTypeNASSMB
		}
		return networkMountType(fileSystemName(`\\` + host + `\` + share + `\`))
	}
	root := volumeRoot(path)
	if root == "" {
		return ""
	}
	kind := driveType(root)
	if kind != driveRemote {
		return classifyWindowsDriveType(kind)
	}
	if mountType := redirectedShareType(uncHostAndShare(mappedTarget(root[:2]))); mountType != "" {
		return mountType
	}
	return networkMountType(fileSystemName(root))
}

// redirectedShareType returns the mount type of a UNC host and share that is not an SMB server, or "".
func redirectedShareType(host, share string) telemetry.MountType {
	switch {
	case isVMShareHost(host):
		return telemetry.MountTypeVMShare
	case isNonSMBShare(host, share):
		return telemetry.MountTypeNASOther
	}
	return ""
}

// Client for NFS reports "NFS" as the volume file system; other redirectors are treated as SMB.
func networkMountType(fileSystemName string) telemetry.MountType {
	if strings.EqualFold(fileSystemName, "NFS") {
		return telemetry.MountTypeNASNFS
	}
	return telemetry.MountTypeNASSMB
}

// WSL distributions and VirtualBox and VMware shared folders are reached through these UNC hosts.
func isVMShareHost(host string) bool {
	switch strings.ToLower(host) {
	case "wsl$", "wsl.localhost", "vboxsvr", "vboxsrv", "vmware-host":
		return true
	}
	return false
}

// RDP drive redirection and WebDAV use UNC paths but are not SMB shares.
func isNonSMBShare(host, share string) bool {
	host = strings.ToLower(host)
	return host == "tsclient" || strings.Contains(host, "@") || strings.EqualFold(share, "DavWWWRoot")
}

func uncHostAndShare(path string) (host, share string) {
	parts := strings.FieldsFunc(path, func(r rune) bool { return r == '\\' || r == '/' })
	if len(parts) > 0 {
		host = parts[0]
	}
	if len(parts) > 1 {
		share = parts[1]
	}
	return host, share
}

func classifyWindowsDriveType(kind int) telemetry.MountType {
	switch kind {
	case driveRemote:
		return telemetry.MountTypeNASSMB
	case driveFixed, driveRemovable, driveCDROM, driveRAMDisk:
		return telemetry.MountTypeLocalDisk
	default:
		return ""
	}
}

// GetDriveType return values: https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getdrivetypew
const (
	driveUnknown   = 0
	driveNoRootDir = 1
	driveRemovable = 2
	driveFixed     = 3
	driveRemote    = 4
	driveCDROM     = 5
	driveRAMDisk   = 6
)

// Extended-length and UNC prefixes: https://learn.microsoft.com/en-us/windows/win32/fileio/naming-a-file#win32-file-namespaces
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
