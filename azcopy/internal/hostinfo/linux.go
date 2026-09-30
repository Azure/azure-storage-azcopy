package hostinfo

import (
	"bufio"
	"io"
	"strconv"
	"strings"
)

func ParseLinuxOSVersion(reader io.Reader) string {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		if value, ok := strings.CutPrefix(line, "PRETTY_NAME="); ok {
			return strings.Trim(strings.TrimSpace(value), `"`)
		}
	}
	_ = scanner.Err()
	return ""
}

func ParseLinuxCPUModel(reader io.Reader) string {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		if key, value, ok := strings.Cut(line, ":"); ok && strings.TrimSpace(key) == "model name" {
			return strings.TrimSpace(value)
		}
	}
	_ = scanner.Err()
	return ""
}

func ParseLinuxMemoryGB(reader io.Reader) int {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		if value, ok := strings.CutPrefix(line, "MemTotal:"); ok {
			fields := strings.Fields(value)
			if len(fields) >= 1 {
				if kilobytes, err := strconv.ParseInt(fields[0], 10, 64); err == nil && kilobytes > 0 {
					return int((kilobytes + (1 << 20 / 2)) / (1 << 20))
				}
			}
			return -1
		}
	}
	_ = scanner.Err()
	return -1
}

func LinuxMountTypeFromReader(path string, reader io.Reader) string {
	bestLength := -1
	bestFSType := ""
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		mountPoint, fsType, ok := parseMountinfoLine(scanner.Text())
		if !ok {
			continue
		}
		if pathHasMountPrefix(path, mountPoint) && len(mountPoint) > bestLength {
			bestLength = len(mountPoint)
			bestFSType = fsType
		}
	}
	_ = scanner.Err()
	return classifyLinuxFSType(bestFSType)
}

var mountinfoPathUnescaper = strings.NewReplacer(
	`\040`, " ",
	`\011`, "\t",
	`\012`, "\n",
	`\134`, `\`,
)

// Example: "36 35 98:0 / /mnt/nas\040share rw,noatime - nfs4 server:/export rw".
// Returns: mountPoint="/mnt/nas share", fsType="nfs4", ok=true.
func parseMountinfoLine(line string) (mountPoint, fsType string, ok bool) {
	separator := strings.Index(line, " - ")
	if separator < 0 {
		return "", "", false
	}
	left := strings.Fields(line[:separator])
	right := strings.Fields(line[separator+len(" - "):])
	if len(left) < 5 || len(right) < 1 {
		return "", "", false
	}
	return mountinfoPathUnescaper.Replace(left[4]), right[0], true
}

func pathHasMountPrefix(path, mountPoint string) bool {
	if mountPoint == "/" || path == mountPoint {
		return true
	}
	return strings.HasPrefix(path, mountPoint+"/")
}

func classifyLinuxFSType(fsType string) string {
	switch {
	case fsType == "":
		return ""
	case strings.HasPrefix(fsType, "nfs"):
		return "nas-nfs"
	case fsType == "cifs" || strings.HasPrefix(fsType, "smb"):
		return "nas-smb"
	default:
		return "local-disk"
	}
}
