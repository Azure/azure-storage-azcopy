// Copyright © Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

//go:build linux

package azcopy

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// osVersion returns a human-readable OS version derived from /etc/os-release
// (PRETTY_NAME), e.g. "Ubuntu 22.04.4 LTS". Returns "" when unavailable.
func osVersion() string {
	f, err := os.Open("/etc/os-release")
	if err != nil {
		return ""
	}
	defer func() { _ = f.Close() }()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if v, ok := strings.CutPrefix(line, "PRETTY_NAME="); ok {
			return strings.Trim(strings.TrimSpace(v), `"`)
		}
	}
	return ""
}

// cpuModel returns the CPU model name from /proc/cpuinfo ("model name").
// Returns "" when unavailable.
func cpuModel() string {
	f, err := os.Open("/proc/cpuinfo")
	if err != nil {
		return ""
	}
	defer func() { _ = f.Close() }()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if k, v, ok := strings.Cut(line, ":"); ok && strings.TrimSpace(k) == "model name" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

// totalMemoryGB returns total physical memory in GiB from /proc/meminfo
// (MemTotal, reported in kB). Returns 0 when unavailable.
func totalMemoryGB() int {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0
	}
	defer func() { _ = f.Close() }()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if v, ok := strings.CutPrefix(line, "MemTotal:"); ok {
			fields := strings.Fields(v) // e.g. "16384000 kB"
			if len(fields) >= 1 {
				if kb, err := strconv.ParseInt(fields[0], 10, 64); err == nil {
					return int((kb + (1 << 20 / 2)) / (1 << 20)) // kB -> GiB, rounded
				}
			}
			return 0
		}
	}
	return 0
}

// nicSpeedMbps returns the highest link speed (in Mbps) among non-loopback
// network interfaces, read from /sys/class/net/<iface>/speed. Returns -1 when no
// speed can be determined (e.g. virtualized NICs report -1 or are unreadable).
func nicSpeedMbps() int {
	entries, err := os.ReadDir("/sys/class/net")
	if err != nil {
		return -1
	}
	best := -1
	for _, e := range entries {
		name := e.Name()
		if name == "lo" {
			continue
		}
		b, err := os.ReadFile(filepath.Join("/sys/class/net", name, "speed"))
		if err != nil {
			continue
		}
		speed, err := strconv.Atoi(strings.TrimSpace(string(b)))
		if err != nil || speed <= 0 {
			continue
		}
		if speed > best {
			best = speed
		}
	}
	return best
}

// localMountType inspects /proc/self/mountinfo to classify the filesystem backing
// the given local path: "nas-nfs", "nas-smb", or "local-disk". Returns "" when
// the mount table cannot be read or the path cannot be resolved (caller falls
// back to "local-disk").
func localMountType(path string) string {
	abs, err := filepath.Abs(path)
	if err != nil {
		return ""
	}
	abs = filepath.Clean(abs)

	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return ""
	}
	defer func() { _ = f.Close() }()

	bestLen := -1
	bestFSType := ""
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		mountPoint, fsType, ok := parseMountinfoLine(scanner.Text())
		if !ok {
			continue
		}
		if pathHasMountPrefix(abs, mountPoint) && len(mountPoint) > bestLen {
			bestLen = len(mountPoint)
			bestFSType = fsType
		}
	}
	return classifyFSType(bestFSType)
}
