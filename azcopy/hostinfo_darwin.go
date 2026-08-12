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

//go:build darwin

package azcopy

import (
	"strings"

	"golang.org/x/sys/unix"
)

// osVersion returns the macOS product version, e.g. "macOS 14.5", from the
// kern.osproductversion sysctl. Returns "" when unavailable.
func osVersion() string {
	v, err := unix.Sysctl("kern.osproductversion")
	if err != nil {
		return ""
	}
	v = strings.TrimSpace(v)
	if v == "" {
		return ""
	}
	return "macOS " + v
}

// cpuModel returns the CPU brand string from the machdep.cpu.brand_string
// sysctl. Returns "" when unavailable (e.g. on some Apple Silicon configs).
func cpuModel() string {
	v, err := unix.Sysctl("machdep.cpu.brand_string")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(v)
}

// totalMemoryGB returns total physical memory in GiB from the hw.memsize sysctl
// (reported in bytes). Returns 0 when unavailable.
func totalMemoryGB() int {
	bytes, err := unix.SysctlUint64("hw.memsize")
	if err != nil || bytes == 0 {
		return 0
	}
	const gib = 1 << 30
	return int((bytes + gib/2) / gib)
}

// nicSpeedMbps is not probed on macOS; link speed is not exposed through a
// stable, dependency-free API. Returns -1.
func nicSpeedMbps() int {
	return -1
}

// localMountType classifies the filesystem backing a local macOS path as
// "nas-nfs", "nas-smb", or "local-disk" using statfs(2)'s f_fstypename. Returns
// "" when statfs fails (caller falls back to "local-disk").
func localMountType(path string) string {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return ""
	}
	fsType := int8SliceToString(st.Fstypename[:])
	switch {
	case strings.HasPrefix(fsType, "nfs"):
		return "nas-nfs"
	case strings.HasPrefix(fsType, "smb"): // smbfs
		return "nas-smb"
	case fsType == "":
		return ""
	default:
		return "local-disk"
	}
}

// int8SliceToString converts a NUL-terminated [16]int8-style C string (as used
// by statfs f_fstypename) to a Go string.
func int8SliceToString(b []int8) string {
	buf := make([]byte, 0, len(b))
	for _, c := range b {
		if c == 0 {
			break
		}
		buf = append(buf, byte(c))
	}
	return string(buf)
}
