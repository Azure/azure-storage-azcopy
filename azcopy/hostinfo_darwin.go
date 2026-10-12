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
	"path/filepath"

	hostinfointernal "github.com/Azure/azure-storage-azcopy/v10/azcopy/internal/hostinfo"
	"github.com/Azure/azure-storage-azcopy/v10/telemetry"
	"golang.org/x/sys/unix"
)

// unix.Sysctl wraps sysctlbyname: https://developer.apple.com/documentation/kernel/1387446-sysctlbyname

// osVersion returns the macOS product version, e.g. "macOS 14.5", from the
// kern.osproductversion sysctl. Returns "" when unavailable.
func osVersion() string {
	return hostinfointernal.DarwinOSVersion(unix.Sysctl("kern.osproductversion"))
}

// cpuModel returns the CPU brand string from the machdep.cpu.brand_string
// sysctl. Returns "" when unavailable (e.g. on some Apple Silicon configs).
func cpuModel() string {
	return hostinfointernal.DarwinCPUModel(unix.Sysctl("machdep.cpu.brand_string"))
}

// totalMemoryGB returns total physical memory in GiB from the hw.memsize sysctl
// (reported in bytes). Returns -1 when unavailable.
func totalMemoryGB() int {
	return hostinfointernal.DarwinMemoryGB(unix.SysctlUint64("hw.memsize"))
}

// osArchitecture reports arm64 on Apple silicon even when an amd64 build runs under Rosetta.
func osArchitecture() string {
	if arm64, err := unix.SysctlUint32("hw.optional.arm64"); err == nil && arm64 == 1 {
		return "arm64"
	}
	machine, err := unix.Sysctl("hw.machine")
	if err != nil {
		return ""
	}
	return hostinfointernal.NormalizeArchitecture(machine)
}

// localMountType classifies the filesystem backing a local macOS path as
// "nas-nfs", "nas-smb", "nas-other" (any other non-local mount, such as AFP,
// WebDAV, or macFUSE), or "local-disk" using statfs(2). Returns "" when statfs
// fails (caller falls back to "local-disk").
// See https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/statfs.2.html
func localMountType(path string) telemetry.MountType {
	// statfs follows symlinks, so a symlink is classified by the directory that contains it.
	var link unix.Stat_t
	if unix.Lstat(path, &link) == nil && link.Mode&unix.S_IFMT == unix.S_IFLNK {
		path = filepath.Dir(path)
	}
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return ""
	}
	fsType := unix.ByteSliceToString(st.Fstypename[:])
	return hostinfointernal.DarwinMountType(fsType, st.Flags&unix.MNT_LOCAL != 0)
}
