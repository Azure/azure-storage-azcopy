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
	hostinfointernal "github.com/Azure/azure-storage-azcopy/v10/azcopy/internal/hostinfo"
	"golang.org/x/sys/unix"
)

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

// localMountType classifies the filesystem backing a local macOS path as
// "nas-nfs", "nas-smb", or "local-disk" using statfs(2)'s f_fstypename. Returns
// "" when statfs fails (caller falls back to "local-disk").
func localMountType(path string) string {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return ""
	}
	fsType := unix.ByteSliceToString(st.Fstypename[:])
	return hostinfointernal.DarwinFSType(fsType)
}
