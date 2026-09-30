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
	"os"
	"path/filepath"

	hostinfointernal "github.com/Azure/azure-storage-azcopy/v10/azcopy/internal/hostinfo"
)

// osVersion returns a human-readable OS version derived from /etc/os-release
// (PRETTY_NAME), e.g. "Ubuntu 22.04.4 LTS". Returns "" when unavailable.
func osVersion() string {
	f, err := os.Open("/etc/os-release")
	if err != nil {
		return ""
	}
	defer func() { _ = f.Close() }()

	return hostinfointernal.ParseLinuxOSVersion(f)
}

// cpuModel returns the CPU model name from /proc/cpuinfo ("model name").
// Returns "" when unavailable.
func cpuModel() string {
	f, err := os.Open("/proc/cpuinfo")
	if err != nil {
		return ""
	}
	defer func() { _ = f.Close() }()

	return hostinfointernal.ParseLinuxCPUModel(f)
}

// totalMemoryGB returns total physical memory in GiB from /proc/meminfo
// (MemTotal, reported in kB). Returns -1 when unavailable.
func totalMemoryGB() int {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return -1
	}
	defer func() { _ = f.Close() }()

	return hostinfointernal.ParseLinuxMemoryGB(f)
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

	return hostinfointernal.LinuxMountTypeFromReader(abs, f)
}
