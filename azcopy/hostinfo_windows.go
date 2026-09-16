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

//go:build windows

package azcopy

import (
	"unsafe"

	hostinfointernal "github.com/Azure/azure-storage-azcopy/v10/azcopy/internal/hostinfo"
	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/registry"
)

// osVersion returns a human-readable Windows version composed from the registry,
// e.g. "Windows 11 Pro 23H2 (22631)". Returns "" when the registry cannot be
// read.
func osVersion() string {
	k, err := registry.OpenKey(registry.LOCAL_MACHINE, `SOFTWARE\Microsoft\Windows NT\CurrentVersion`, registry.QUERY_VALUE)
	if err != nil {
		return ""
	}
	defer func() { _ = k.Close() }()

	return hostinfointernal.WindowsOSVersion(k)
}

// cpuModel returns the processor name string from the registry. Returns "" when
// unavailable.
func cpuModel() string {
	k, err := registry.OpenKey(registry.LOCAL_MACHINE, `HARDWARE\DESCRIPTION\System\CentralProcessor\0`, registry.QUERY_VALUE)
	if err != nil {
		return ""
	}
	defer func() { _ = k.Close() }()

	return hostinfointernal.WindowsCPUModel(k)
}

// memoryStatusEx mirrors the Win32 MEMORYSTATUSEX structure.
type memoryStatusEx struct {
	cbSize                  uint32
	dwMemoryLoad            uint32
	ullTotalPhys            uint64
	ullAvailPhys            uint64
	ullTotalPageFile        uint64
	ullAvailPageFile        uint64
	ullTotalVirtual         uint64
	ullAvailVirtual         uint64
	ullAvailExtendedVirtual uint64
}

var (
	modkernel32             = windows.NewLazySystemDLL("kernel32.dll")
	procGlobalMemoryStatusE = modkernel32.NewProc("GlobalMemoryStatusEx")
)

// totalMemoryGB returns total physical memory in GiB via GlobalMemoryStatusEx.
// Returns -1 when the call fails.
func totalMemoryGB() int {
	var ms memoryStatusEx
	ms.cbSize = uint32(unsafe.Sizeof(ms))
	ret, _, _ := procGlobalMemoryStatusE.Call(uintptr(unsafe.Pointer(&ms)))
	if ret == 0 || ms.ullTotalPhys == 0 {
		return -1
	}
	return hostinfointernal.PhysicalMemoryGB(ms.ullTotalPhys)
}

// localMountType classifies the filesystem backing a local Windows path as
// "nas-smb" (UNC share or mapped network drive) or "local-disk". Returns "" when
// the drive type cannot be determined (caller falls back to "local-disk").
//
// Windows does not expose a stable, dependency-free way to distinguish SMB from
// NFS (Client for NFS) mounts, so network-backed paths are reported as "nas-smb"
// on a best-effort basis.
func localMountType(path string) string {
	return hostinfointernal.WindowsMountType(path, driveType)
}

var procGetDriveTypeW = modkernel32.NewProc("GetDriveTypeW")

// driveType wraps GetDriveTypeW for a volume root path such as `C:\`.
func driveType(root string) int {
	p, err := windows.UTF16PtrFromString(root)
	if err != nil {
		return 0
	}
	ret, _, _ := procGetDriveTypeW.Call(uintptr(unsafe.Pointer(p)))
	return int(ret)
}
