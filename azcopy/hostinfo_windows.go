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
	"time"
	"unsafe"

	hostinfointernal "github.com/Azure/azure-storage-azcopy/v10/azcopy/internal/hostinfo"
	"github.com/Azure/azure-storage-azcopy/v10/telemetry"
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
// See https://learn.microsoft.com/en-us/windows/win32/api/sysinfoapi/nf-sysinfoapi-globalmemorystatusex
func totalMemoryGB() int {
	var ms memoryStatusEx
	ms.cbSize = uint32(unsafe.Sizeof(ms))
	ret, _, _ := procGlobalMemoryStatusE.Call(uintptr(unsafe.Pointer(&ms)))
	if ret == 0 || ms.ullTotalPhys == 0 {
		return -1
	}
	return hostinfointernal.PhysicalMemoryGB(ms.ullTotalPhys)
}

var procIsWow64Process2 = modkernel32.NewProc("IsWow64Process2")

// osArchitecture returns "" before Windows 10 version 1511, which lacks IsWow64Process2.
// See https://learn.microsoft.com/en-us/windows/win32/api/wow64apiset/nf-wow64apiset-iswow64process2
func osArchitecture() string {
	if procIsWow64Process2.Find() != nil {
		return ""
	}
	var processMachine, nativeMachine uint16
	ok, _, _ := procIsWow64Process2.Call(uintptr(windows.CurrentProcess()), uintptr(unsafe.Pointer(&processMachine)), uintptr(unsafe.Pointer(&nativeMachine)))
	if ok == 0 {
		return ""
	}
	return hostinfointernal.WindowsArchitecture(nativeMachine)
}

// localMountType classifies the filesystem backing a local Windows path as
// "nas-nfs", "nas-smb", "nas-other" (RDP drive redirection or WebDAV), "vm-share"
// (WSL, VirtualBox, or VMware shared folders), or
// "local-disk". Returns "" when the drive type cannot be determined (caller falls
// back to "local-disk").
func localMountType(path string) telemetry.MountType {
	return hostinfointernal.WindowsMountType(path, driveType, mappedDriveTarget, volumeFileSystemName)
}

var procWNetGetConnectionW = windows.NewLazySystemDLL("mpr.dll").NewProc("WNetGetConnectionW")

// mappedDriveTarget returns the UNC path a mapped drive such as "Z:" points to, or "".
// See https://learn.microsoft.com/en-us/windows/win32/api/winnetwk/nf-winnetwk-wnetgetconnectionw
func mappedDriveTarget(drive string) string {
	if procWNetGetConnectionW.Find() != nil {
		return ""
	}
	local, err := windows.UTF16PtrFromString(drive)
	if err != nil {
		return ""
	}
	var remote [1024]uint16
	size := uint32(len(remote))
	ret, _, _ := procWNetGetConnectionW.Call(uintptr(unsafe.Pointer(local)), uintptr(unsafe.Pointer(&remote[0])), uintptr(unsafe.Pointer(&size)))
	if ret != 0 {
		return ""
	}
	return windows.UTF16ToString(remote[:])
}

// Bounded because querying an unreachable share can block for the SMB connection timeout.
func volumeFileSystemName(root string) string {
	result := make(chan string, 1)
	go func() { result <- queryVolumeFileSystemName(root) }()
	select {
	case name := <-result:
		return name
	case <-time.After(time.Second):
		return ""
	}
}

// See https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getvolumeinformationw
func queryVolumeFileSystemName(root string) string {
	p, err := windows.UTF16PtrFromString(root)
	if err != nil {
		return ""
	}
	var name [windows.MAX_PATH + 1]uint16
	if err := windows.GetVolumeInformation(p, nil, 0, nil, nil, nil, &name[0], uint32(len(name))); err != nil {
		return ""
	}
	return windows.UTF16ToString(name[:])
}

var procGetDriveTypeW = modkernel32.NewProc("GetDriveTypeW")

// driveType wraps GetDriveTypeW for a volume root path such as `C:\`.
// See https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getdrivetypew
func driveType(root string) int {
	p, err := windows.UTF16PtrFromString(root)
	if err != nil {
		return 0
	}
	ret, _, _ := procGetDriveTypeW.Call(uintptr(unsafe.Pointer(p)))
	return int(ret)
}
