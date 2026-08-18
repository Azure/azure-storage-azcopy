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
	"fmt"
	"strings"
	"unsafe"

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

	product, _, _ := k.GetStringValue("ProductName")
	display, _, _ := k.GetStringValue("DisplayVersion")
	if display == "" {
		display, _, _ = k.GetStringValue("ReleaseId")
	}
	build, _, _ := k.GetStringValue("CurrentBuild")

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

// cpuModel returns the processor name string from the registry. Returns "" when
// unavailable.
func cpuModel() string {
	k, err := registry.OpenKey(registry.LOCAL_MACHINE, `HARDWARE\DESCRIPTION\System\CentralProcessor\0`, registry.QUERY_VALUE)
	if err != nil {
		return ""
	}
	defer func() { _ = k.Close() }()

	name, _, err := k.GetStringValue("ProcessorNameString")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(name)
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
// Returns 0 when the call fails.
func totalMemoryGB() int {
	var ms memoryStatusEx
	ms.cbSize = uint32(unsafe.Sizeof(ms))
	ret, _, _ := procGlobalMemoryStatusE.Call(uintptr(unsafe.Pointer(&ms)))
	if ret == 0 || ms.ullTotalPhys == 0 {
		return 0
	}
	const gib = 1 << 30
	return int((ms.ullTotalPhys + gib/2) / gib)
}

type windowsNICAdapter struct {
	interfaceType    uint32
	operStatus       uint32
	transmitBitsPerS uint64
	receiveBitsPerS  uint64
}

func bestWindowsNICSpeedMbps(adapters []windowsNICAdapter) int {
	best := -1
	for _, adapter := range adapters {
		if adapter.operStatus != windows.IfOperStatusUp ||
			adapter.interfaceType == windows.IF_TYPE_SOFTWARE_LOOPBACK ||
			adapter.interfaceType == windows.IF_TYPE_TUNNEL {
			continue
		}

		bitsPerSecond := adapter.transmitBitsPerS
		if adapter.receiveBitsPerS > bitsPerSecond {
			bitsPerSecond = adapter.receiveBitsPerS
		}
		if bitsPerSecond == 0 {
			continue
		}

		mbps := int(bitsPerSecond / 1_000_000)
		if mbps > best {
			best = mbps
		}
	}
	return best
}

// nicSpeedMbps returns the highest advertised transmit or receive link speed
// among operational non-loopback, non-tunnel adapters. Returns -1 when Windows
// does not report a usable speed.
func nicSpeedMbps() int {
	const flags = windows.GAA_FLAG_SKIP_UNICAST |
		windows.GAA_FLAG_SKIP_ANYCAST |
		windows.GAA_FLAG_SKIP_MULTICAST |
		windows.GAA_FLAG_SKIP_DNS_SERVER |
		windows.GAA_FLAG_SKIP_FRIENDLY_NAME

	var bufferSize uint32
	err := windows.GetAdaptersAddresses(windows.AF_UNSPEC, flags, 0, nil, &bufferSize)
	if err != windows.ERROR_BUFFER_OVERFLOW || bufferSize == 0 {
		return -1
	}

	for attempts := 0; attempts < 2; attempts++ {
		buffer := make([]byte, bufferSize)
		first := (*windows.IpAdapterAddresses)(unsafe.Pointer(&buffer[0]))
		err = windows.GetAdaptersAddresses(windows.AF_UNSPEC, flags, 0, first, &bufferSize)
		if err == windows.ERROR_BUFFER_OVERFLOW {
			continue
		}
		if err != nil {
			return -1
		}

		adapters := make([]windowsNICAdapter, 0, 8)
		for adapter := first; adapter != nil; adapter = adapter.Next {
			adapters = append(adapters, windowsNICAdapter{
				interfaceType:    adapter.IfType,
				operStatus:       adapter.OperStatus,
				transmitBitsPerS: adapter.TransmitLinkSpeed,
				receiveBitsPerS:  adapter.ReceiveLinkSpeed,
			})
		}
		return bestWindowsNICSpeedMbps(adapters)
	}

	return -1
}

// localMountType classifies the filesystem backing a local Windows path as
// "nas-smb" (UNC share or mapped network drive) or "local-disk". Returns "" when
// the drive type cannot be determined (caller falls back to "local-disk").
//
// Windows does not expose a stable, dependency-free way to distinguish SMB from
// NFS (Client for NFS) mounts, so network-backed paths are reported as "nas-smb"
// on a best-effort basis.
func localMountType(path string) string {
	p := stripExtendedLengthPrefix(path)
	if isUNCPath(p) {
		return "nas-smb"
	}
	root := volumeRoot(p)
	if root == "" {
		return ""
	}
	switch driveType(root) {
	case driveRemote:
		return "nas-smb"
	case driveFixed, driveRemovable, driveCDROM, driveRAMDisk:
		return "local-disk"
	default:
		return ""
	}
}

// Win32 GetDriveType return values.
const (
	driveUnknown   = 0
	driveNoRootDir = 1
	driveRemovable = 2
	driveFixed     = 3
	driveRemote    = 4
	driveCDROM     = 5
	driveRAMDisk   = 6
)

var procGetDriveTypeW = modkernel32.NewProc("GetDriveTypeW")

// driveType wraps GetDriveTypeW for a volume root path such as `C:\`.
func driveType(root string) int {
	p, err := windows.UTF16PtrFromString(root)
	if err != nil {
		return driveUnknown
	}
	ret, _, _ := procGetDriveTypeW.Call(uintptr(unsafe.Pointer(p)))
	return int(ret)
}

// stripExtendedLengthPrefix removes the Windows extended-length path prefixes
// `\\?\` and `\\?\UNC\` so the remaining path can be classified normally. For
// the UNC form it restores a leading `\\`.
func stripExtendedLengthPrefix(p string) string {
	if rest, ok := strings.CutPrefix(p, `\\?\UNC\`); ok {
		return `\\` + rest
	}
	if rest, ok := strings.CutPrefix(p, `\\?\`); ok {
		return rest
	}
	return p
}

// isUNCPath reports whether p is a UNC path (e.g. `\\server\share\...`).
func isUNCPath(p string) bool {
	return strings.HasPrefix(p, `\\`) || strings.HasPrefix(p, `//`)
}

// volumeRoot returns the `X:\` volume root for a drive-letter path, or "" when p
// is not a drive-letter path.
func volumeRoot(p string) string {
	if len(p) >= 2 && p[1] == ':' {
		c := p[0]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') {
			return string(c) + `:\`
		}
	}
	return ""
}
