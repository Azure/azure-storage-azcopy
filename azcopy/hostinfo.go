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

package azcopy

// hostHardwareInfo holds best-effort hardware/OS facts about the machine running
// AzCopy. Each field carries a sentinel ("" or -1) when it cannot be determined
// on the current platform. The per-field probes are implemented in the
// platform-specific hostinfo_*.go files.
type hostHardwareInfo struct {
	osVersion     string // e.g. "Ubuntu 22.04.4 LTS" / "Windows 10 Pro 19045"
	cpuModel      string // e.g. "Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz"
	memoryTotalGB int    // total physical memory rounded to GiB, -1 when unknown
}

// probeHostHardware gathers best-effort host hardware facts. It never blocks on
// the network and never fails: missing values are returned as sentinels.
func probeHostHardware() hostHardwareInfo {
	return hostHardwareInfo{
		osVersion:     osVersion(),
		cpuModel:      cpuModel(),
		memoryTotalGB: totalMemoryGB(),
	}
}

// HostHardwareInfo is the read-only host probe snapshot exposed to E2E tests.
type HostHardwareInfo struct {
	OSVersion     string
	CPUModel      string
	MemoryTotalGB int
}

// ProbeHostHardware returns the same host snapshot used by telemetry resource
// assembly without exposing its private representation.
func ProbeHostHardware() HostHardwareInfo {
	hardware := probeHostHardware()
	return HostHardwareInfo{
		OSVersion:     hardware.osVersion,
		CPUModel:      hardware.cpuModel,
		MemoryTotalGB: hardware.memoryTotalGB,
	}
}

// LocalMountType returns the best-effort backing-store category for a local
// path. An empty result means the platform could not determine the mount type.
func LocalMountType(path string) string {
	return localMountType(path)
}
