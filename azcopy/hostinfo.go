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

import (
	"net/http"
	"strings"
	"time"
)

// hostHardwareInfo holds best-effort hardware/OS facts about the machine running
// AzCopy. Each field carries a sentinel ("" or -1) when it cannot be determined
// on the current platform. The per-field probes are implemented in the
// platform-specific hostinfo_*.go files.
type hostHardwareInfo struct {
	osVersion     string // e.g. "Ubuntu 22.04.4 LTS" / "Windows 10 Pro 19045"
	cpuModel      string // e.g. "Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz"
	memoryTotalGB int    // total physical memory rounded to GiB, 0 when unknown
	nicMbps       int    // best NIC link speed in Mbps, -1 when unknown
}

// probeHostHardware gathers best-effort host hardware facts. It never blocks on
// the network and never fails: missing values are returned as sentinels.
func probeHostHardware() hostHardwareInfo {
	return hostHardwareInfo{
		osVersion:     osVersion(),
		cpuModel:      cpuModel(),
		memoryTotalGB: totalMemoryGB(),
		nicMbps:       nicSpeedMbps(),
	}
}

// ---------------------------------------------------------------------------
// Azure Instance Metadata Service (IMDS)
// ---------------------------------------------------------------------------

// imdsTimeout bounds how long the IMDS probe may block job startup. IMDS lives
// on a non-routable link-local address and replies almost instantly when
// present, so a short timeout is enough to detect "not on an Azure VM".
const imdsTimeout = 1 * time.Second

// imdsInfo captures the subset of IMDS data the telemetry agent cares about.
type imdsInfo struct {
	isAzureVM bool // true when IMDS responded (i.e. running on an Azure VM)
}

// probeIMDS queries the Azure Instance Metadata Service to determine whether
// AzCopy is running on an Azure VM and, if so, in which region. It is
// best-effort: any error (including not being on an Azure VM) yields a zero
// value with isAzureVM=false. No proxy is used because IMDS is a non-routable
// link-local address visible only to Azure VMs, and the response is not
// security-sensitive.
func probeIMDS() imdsInfo {
	return probeIMDSWithClient(&http.Client{Timeout: imdsTimeout})
}

// probeIMDSWithClient is the testable core of probeIMDS.
func probeIMDSWithClient(client *http.Client) imdsInfo {
	const url = "http://169.254.169.254/metadata/instance/compute/location?api-version=2021-02-01&format=text"
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return imdsInfo{}
	}
	req.Header.Add("Metadata", "true")

	resp, err := client.Do(req)
	if err != nil {
		return imdsInfo{}
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		// A response (even a non-200) still tells us we are on an Azure VM.
		return imdsInfo{isAzureVM: true}
	}

	return imdsInfo{isAzureVM: true}
}

// ---------------------------------------------------------------------------
// Mount-table parsing helpers (used by the Linux localMountType probe; kept
// here, platform-independent, so they remain unit-testable on any OS).
// ---------------------------------------------------------------------------

// parseMountinfoLine extracts the mount point (field 5) and filesystem type (the
// first field after the " - " separator) from a /proc/self/mountinfo line.
func parseMountinfoLine(line string) (mountPoint, fsType string, ok bool) {
	sep := strings.Index(line, " - ")
	if sep < 0 {
		return "", "", false
	}
	left := strings.Fields(line[:sep])
	right := strings.Fields(line[sep+len(" - "):])
	if len(left) < 5 || len(right) < 1 {
		return "", "", false
	}
	// Field 5 (index 4) is the mount point; octal-style escapes (e.g. \040 for
	// space) are left as-is, which is acceptable for prefix matching of typical
	// mount roots.
	return left[4], right[0], true
}

// pathHasMountPrefix reports whether mountPoint is the mount point covering path
// (either identical, the filesystem root "/", or a path-segment prefix).
func pathHasMountPrefix(path, mountPoint string) bool {
	if mountPoint == "/" || path == mountPoint {
		return true
	}
	return strings.HasPrefix(path, mountPoint+"/")
}

// classifyFSType maps a Linux filesystem type to a telemetry mount category:
// "nas-nfs" | "nas-smb" | "local-disk", or "" for an empty input.
func classifyFSType(fsType string) string {
	switch {
	case fsType == "":
		return ""
	case strings.HasPrefix(fsType, "nfs"): // nfs, nfs4
		return "nas-nfs"
	case fsType == "cifs" || strings.HasPrefix(fsType, "smb"): // cifs, smb3, smbfs
		return "nas-smb"
	default:
		return "local-disk"
	}
}
