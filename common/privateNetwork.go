// Copyright © 2017 Microsoft <wastore@microsoft.com>
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

package common

import (
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ==============================================================================================
// For C2C Private Networking configurations
// ==============================================================================================
type PrivateNetworkConfig struct {
	Enabled            bool     // By default private network is disabled unless user explicitly enables it
	PrivateEndpointIPs []string // List of private endpoint IPs
	BucketName         string   // Bucket Name required to form Endpoint URL
}

var privateNetworkArgs PrivateNetworkConfig = PrivateNetworkConfig{}

// PE Health Status Enum
type HealthStatus uint32

const (
	Healthy HealthStatus = iota
	Unhealthy
)

// IPEntry holds one private IP with health info
type IPEntry struct {
	IP               string
	ConnectionStatus HealthStatus // 0 = healthy, 1 = unhealthy
	LastChecked      time.Time
	LastErrCode      int
	LastErrMsg       string
	NumRequests      uint64 // field to store number of requests sent through this IP
	IpEntryLock      sync.RWMutex
}

//var UnHealthyPrivateEndpoints = make(map[string]IPEntry)
//var UnHealthyMu sync.RWMutex

// Global variables for external access to private endpoint IPs
var globalPrivateEndpointIPs []*IPEntry
var globalIPsMutex sync.RWMutex

// RoundRobinTransport implements http.RoundTripper with retries and cooldowns
type RoundRobinTransport struct {
	//ips  []*IPEntry
	host string
	//	healthyIPs      atomic.Value // []*IPEntry, cached healthy list
	counter         uint64
	transport       *http.Transport
	cooldown        time.Duration // how long to wait before retrying unhealthy IP
	perIPRetries    int           // number of times to retry the same IP before moving on
	perIPRetryDelay time.Duration // delay between retries to same IP
	//	stopChan        chan struct{} // channel to stop the periodic refresh goroutine
	counterLock sync.RWMutex // mutex to protect access to the RoundRobinTransport fields
}

// Set private network arguments
func SetPrivateNetworkArgs(privateNetworkEnabled bool, privateEndpointIPs []string, bucketName string) {
	re := regexp.MustCompile(`[^0-9.]`)
	var resultPeEndpoints []string
	privateNetworkArgs.Enabled = privateNetworkEnabled
	for _, ip := range privateEndpointIPs {
		ipAddress := strings.TrimSpace(ip) // removes spaces, tabs, newlines
		resultPeEndpoints = append(resultPeEndpoints, re.ReplaceAllString(ipAddress, ""))
	}
	privateNetworkArgs.PrivateEndpointIPs = resultPeEndpoints
	privateNetworkArgs.BucketName = bucketName
}

// RoundRobinTransport creates the transport
func NewRoundRobinTransport(ips []string, host string, cooldownInSecs int, ipRetries int, ipRetryIntervalInMilliSecs int) *RoundRobinTransport {
	SetGlobalPrivateEndpointIPs(ips)
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: false, ServerName: host}

	rr := &RoundRobinTransport{
		//ips:             globalPrivateEndpointIPs,
		host:            host,
		transport:       tr,
		cooldown:        time.Duration(cooldownInSecs) * time.Second,
		perIPRetries:    ipRetries,
		perIPRetryDelay: time.Duration(ipRetryIntervalInMilliSecs) * time.Millisecond,
		// stopChan:        make(chan struct{}),
	}
	//rr.refreshHealthyPool()

	// Start a goroutine to periodically refresh the healthy pool
	//go rr.periodicHealthyPoolRefresh()
	return rr
}

// RoundTrip retries request with different IPs up to rr.maxRetries.
// For each chosen IP, it will retry the same IP rr.perIPRetries times (with a small delay)
// before marking the IP unhealthy and moving on to the next IP.
func (rr *RoundRobinTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var lastErr error
	var peIP string

	//healthy := rr.healthyIPs.Load().([]*IPEntry)
	//initialHealthyCount := len(healthy)
	numPrivateEndpoints := GetGlobalPrivateEndpointIPCount()
	for iter := 0; iter < numPrivateEndpoints; iter++ {

		rr.counterLock.Lock()
		idx := rr.counter % uint64(numPrivateEndpoints)
		atomic.AddUint64(&rr.counter, 1)
		rr.counterLock.Unlock()

		entry := globalPrivateEndpointIPs[idx]
		peIP = entry.IP
		//log.Printf("Selected Private endpoint IP: %s Unhealth Status: %d LastTime: %v\n",
		// peIP, entry.ConnectionStatus, entry.LastChecked)

		// Skip if still in cooldown
		if entry.ConnectionStatus == Unhealthy {
			if time.Since(entry.LastChecked) >= rr.cooldown {
				entry.MarkHealthy()
				updateStatus := UpdateGlobalPrivateEndpointIP(idx, entry)
				log.Printf("Updating Private Endpoint:%s connection state from UNHEALTHY->HEALTHY after cooldown at %v (LastChecked: %v) with Update Status:%s", peIP, time.Now(), entry.LastChecked, updateStatus)
			} else {
				log.Printf("[Counter=%d] Skipping Unhealthy IP %s (still in cooldown) (LastChecked: %v)", idx, peIP, entry.LastChecked)
				continue
			}
		}

		// Try the same IP up to perIPRetries times before moving on
		for ipAttempt := 0; ipAttempt < rr.perIPRetries; ipAttempt++ {
			// Re-create a fresh clone for each attempt (body-safe for idempotent requests)
			clonedReq := req.Clone(req.Context())

			// Override destination to PE IP:Port and preserve original Host header
			clonedReq.URL.Scheme = req.URL.Scheme
			clonedReq.URL.Host = peIP
			clonedReq.Host = rr.host

			log.Printf("[Counter=%d Retry=%d] Sending request to PrivateEndpoint IP: %s (Host header: %s)", idx, ipAttempt, clonedReq.URL.Host, clonedReq.Host)

			resp, err := rr.transport.RoundTrip(clonedReq)
			if err == nil {
				log.Printf("[Counter=%d Retry=%d] SUCCESS using IP %s", idx, ipAttempt, peIP)
				//entry.IncrementNumRequests()
				globalIPsMutex.Lock()
				defer globalIPsMutex.Unlock()
				globalPrivateEndpointIPs[idx].IncrementNumRequests()
				return resp, nil
			}

			// Transport-level failure (err != nil). Capture diagnostics.
			log.Printf("[Counter=%d Retry=%d] FAILED using IP %s with Error %v", idx, ipAttempt, peIP, err)
			var errCode int
			var errMsg string

			// If resp is non-nil on error, close body to avoid leaks and capture ConnectionStatus for diagnostics.
			if resp != nil {
				// best-effort: capture ConnectionStatus and close body
				errCode = resp.StatusCode
				errMsg = resp.Status
				if resp.Body != nil {
					b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
					_ = resp.Body.Close()
					bodySnippet := strings.TrimSpace(string(b))
					if bodySnippet != "" {
						errMsg = fmt.Sprintf("%s: %s", resp.Status, bodySnippet)
					} else {
						errMsg = resp.Status
					}
				} else {
					errMsg = resp.Status
				}
				//entry.UpdateIPEntryError(errCode, errMsg)
			} else {
				errCode = 0
				errMsg = err.Error()
				//entry.UpdateIPEntryError(0, err.Error())
			}

			lastErr = err

			// If we still have per-IP attempts left, wait and retry the same IP
			if ipAttempt+1 < rr.perIPRetries {
				log.Printf("[Counter=%d Retry=%d] Retrying same IP %s after %v", idx, ipAttempt, peIP, rr.perIPRetryDelay)
				time.Sleep(rr.perIPRetryDelay)
				continue
			}

			// Exhausted per-IP retries: mark the IP unhealthy, record time, update global map and healthy pool,
			// then break to pick another IP (outer loop continues).
			// attempt to mark unhealthy: 0 -> 1
			if atomic.CompareAndSwapUint32((*uint32)(&entry.ConnectionStatus), uint32(Healthy), uint32(Unhealthy)) {
				entry.MarkUnhealthy(errCode, errMsg)
				updateStatus := UpdateGlobalPrivateEndpointIP(idx, entry)
				log.Printf("Updating Private Endpoint:%s connection state from HEALTHY->UNHEALTHY after error response with Error Code %d ErrorMsg:%s at %v Update Status:%s", peIP, entry.LastErrCode, entry.LastErrMsg, entry.LastChecked, updateStatus)
			}
			// break inner loop; outer loop will select another IP (or finish if attempts exhausted)
			break
		}
		// continue outer loop to try next IP (if any attempts remain)
	}

	fmt.Errorf("No healthy Private Endpoint IPs are available")
	// All attempts exhausted
	return nil, fmt.Errorf("Request failed after trying all healthy Private Endpoint IPs. Last error from IP %s: %v", peIP, lastErr)
}

// refreshHealthyPool updates the healthy IP list atomically
// func (rr *RoundRobinTransport) refreshHealthyPool() {
// 	var healthy []*IPEntry
// 	for _, e := range rr.ips {
// 		// log.Printf("refreshHealthyPool Counter: %d IP Address: %s Unhealthy: %d\n", rr.counter, e.IP, e.ConnectionStatus)
// 		if (e.ConnectionStatus == Healthy) || (time.Since(e.LastChecked) >= rr.cooldown) {
// 			healthy = append(healthy, e)
// 		}
// 	}
// 	log.Printf("refreshHealthyPool Counter: %d Healthy Pool Count: %d\n", rr.counter, len(healthy))
// 	rr.healthyIPs.Store(healthy)
// }

// periodicHealthyPoolRefresh runs in a goroutine and refreshes the healthy pool periodically
// func (rr *RoundRobinTransport) periodicHealthyPoolRefresh() {
// 	ticker := time.NewTicker(rr.cooldown)
// 	defer ticker.Stop()

// 	for {
// 		select {
// 		case <-ticker.C:
// 			rr.refreshHealthyPool()
// 		case <-rr.stopChan:
// 			log.Printf("Stopping periodic healthy pool refresh goroutine")
// 			return
// 		}
// 	}
// }

// Close cleans up idle connections and stops the periodic refresh goroutine
func (rr *RoundRobinTransport) Close() {
	//close(rr.stopChan)
	rr.transport.CloseIdleConnections()
}

// GetPrivateEndpointStatus returns a copy of all private IP entries in the round robin transport
func GetPrivateEndpointStatus() []*IPEntry {
	globalIPsMutex.RLock()
	defer globalIPsMutex.RUnlock()

	if globalPrivateEndpointIPs == nil {
		return nil
	}

	// Return a copy to prevent external modifications
	result := make([]*IPEntry, len(globalPrivateEndpointIPs))
	copy(result, globalPrivateEndpointIPs)
	return result
}

// SetGlobalPrivateEndpointIPs sets the global private endpoint IPs for external access
func SetGlobalPrivateEndpointIPs(ips []string) {
	globalIPsMutex.Lock()
	defer globalIPsMutex.Unlock()

	for i, ip := range ips {
		globalPrivateEndpointIPs[i] = InitializePrivateEndpointIpEntry(ip)
		log.Printf("PrivateEndpointIp:%s is set to Healthy\n ", globalPrivateEndpointIPs[i].IP)
	}
	log.Printf("Number of Private Endpoints:%d set", len(globalPrivateEndpointIPs))
}

// GetGlobalPrivateEndpointIPCount returns the number of global private endpoint IPs
func GetGlobalPrivateEndpointIPCount() int {
	globalIPsMutex.RLock()
	defer globalIPsMutex.RUnlock()

	if globalPrivateEndpointIPs == nil {
		return 0
	}
	return len(globalPrivateEndpointIPs)
}

// UpdateGlobalPrivateEndpointIP updates a specific IP entry in the global list
func UpdateGlobalPrivateEndpointIP(index uint64, updatedEntry *IPEntry) bool {
	globalIPsMutex.Lock()
	defer globalIPsMutex.Unlock()

	if globalPrivateEndpointIPs == nil || index >= uint64(len(globalPrivateEndpointIPs)) || updatedEntry == nil {
		log.Printf("Updation of PrivateEndpoint List failed for the Index:%d with Length:%d\n ", index, len(globalPrivateEndpointIPs))
		return false
	}

	globalPrivateEndpointIPs[index] = updatedEntry
	return true
}

// Function to check if private network is enabled or not. By default it should be disabled and return false
func IsPrivateNetworkEnabled() bool {
	if privateNetworkArgs.Enabled {
		//fmt.Printf("Private Networking is enabled with Private Endpoints: %v and BucketName: %s\n", privateNetworkArgs.PrivateEndpointIPs, privateNetworkArgs.BucketName)
		return true
	} else {
		//fmt.Println("Private Networking is not enabled")
		return false
	}
}

// GetUnHealthyPrivateEndpoints returns a copy of the global unhealthy endpoints map.
// The returned map is a shallow copy of IPEntry values (structs are copied),
// so callers can read/inspect or marshal the returned map without holding locks
// and without affecting the global map.
// func GetUnHealthyPrivateEndpoints() map[string]IPEntry {
// 	UnHealthyMu.RLock()
// 	defer UnHealthyMu.RUnlock()

// 	if len(UnHealthyPrivateEndpoints) == 0 {
// 		return nil
// 	}

// 	copyMap := make(map[string]IPEntry, len(UnHealthyPrivateEndpoints))
// 	for k, v := range UnHealthyPrivateEndpoints {
// 		copyMap[k] = v
// 	}
// 	return copyMap
// }

// UpdateIPEntryError safely updates the error code and error message for an IPEntry
// This function is thread-safe and uses the IPEntry's mutex for synchronization
// func (entry *IPEntry) UpdateIPEntryError(errCode int, errMsg string) {
// 	entry.IpEntryLock.Lock()
// 	defer entry.IpEntryLock.Unlock()

// 	entry.LastErrCode = errCode
// 	entry.LastErrMsg = errMsg
// }

// ClearIPEntryError safely clears the error code and error message for an IPEntry
// This function is thread-safe and uses the IPEntry's mutex for synchronization
// func (entry *IPEntry) ClearIPEntryError() {
// 	entry.IpEntryLock.Lock()
// 	defer entry.IpEntryLock.Unlock()

// 	entry.LastErrCode = 0
// 	entry.LastErrMsg = ""
// }

// UpdateLastChecked safely updates the LastChecked timestamp for an IPEntry
// This function is thread-safe and uses the IPEntry's mutex for synchronization
// func (entry *IPEntry) UpdateLastChecked() {
// 	entry.IpEntryLock.Lock()
// 	defer entry.IpEntryLock.Unlock()

// 	entry.LastChecked = time.Now()
// }

// MarkHealthy safely marks the IPEntry as healthy
func (entry *IPEntry) MarkHealthy() {
	entry.IpEntryLock.Lock()
	defer entry.IpEntryLock.Unlock()

	entry.ConnectionStatus = Healthy
	entry.LastErrCode = 0
	entry.LastErrMsg = ""
	entry.LastChecked = time.Now()
	log.Printf("Marking Private Endpoint %s as Healthy at time %v\n", entry.IP, entry.LastChecked)
}

// MarkUnhealthy safely marks the IPEntry as unhealthy with error details
func (entry *IPEntry) MarkUnhealthy(errCode int, errMsg string) {
	entry.IpEntryLock.Lock()
	defer entry.IpEntryLock.Unlock()
	entry.ConnectionStatus = Unhealthy
	entry.LastErrCode = errCode
	entry.LastErrMsg = errMsg
	entry.LastChecked = time.Now()
	log.Printf("Marking Private Endpoint %s as Unhealthy with Error Code: %d Error Message:%s at time %v\n", entry.IP, entry.LastErrCode, entry.LastErrMsg, entry.LastChecked)
}

// MarkUnhealthy safely marks the IPEntry as unhealthy with error details
func InitializePrivateEndpointIpEntry(privateEndpointIp string) *IPEntry {
	defaultErrorCode := 0
	defaultErrorMessage := ""

	entry := &IPEntry{
		IP:               privateEndpointIp,
		ConnectionStatus: Healthy,
		LastErrCode:      defaultErrorCode,
		LastErrMsg:       defaultErrorMessage,
		LastChecked:      time.Now(),
		NumRequests:      0,
	}
	log.Printf("Initializing IPEntry for Private Endpoint %s as Healthy at time %v\n", entry.IP, entry.LastChecked)
	return entry
}

// IncrementNumRequests safely increments the NumRequests counter for an IPEntry
func (entry *IPEntry) IncrementNumRequests() {
	entry.IpEntryLock.Lock()
	defer entry.IpEntryLock.Unlock()
	entry.NumRequests++
}

// UpdateIPEntryError safely updates the error code and error message for an IPEntry
// This function is thread-safe and uses the IPEntry's mutex for synchronization
// func (entry *IPEntry) UpdateIPEntryError(errCode int, errMsg string) {
// 	entry.IpEntryLock.Lock()
// 	defer entry.IpEntryLock.Unlock()

// 	entry.LastErrCode = errCode
// 	entry.LastErrMsg = errMsg
// }
