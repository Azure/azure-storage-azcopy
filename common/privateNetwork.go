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
}

var UnHealthyPrivateEndpoints = make(map[string]IPEntry)
var UnHealthyMu sync.RWMutex

// RoundRobinTransport implements http.RoundTripper with retries and cooldowns
type RoundRobinTransport struct {
	ips             []*IPEntry
	host            string
	healthyIPs      atomic.Value // []*IPEntry, cached healthy list
	counter         uint64
	transport       *http.Transport
	cooldown        time.Duration // how long to wait before retrying unhealthy IP
	perIPRetries    int           // number of times to retry the same IP before moving on
	perIPRetryDelay time.Duration // delay between retries to same IP
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
	entries := make([]*IPEntry, len(ips))
	for i, ip := range ips {
		entries[i] = &IPEntry{IP: ip, ConnectionStatus: Healthy, LastChecked: time.Now()}
		log.Printf("PrivateEndpoint%d IPAddress:%s Unhealthy Status:%d LastChecked :%v\n ", i, entries[i].IP, entries[i].ConnectionStatus, entries[i].LastChecked)
	}

	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: false, ServerName: host}

	rr := &RoundRobinTransport{
		ips:             entries,
		host:            host,
		transport:       tr,
		cooldown:        time.Duration(cooldownInSecs) * time.Second,
		perIPRetries:    ipRetries,
		perIPRetryDelay: time.Duration(ipRetryIntervalInMilliSecs) * time.Millisecond,
	}
	rr.refreshHealthyPool()
	return rr
}

// RoundTrip retries request with different IPs up to rr.maxRetries.
// For each chosen IP, it will retry the same IP rr.perIPRetries times (with a small delay)
// before marking the IP unhealthy and moving on to the next IP.
func (rr *RoundRobinTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var lastErr error
	var peIP string

	healthy := rr.healthyIPs.Load().([]*IPEntry)
	initialHealthyCount := len(healthy)
	for attempt := 1; attempt <= initialHealthyCount; attempt++ {
		if len(healthy) == 0 {
			fmt.Errorf("No healthy Private Endpoint IPs are available", attempt)
			return nil, fmt.Errorf("no healthy Private Endpoint IPs are available")
		}

		idx := atomic.AddUint64(&rr.counter, 1)
		entry := healthy[idx%uint64(len(healthy))]
		peIP = entry.IP
		//log.Printf("Selected Private endpoint IP: %s Unhealth Status: %d LastTime: %v\n",
		// peIP, entry.ConnectionStatus, entry.LastChecked)

		// Skip if still in cooldown
		if entry.ConnectionStatus == Unhealthy &&
			time.Since(entry.LastChecked) < rr.cooldown {
			log.Printf("[Counter=%d] Skipping Unhealthy IP %s (still in cooldown)", idx, peIP)
			continue
		}

		// Try the same IP up to perIPRetries times before moving on
		for ipAttempt := 1; ipAttempt <= rr.perIPRetries; ipAttempt++ {
			// Re-create a fresh clone for each attempt (body-safe for idempotent requests)
			clonedReq := req.Clone(req.Context())

			// Override destination to PE IP:Port and preserve original Host header
			clonedReq.URL.Scheme = req.URL.Scheme
			clonedReq.URL.Host = peIP
			clonedReq.Host = rr.host

			log.Printf("[Counter=%d Retry=%d] Sending request to PrivateEndpoint IP: %s (Host header: %s)", idx, ipAttempt, clonedReq.URL.Host, clonedReq.Host)

			resp, err := rr.transport.RoundTrip(clonedReq)
			if err == nil {
				if atomic.CompareAndSwapUint32((*uint32)(&entry.ConnectionStatus), uint32(Unhealthy), uint32(Healthy)) {
					// remove from global unhealthy map and refresh pool once (state changed)
					UnHealthyMu.Lock()
					delete(UnHealthyPrivateEndpoints, entry.IP)
					UnHealthyMu.Unlock()

					// update diagnostics (clear)
					entry.LastErrCode = 0
					entry.LastErrMsg = ""

					rr.refreshHealthyPool()
					// log.Printf("[Counter=%d Retry=%d] SUCCESS using IP %s", idx, ipAttempt, peIP)
				}
				return resp, nil
			}

			// Transport-level failure (err != nil). Capture diagnostics.
			log.Printf("[Counter=%d Retry=%d] FAILED using IP %s with Error %v", idx, ipAttempt, peIP, err)

			// If resp is non-nil on error, close body to avoid leaks and capture ConnectionStatus for diagnostics.
			if resp != nil {
				// best-effort: capture ConnectionStatus and close body
				entry.LastErrCode = resp.StatusCode
				entry.LastErrMsg = resp.Status
				if resp.Body != nil {
					b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
					_ = resp.Body.Close()
					bodySnippet := strings.TrimSpace(string(b))
					if bodySnippet != "" {
						entry.LastErrMsg = fmt.Sprintf("%s: %s", resp.Status, bodySnippet)
					} else {
						entry.LastErrMsg = resp.Status
					}
				} else {
					entry.LastErrMsg = resp.Status
				}
			} else {
				entry.LastErrCode = 0
				entry.LastErrMsg = err.Error()
			}

			lastErr = err

			// If we still have per-IP attempts left, wait and retry the same IP
			if ipAttempt < rr.perIPRetries {
				log.Printf("[Counter=%d Retry=%d] Retrying same IP %s after %v", idx, ipAttempt, peIP, rr.perIPRetryDelay)
				time.Sleep(rr.perIPRetryDelay)
				continue
			}

			// Exhausted per-IP retries: mark the IP unhealthy, record time, update global map and healthy pool,
			// then break to pick another IP (outer loop continues).
			// attempt to mark unhealthy: 0 -> 1
			if atomic.CompareAndSwapUint32((*uint32)(&entry.ConnectionStatus), uint32(Healthy), uint32(Unhealthy)) {
				entry.LastChecked = time.Now()
				// populate global unhealthy map and refresh pool on transition
				UnHealthyMu.Lock()
				UnHealthyPrivateEndpoints[peIP] = *entry
				UnHealthyMu.Unlock()

				rr.refreshHealthyPool()
				log.Printf("[Counter %d] Marked IP %s as unhealthy after %d failed attempts with Error Message: %s",
					idx, peIP, rr.perIPRetries, entry.LastErrMsg)
			}
			// break inner loop; outer loop will select another IP (or finish if attempts exhausted)
			break
		}
		// continue outer loop to try next IP (if any attempts remain)
	}

	// All attempts exhausted
	return nil, fmt.Errorf("Request failed after trying all healthy Private Endpoint IPs. Last error from IP %s: %v", peIP, lastErr)
}

// refreshHealthyPool updates the healthy IP list atomically
func (rr *RoundRobinTransport) refreshHealthyPool() {
	var healthy []*IPEntry
	for _, e := range rr.ips {
		// log.Printf("refreshHealthyPool Counter: %d IP Address: %s Unhealthy: %d\n", rr.counter, e.IP, e.ConnectionStatus)
		if (e.ConnectionStatus == Healthy) || (time.Since(e.LastChecked) >= rr.cooldown) {
			healthy = append(healthy, e)
		}
	}
	log.Printf("refreshHealthyPool Counter: %d Healthy Pool Count: %d\n", rr.counter, len(healthy))
	rr.healthyIPs.Store(healthy)
}

// Close cleans up idle connections
func (rr *RoundRobinTransport) Close() {
	rr.transport.CloseIdleConnections()
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
func GetUnHealthyPrivateEndpoints() map[string]IPEntry {
	UnHealthyMu.RLock()
	defer UnHealthyMu.RUnlock()

	if len(UnHealthyPrivateEndpoints) == 0 {
		return nil
	}

	copyMap := make(map[string]IPEntry, len(UnHealthyPrivateEndpoints))
	for k, v := range UnHealthyPrivateEndpoints {
		copyMap[k] = v
	}
	return copyMap
}
