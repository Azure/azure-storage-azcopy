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

// IPEntry holds one private IP with health info
type IPEntry struct {
	IP          string
	unhealthy   int32 // 0 = healthy, 1 = unhealthy
	lastChecked time.Time
	lastErrCode int
	lastErrMsg  string
}

var UnHealthyPrivateEndpoints = make(map[string]IPEntry)
var UnHealthyMu sync.RWMutex

// RoundRobinTransport implements http.RoundTripper with retries and cooldowns
type RoundRobinTransport struct {
	ips             []*IPEntry
	host            string
	healthyIPs      atomic.Value // []*IPEntry, cached healthy list
	counter         uint32
	transport       *http.Transport
	maxRetries      int
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
func NewRoundRobinTransport(ips []string, host string, maxRetries int, cooldownInSecs int, ipRetries int, ipRetryIntervalInMilliSecs int) *RoundRobinTransport {
	entries := make([]*IPEntry, len(ips))
	for i, ip := range ips {
		entries[i] = &IPEntry{IP: ip, unhealthy: 0, lastChecked: time.Now()}
		log.Printf("PrivateEndpoint%d IPAddress:%s Unhealthy Status:%d LastChecked :%v\n ", i, entries[i].IP, entries[i].unhealthy, entries[i].lastChecked)
	}

	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: false, ServerName: host}

	rr := &RoundRobinTransport{
		ips:             entries,
		host:            host,
		transport:       tr,
		maxRetries:      maxRetries,
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

	for attempt := 1; attempt <= rr.maxRetries; attempt++ {
		healthy := rr.healthyIPs.Load().([]*IPEntry)
		if len(healthy) == 0 {
			fmt.Errorf("[Attempt %d] No healthy IPs available", attempt)
			return nil, fmt.Errorf("no healthy IPs available")
		}

		idx := atomic.AddUint32(&rr.counter, 1)
		entry := healthy[idx%uint32(len(healthy))]
		peIP = entry.IP
		log.Printf("[Attempt %d Counter=%d]  -> Selected IP: %s Unhealth Status: %d LastTime: %v\n",
			attempt, idx, peIP, entry.unhealthy, entry.lastChecked)

		// Skip if still in cooldown
		if atomic.LoadInt32(&entry.unhealthy) == 1 &&
			time.Since(entry.lastChecked) < rr.cooldown {
			log.Printf("[Attempt %d Counter=%d] Skipping IP %s (still in cooldown)",
				attempt, idx, peIP)
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

			log.Printf("[RoundTrip] [%d.%d] Sending request to %s (Host header: %s)", attempt, ipAttempt, clonedReq.URL.Host, clonedReq.Host)

			resp, err := rr.transport.RoundTrip(clonedReq)
			if err == nil {
				// Success on this IP: mark healthy, remove from global unhealthy map, return resp
				atomic.StoreInt32(&entry.unhealthy, 0)

				UnHealthyMu.Lock()
				delete(UnHealthyPrivateEndpoints, entry.IP)
				UnHealthyMu.Unlock()

				// reset last error info
				entry.lastErrCode = 0
				entry.lastErrMsg = ""

				rr.refreshHealthyPool()
				log.Printf("[Attempt %d Counter %d.%d] SUCCESS using IP %s", attempt, idx, ipAttempt, peIP)
				return resp, nil
			}

			// Transport-level failure (err != nil). Capture diagnostics.
			log.Printf("[Attempt %d Counter %d.%d] FAILED using IP %s -> %v", attempt, idx, ipAttempt, peIP, err)

			// If resp is non-nil on error, close body to avoid leaks and capture status for diagnostics.
			if resp != nil {
				// best-effort: capture status and close body
				entry.lastErrCode = resp.StatusCode
				entry.lastErrMsg = resp.Status
				if resp.Body != nil {
					b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
					_ = resp.Body.Close()
					bodySnippet := strings.TrimSpace(string(b))
					if bodySnippet != "" {
						entry.lastErrMsg = fmt.Sprintf("%s: %s", resp.Status, bodySnippet)
					} else {
						entry.lastErrMsg = resp.Status
					}
				} else {
					entry.lastErrMsg = resp.Status
				}
			} else {
				entry.lastErrCode = 0
				entry.lastErrMsg = err.Error()
			}

			lastErr = err

			// If we still have per-IP attempts left, wait and retry the same IP
			if ipAttempt < rr.perIPRetries {
				log.Printf("[Attempt %d Counter %d.%d] Retrying same IP %s after %v", attempt, idx, ipAttempt, peIP, rr.perIPRetryDelay)
				time.Sleep(rr.perIPRetryDelay)
				continue
			}

			// Exhausted per-IP retries: mark the IP unhealthy, record time, update global map and healthy pool,
			// then break to pick another IP (outer loop continues).
			atomic.StoreInt32(&entry.unhealthy, 1)
			entry.lastChecked = time.Now()
			log.Printf("[Attempt %d Counter %d.%d] Marked IP %s as unhealthy after %d failed attempts: %s", attempt, idx, ipAttempt, peIP, rr.perIPRetries, entry.lastErrMsg)
			rr.refreshHealthyPool()

			UnHealthyMu.Lock()
			UnHealthyPrivateEndpoints[peIP] = *entry
			UnHealthyMu.Unlock()
			// break inner loop; outer loop will select another IP (or finish if attempts exhausted)
			break
		}
		// continue outer loop to try next IP (if any attempts remain)
	}

	// All attempts exhausted
	return nil, fmt.Errorf("all retries for ip %v failed after %d attempts: %w", peIP, rr.maxRetries, lastErr)
}

// refreshHealthyPool updates the healthy IP list atomically
func (rr *RoundRobinTransport) refreshHealthyPool() {
	var healthy []*IPEntry
	for _, e := range rr.ips {
		log.Printf("refreshHealthyPool Counter: %d IP Address: %s Unhealthy: %d\n", rr.counter, e.IP, e.unhealthy)
		if atomic.LoadInt32(&e.unhealthy) == 0 ||
			time.Since(e.lastChecked) >= rr.cooldown {
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
