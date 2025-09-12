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
	"log"
	"net/http"
	"regexp"
	"strings"
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
}

// RoundRobinTransport implements http.RoundTripper with retries and cooldowns
type RoundRobinTransport struct {
	ips        []*IPEntry
	host       string
	healthyIPs atomic.Value // []*IPEntry, cached healthy list
	counter    uint32
	transport  *http.Transport
	maxRetries int
	cooldown   time.Duration // how long to wait before retrying unhealthy IP
}

// Set private network arguments
func SetPrivateNetworkArgs(privateNetworkEnabled bool, privateEndpointIPs []string, bucketName string) {
	re := regexp.MustCompile(`[^0-9.]`)
	privateNetworkArgs.Enabled = privateNetworkEnabled
	for i, ip := range privateEndpointIPs {
		ipAddress := strings.TrimSpace(ip) // removes spaces, tabs, newlines
		privateNetworkArgs.PrivateEndpointIPs[i] = re.ReplaceAllString(ipAddress, "")
	}
	privateNetworkArgs.BucketName = bucketName
}

// RoundRobinTransport creates the transport
func NewRoundRobinTransport(ips []string, host string, maxRetries int, cooldown time.Duration) *RoundRobinTransport {
	entries := make([]*IPEntry, len(ips))
	for i, ip := range ips {
		entries[i] = &IPEntry{IP: ip, unhealthy: 0, lastChecked: time.Now()}
		fmt.Println("PrivateEndpoint%d IPAddress:%s Unhealthy Status:%d LastChecked :%v\n ", i, entries[i].IP, entries[i].unhealthy, entries[i].lastChecked)
	}

	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: false, ServerName: host}

	rr := &RoundRobinTransport{
		ips:        entries,
		host:       host,
		transport:  tr,
		maxRetries: maxRetries,
		cooldown:   cooldown,
	}
	rr.refreshHealthyPool()
	return rr
}

// RoundTrip retries request with different IPs up to rr.maxRetries
func (rr *RoundRobinTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var lastErr error
	var peIP string
	for attempt := 1; attempt < (rr.maxRetries + 1); attempt++ {
		healthy := rr.healthyIPs.Load().([]*IPEntry)
		if len(healthy) == 0 {
			fmt.Errorf("[Attempt %d] No healthy IPs available", attempt)
			return nil, fmt.Errorf("no healthy IPs available")
		}

		idx := atomic.AddUint32(&rr.counter, 1)
		entry := healthy[idx%uint32(len(healthy))]
		peIP := entry.IP
		fmt.Println("[Attempt %d Counter=%d]  -> Selected IP: %s Unhealth Status: %d LastTime: %v\n",
			attempt, idx, peIP, entry.unhealthy, entry.lastChecked)

		// Skip if still in cooldown
		if atomic.LoadInt32(&entry.unhealthy) == 1 &&
			time.Since(entry.lastChecked) < rr.cooldown {
			fmt.Println("[Attempt %d Counter=%d] Skipping IP %s (still in cooldown)",
				attempt, idx, peIP)
			continue
		}

		// Try request with this IP
		clonedReq := req.Clone(req.Context())

		fmt.Println("[RoundTrip] Original request: %s://%s%s", clonedReq.URL.Scheme, clonedReq.URL.Host, clonedReq.URL.Path)
		fmt.Println("[RoundTrip] Original Request Header Host: %s", clonedReq.Host)

		// Override destination to PE IP:Port
		clonedReq.URL.Scheme = req.URL.Scheme
		clonedReq.URL.Host = peIP

		// Keep original Host header so S3 understands the request
		clonedReq.Host = rr.host

		fmt.Println("[RoundTrip] Updated request: %s://%s%s", clonedReq.URL.Scheme, clonedReq.URL.Host, clonedReq.URL.Path)
		fmt.Println("[RoundTrip] Updated Request Header Host: %s", clonedReq.Host)

		resp, err := rr.transport.RoundTrip(clonedReq)
		if err == nil {
			// Success: mark IP healthy
			atomic.StoreInt32(&entry.unhealthy, 0)
			rr.refreshHealthyPool()
			fmt.Println("[Attempt %d Counter %d] SUCCESS using IP %s", attempt, idx, peIP)
			return resp, nil
		}

		log.Printf("[Attempt %d Counter %d] FAILED using IP %s -> %v", attempt, idx, peIP, err)
		if resp != nil {
			fmt.Errorf("[RoundTrip] Response: %s %s", resp.Proto, resp.Status)
			for k, v := range resp.Header {
				fmt.Errorf("[RoundTrip] Response Header:  %s: %s", k, strings.Join(v, ", "))
			}
		}

		// Failure: mark unhealthy
		atomic.StoreInt32(&entry.unhealthy, 1)
		entry.lastChecked = time.Now()
		fmt.Println("[Attempt %d Counter %d] Marked IP %s as unhealthy", attempt, idx, peIP)
		rr.refreshHealthyPool()

		lastErr = err
	}

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
		fmt.Printf("Private Networking is enabled with Private Endpoints: %v and BucketName: %s\n", privateNetworkArgs.PrivateEndpointIPs, privateNetworkArgs.BucketName)
		return true
	} else {
		fmt.Println("Private Networking is not enabled")
		return false
	}
}
