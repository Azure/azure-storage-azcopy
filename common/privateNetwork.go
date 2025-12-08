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

// Global variables for external access to private endpoint IPs
var globalPrivateEndpointIPs []*IPEntry
var globalIPsMutex sync.RWMutex

// RoundRobinTransport implements http.RoundTripper with retries and cooldowns
type RoundRobinTransport struct {
	host            string
	counter         uint64
	transport       *http.Transport
	cooldown        time.Duration // how long to wait before retrying unhealthy IP
	perIPRetries    int           // number of times to retry the same IP before moving on
	perIPRetryDelay time.Duration // delay between retries to same IP
	counterLock     sync.RWMutex  // mutex to protect access to the RoundRobinTransport fields
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
		host:            host,
		transport:       tr,
		cooldown:        time.Duration(cooldownInSecs) * time.Second,
		perIPRetries:    ipRetries,
		perIPRetryDelay: time.Duration(ipRetryIntervalInMilliSecs) * time.Millisecond,
	}
	return rr
}

// RoundTrip retries request with different IPs up to rr.maxRetries.
// For each chosen IP, it will retry the same IP rr.perIPRetries times (with a small delay)
// before marking the IP unhealthy and moving on to the next IP.
func (rr *RoundRobinTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var lastErrMsg string
	var peIP string
	var lastErrorCode int
	var attemptedAnyIP bool

	log.Printf("*****Request Method: %s, Host: %s, Query: %s, Body: %v, URI: %s****", req.Method, req.URL.Host, req.URL.RawQuery, req.Body, req.RequestURI)
	numPrivateEndpoints := GetGlobalPrivateEndpointIPCount()

	// Check if we have any endpoints at all
	if numPrivateEndpoints == 0 {
		return nil, fmt.Errorf("No private endpoint IPs configured")
	}

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
				log.Printf("Updating Private Endpoint:%s connection state from UNHEALTHY->HEALTHY after cooldown at %v (LastChecked: %v)", peIP, time.Now(), entry.LastChecked)
			} else {
				log.Printf("[Counter=%d] Skipping Unhealthy IP %s (still in cooldown) (LastChecked: %v)", idx, peIP, entry.LastChecked)
				// Track that this IP was skipped due to being unhealthy
				lastErrMsg = fmt.Sprintf("IP %s is unhealthy and still in cooldown", peIP)
				lastErrorCode = entry.LastErrCode
				continue
			}
		}

		// Mark that we're attempting to use this IP
		attemptedAnyIP = true

		// Try the same IP up to perIPRetries times before moving on
		for ipAttempt := 0; ipAttempt < rr.perIPRetries; ipAttempt++ {
			// Re-create a fresh clone for each attempt (body-safe for idempotent requests)
			clonedReq := req.Clone(req.Context())

			// Override destination to PE IP:Port and preserve original Host header
			clonedReq.URL.Scheme = req.URL.Scheme
			clonedReq.URL.Host = peIP
			clonedReq.Host = rr.host
			var errCode int
			var errMsg string
			var isRetryableErr bool
			var isS3AccessDeniedErr bool

			log.Printf("[Counter=%d Retry=%d] Sending request to PrivateEndpoint IP: %s (Host header: %s)", idx, ipAttempt, clonedReq.URL.Host, clonedReq.Host)

			resp, err := rr.transport.RoundTrip(clonedReq)
			if err == nil {
				if resp != nil {
					httpS3Err := DetectS3HTTPStatusError(resp)
					if httpS3Err != nil {
						errCode = httpS3Err.HTTPStatusError.GetErrorCode()
						if httpS3Err.S3Error != nil {
							errMsg = httpS3Err.S3Error.Code + ":" + httpS3Err.S3Error.Message
							isRetryableErr = IsRetryableS3Error(httpS3Err.S3Error.Code)
							isRetryableErr = httpS3Err.HTTPStatusError.IsRetryable || isRetryableErr
							isS3AccessDeniedErr = IsAccessDeniedError(httpS3Err.S3Error.Code)
							log.Printf("[Counter=%d Retry=%d] FAILED with S3 Error, Error Code:%d Error Message:%s retryable:%v", idx, ipAttempt, errCode, errMsg, isRetryableErr)
						} else {
							errCode = httpS3Err.HTTPStatusError.GetErrorCode()
							errMsg = httpS3Err.GetErrorMessage()
							isRetryableErr = httpS3Err.HTTPStatusError.IsRetryable
							log.Printf("[Counter=%d Retry=%d] FAILED HTTP Error, Error Code:%d Error Message:%s retryable:%v", idx, ipAttempt, errCode, errMsg, isRetryableErr)
						}

						// Retry if the HTTP or S3 Error is retryable
						if (ipAttempt+1 < rr.perIPRetries) && isRetryableErr {
							log.Printf("[Counter=%d Retry=%d] Retrying same IP %s after %v", idx, ipAttempt, peIP, rr.perIPRetryDelay)
							time.Sleep(rr.perIPRetryDelay)
							continue
						}

						// Mark the Private Endpoint as Unhealthy after detecting non retryable HTTP and S3 critical error
						if isS3AccessDeniedErr {
							if atomic.CompareAndSwapUint32((*uint32)(&entry.ConnectionStatus), uint32(Healthy), uint32(Unhealthy)) {
								entry.MarkUnhealthy(errCode, errMsg)
								log.Printf("Updating Private Endpoint:%s connection state from HEALTHY->UNHEALTHY after error response with Error Code %d ErrorMsg:%s at %v", peIP, entry.LastErrCode, entry.LastErrMsg, entry.LastChecked)
							}
						}

						// For any HTTP/S3 error (retryable or not), we should continue to try other IPs or fail
						lastErrMsg = errMsg
						lastErrorCode = errCode
						break // Exit the per-IP retry loop to try next IP
					} else {
						errCode = 0
						errMsg = ""
						log.Printf("[Counter=%d Retry=%d] SUCCESS using IP %s without error", idx, ipAttempt, peIP)
					}

				} else {
					log.Printf("[Counter=%d Retry=%d] SUCCESS using IP %s", idx, ipAttempt, peIP)
				}

				globalPrivateEndpointIPs[idx].IncrementNumRequests()
				return resp, nil
			}

			// Transport-level failure (err != nil). Capture diagnostics.
			isNetworkRetryableErr := IsRetryableNetworkError(err)
			log.Printf("[Counter=%d Retry=%d] FAILED using IP %s with Error %v", idx, ipAttempt, peIP, err)

			// If resp is non-nil on error, close body to avoid leaks and capture ConnectionStatus for diagnostics.
			if resp != nil {
				httpS3Err := DetectS3HTTPStatusError(resp)
				if httpS3Err != nil {
					errCode = httpS3Err.HTTPStatusError.GetErrorCode()
					if httpS3Err.S3Error != nil {
						errMsg = httpS3Err.S3Error.Code + ":" + httpS3Err.S3Error.Message
						isRetryableErr = IsRetryableS3Error(httpS3Err.S3Error.Code)
						isRetryableErr = httpS3Err.HTTPStatusError.IsRetryable || isRetryableErr
						log.Printf("[Counter=%d Retry=%d] FAILED with S3 Error, Error Code:%d Error Message:%s retryable:%v", idx, ipAttempt, errCode, errMsg, isRetryableErr)
					} else if httpS3Err.HTTPStatusError != nil {
						errCode = httpS3Err.HTTPStatusError.GetErrorCode()
						errMsg = httpS3Err.HTTPStatusError.GetErrorMessage()
						isRetryableErr = httpS3Err.HTTPStatusError.IsRetryable
						log.Printf("[Counter=%d Retry=%d] FAILED HTTP Error, Error Code:%d Error Message:%s retryable:%v", idx, ipAttempt, errCode, errMsg, isRetryableErr)
					}
				} else {
					errCode = 0
					errMsg = ""
				}
			} else {
				// No response - parse error from the error object itself
				errCode = 0
				errMsg = err.Error()
				log.Printf("[Counter=%d Retry=%d] Network error with no response, Error Message:%s retryable:%v", idx, ipAttempt, errMsg, isRetryableErr)
			}

			lastErrMsg = errMsg
			lastErrorCode = errCode

			// If we still have per-IP attempts left, wait and retry the same IP
			if (ipAttempt+1 < rr.perIPRetries) && (isRetryableErr || isNetworkRetryableErr) {
				log.Printf("[Counter=%d Retry=%d] Retrying same IP %s after %v", idx, ipAttempt, peIP, rr.perIPRetryDelay)
				time.Sleep(rr.perIPRetryDelay)
				continue
			}
		}

		// Exhausted per-IP retries: mark the IP unhealthy, record time, update global map and healthy pool,
		// then break to pick another IP (outer loop continues).
		// attempt to mark unhealthy: 0 -> 1
		if (lastErrMsg != "") && atomic.CompareAndSwapUint32((*uint32)(&entry.ConnectionStatus), uint32(Healthy), uint32(Unhealthy)) {
			entry.MarkUnhealthy(lastErrorCode, lastErrMsg)
			log.Printf("Updating Private Endpoint:%s connection state from HEALTHY->UNHEALTHY after error response with Error Code %d ErrorMsg:%s at %v", peIP, entry.LastErrCode, entry.LastErrMsg, entry.LastChecked)
		}
		// continue outer loop to try next IP (if any attempts remain)
	}

	log.Printf("All Private Endpoint IPs have been attempted")

	// All attempts exhausted - return appropriate error message
	if !attemptedAnyIP {
		log.Printf("All private endpoint IPs are unhealthy and in cooldown period. Returning error")
		return nil, fmt.Errorf("All private endpoint IPs are unhealthy and in cooldown period")
	}
	if lastErrMsg == "" {
		log.Printf("All private endpoint IPs are unhealthy and cannot be used. Returning error")
		return nil, fmt.Errorf("All private endpoint IPs are unhealthy and cannot be used")
	}
	log.Printf("Request failed after trying all private endpoint IPs. Returning error")
	return nil, fmt.Errorf("Request failed after trying all private endpoint IPs. Last error from IP %s: %v", peIP, lastErrMsg)
}

// Close cleans up idle connections and stops the periodic refresh goroutine
func (rr *RoundRobinTransport) Close() {
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

	if len(globalPrivateEndpointIPs) == 0 {
		globalPrivateEndpointIPs = make([]*IPEntry, len(ips))
		for i, ip := range ips {
			globalPrivateEndpointIPs[i] = InitializePrivateEndpointIpEntry(ip)
			log.Printf("PrivateEndpointIp:%s is set to Healthy\n ", globalPrivateEndpointIPs[i].IP)
		}
		log.Printf("Number of Private Endpoints:%d set", len(globalPrivateEndpointIPs))
	} else {
		if len(ips) != len(globalPrivateEndpointIPs) {
			log.Printf("Inconsistent non zero number of Private Endpoints:%d %d", len(globalPrivateEndpointIPs), len(ips))
		} else {
			log.Printf("Global Endpoint List is already initial with Private Endpoints:%d ", len(globalPrivateEndpointIPs))
		}
	}
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
