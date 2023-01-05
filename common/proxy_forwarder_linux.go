//go:build !windows
// +build !windows

package common

import "net/http"

// GetProxyFunc is a forwarder for the OS-Exclusive proxyMiddleman_os.go files
func GetProxyFunc() func(*http.Request) (*url.URL, error) {
	return http.ProxyFromEnvironment()
}
