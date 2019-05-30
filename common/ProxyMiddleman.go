package common

import (
	"net/http"
	"net/url"
)

//ProxyMiddleman is a forwarder for the OS-Exclusive proxyMiddleman_os.go files
func ProxyMiddleman() func(*http.Request) (*url.URL, error) {
	return proxyMiddleman()
}
