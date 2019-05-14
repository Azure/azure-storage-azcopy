package common

import (
	"net/http"
	"net/url"
)

func ProxyMiddleman() func(*http.Request) (*url.URL, error) {
	return proxyMiddleman()
}
