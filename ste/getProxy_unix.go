// +build linux darwin

package ste

import (
	"golang.org/x/net/http/httpproxy"
	"net/http"
	"net/url"
)

func getProxy() func(*url.URL) (*url.URL, error) {
	return httpproxy.FromEnvironment().ProxyFunc()
}

func proxyFromFunc(f func(*url.URL) (*url.URL, error)) func(*http.Request) (*url.URL, error) {
	return func(request *http.Request) (*url.URL, error) {
		return f(request.URL)
	}
}
