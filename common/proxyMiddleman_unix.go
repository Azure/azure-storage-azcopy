//+build darwin unix linux

package common

import (
	"net/http"
	"net/url"
)

func proxyMiddleman() func(req *http.Request) (i *url.URL, e error) {
	return http.ProxyFromEnvironment
}
