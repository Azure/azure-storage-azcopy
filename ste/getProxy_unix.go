// +build linux darwin

package ste

import (
	"net/http"
	"net/url"
)

func getProxy() func(*http.Request) (*url.URL, error) {
	return http.ProxyFromEnvironment
}
