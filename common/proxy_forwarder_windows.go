//go:build windows
// +build windows

package common

import (
	"net/http"
	"net/url"

	"github.com/mattn/go-ieproxy"
)

func GetProxyFunc() func(*http.Request) (*url.URL, error) {
	return ieproxy.GetProxyFunc()
}
