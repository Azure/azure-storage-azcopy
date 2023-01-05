package common

import (
	"github.com/mattn/go-ieproxy"
	"net/http"
	"net/url"
)

func GetProxyFunc() func(*http.Request) (*url.URL, error) {
	return ieproxy.GetProxyFunc()
}
