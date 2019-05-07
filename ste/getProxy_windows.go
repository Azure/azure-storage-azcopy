package ste

import (
	"github.com/Azure/azure-storage-azcopy/common"
	"golang.org/x/net/http/httpproxy"
	"golang.org/x/sys/windows/registry"
	"net/http"
	"net/url"
	"strings"
)

//TODO: Make this return httpproxy.config, make function to return properly
func getProxy() func(*url.URL) (*url.URL, error) {
	prox := httpproxy.FromEnvironment()

	key, err := registry.OpenKey(registry.CURRENT_USER, `Software\Microsoft\Windows\CurrentVersion\Internet Settings`, registry.QUERY_VALUE)
	if err != nil {
		return prox.ProxyFunc()
	}
	defer key.Close()

	proxyEnableRegKey, _, err := key.GetIntegerValue("ProxyEnable")
	if err != nil {
		return prox.ProxyFunc()
	}

	if prox.HTTPProxy != "" || prox.HTTPSProxy != "" || prox.NoProxy != "" || proxyEnableRegKey == 0 {
		return prox.ProxyFunc()
	}

	strURL, _, err := key.GetStringValue("ProxyServer")
	if err != nil {
		return prox.ProxyFunc()
	}

	override, _, err := key.GetStringValue("ProxyOverride")
	if err != nil {
		return prox.ProxyFunc()
	}

	proxyURL, err := url.Parse(strURL)
	if err != nil {
		return prox.ProxyFunc()
	}

	cfg := httpproxy.Config{
		HTTPSProxy: common.IffString(strings.HasPrefix(proxyURL.String(), "https"), proxyURL.String(), ""),
		HTTPProxy:  common.IffString(strings.HasPrefix(proxyURL.String(), "https"), "", proxyURL.String()),
		NoProxy:    override,
	}

	return cfg.ProxyFunc()
}

func proxyFromFunc(f func(*url.URL) (*url.URL, error)) func(*http.Request) (*url.URL, error) {
	return func(request *http.Request) (*url.URL, error) {
		return f(request.URL)
	}
}
