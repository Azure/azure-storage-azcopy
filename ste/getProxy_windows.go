package ste

import (
	"golang.org/x/net/http/httpproxy"
	"golang.org/x/sys/windows/registry"
	"net/http"
	"net/url"
)

func getProxy() func(*http.Request) (*url.URL, error) {
	if prox := httpproxy.FromEnvironment(); prox.HTTPProxy != "" || prox.HTTPSProxy != "" || prox.NoProxy != "" {
		return http.ProxyFromEnvironment
	}

	key, err := registry.OpenKey(registry.CURRENT_USER, `Software\Microsoft\Windows\CurrentVersion\Internet Settings`, registry.QUERY_VALUE)
	if err != nil {
		return http.ProxyFromEnvironment
	}
	defer key.Close()

	strURL, _, err := key.GetStringValue("ProxyServer")
	if err != nil {
		return http.ProxyFromEnvironment
	}

	proxyURL, err := url.Parse(strURL)
	if err != nil {
		return http.ProxyFromEnvironment
	}

	pURL := http.ProxyURL(proxyURL)

	return pURL
}
