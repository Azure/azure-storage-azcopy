package common

import (
	"github.com/mattn/go-ieproxy"
	"golang.org/x/net/http/httpproxy"
	"net/http"
	"net/url"
)

func proxyMiddleman() func(req *http.Request) (i *url.URL, e error) {
	//Get the proxy from mattn/go-ieproxy
	conf := ieproxy.GetConf()

	if conf.Automatic.Active {
		//If automatic proxy obtaining is specified
		return func(req *http.Request) (i *url.URL, e error) {
			return &url.URL{Host: conf.Automatic.FindProxyForURL(req.URL.String())}, nil
		}
	} else if conf.Static.Active {
		//If static proxy obtaining is specified
		prox := httpproxy.Config{
			HTTPSProxy: mapFallback("https", "", conf.Static.Protocols),
			HTTPProxy:  mapFallback("http", "", conf.Static.Protocols),
			NoProxy:    conf.Static.NoProxy,
		}

		return func(req *http.Request) (i *url.URL, e error) {
			return prox.ProxyFunc()(req.URL)
		}
	} else {
		//Final fallthrough case; use the environment variables.
		return http.ProxyFromEnvironment
	}
}

func mapFallback(oKey, fbKey string, m map[string]string) string {
	if v, ok := m[oKey]; ok {
		return v
	} else {
		return m[fbKey]
	}
}
