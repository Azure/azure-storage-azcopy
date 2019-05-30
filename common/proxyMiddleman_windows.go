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
			return url.Parse(conf.Automatic.FindProxyForURL(req.URL.String()))
		}
	} else if conf.Static.Active {
		//If static proxy obtaining is specified
		prox := httpproxy.Config{
			HTTPSProxy: conf.Static.Protocols["https"],
			HTTPProxy:  conf.Static.Protocols["http"],
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
