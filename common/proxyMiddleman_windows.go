package common

import (
	"github.com/mattn/go-ieproxy"
	"golang.org/x/net/http/httpproxy"
	"net/http"
	"net/url"
)

func proxyMiddleman() func(req *http.Request) (i *url.URL, e error) {
	conf := ieproxy.GetConf()

	if conf.Automatic.Active {
		return func(req *http.Request) (i *url.URL, e error) {
			return url.Parse(conf.Automatic.FindProxyForURL(req.URL.String()))
		}
	} else if conf.Static.Active {
		prox := httpproxy.Config{
			HTTPSProxy: conf.Static.Protocols["https"],
			HTTPProxy:  conf.Static.Protocols["http"],
			NoProxy:    conf.Static.NoProxy,
		}

		return func(req *http.Request) (i *url.URL, e error) {
			return prox.ProxyFunc()(req.URL)
		}
	} else {
		return http.ProxyFromEnvironment
	}
}
