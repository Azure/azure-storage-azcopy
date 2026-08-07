package cred

import (
	"net"
	"net/http"
	"time"
)

// Straight lifted up out of the old oauthtokenmanager

func newAzcopyHTTPClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			//Proxy: GlobalProxyLookup, // todo; fixme!
			// We use Dial instead of DialContext as DialContext has been reported to cause slower performance.
			Dial /*Context*/ : (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 10 * time.Second,
				DualStack: true,
			}).Dial, /*Context*/
			MaxIdleConns:           0, // No limit
			MaxIdleConnsPerHost:    1000,
			IdleConnTimeout:        180 * time.Second,
			TLSHandshakeTimeout:    10 * time.Second,
			ExpectContinueTimeout:  1 * time.Second,
			DisableKeepAlives:      false,
			DisableCompression:     true,
			MaxResponseHeaderBytes: 0,
			// ResponseHeaderTimeout:  time.Duration{},
			// ExpectContinueTimeout:  time.Duration{},
		},
	}
}
