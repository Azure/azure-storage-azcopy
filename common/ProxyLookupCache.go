// Copyright © Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package common

import (
	"errors"
	"github.com/mattn/go-ieproxy"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

var GlobalProxyLookup func(*http.Request) (*url.URL, error)

func init() {
	c := &proxyLookupCache{
		m:               &sync.Map{},
		refreshInterval: time.Minute * 5, // this is plenty, given the usual retry policies in AzCopy
		lookupLock:      &sync.Mutex{},
		lookupMethod:    ieproxy.GetProxyFunc(),
	}

	ev := GetLifecycleMgr().GetEnvironmentVariable(EEnvironmentVariable.CacheProxyLookup())
	if strings.ToLower(ev) == "true" {
		GlobalProxyLookup = c.getProxy
	} else {
		// Use full URL in the lookup, and don't cache the result
		// In theory, WinHttpGetProxyForUrl can take the path portion of the URL into account,
		// to give a different proxy server depending on the path. That's only possible if
		// there's a lookup done for each request.
		// In practice, we expect very few users will need this.
		GlobalProxyLookup = func(req *http.Request) (*url.URL, error) {
			v := c.getProxyNoCache(req)
			return v.url, v.err
		}
	}
}

var ProxyLookupTimeoutError = errors.New("proxy lookup timed out")

type proxyLookupResult struct {
	url *url.URL
	err error
}

// proxyLookupCache caches the result of proxy lookups
// It's here, rather than contributed to mattn.ieproxy because it's not general-purpose.
// In particular, it assumes that there will not be lots and lots of different hosts in the cache,
// and so it has no ability to clear them or reduce the size of the cache. That assumption is true
// in AzCopy, but not true in general.
// TODO: should we find a better solution, so it can be contributed to mattn.ieproxy instead of done here
type proxyLookupCache struct {
	m               *sync.Map // is optimized for caches that only grow (as is the case here)
	refreshInterval time.Duration
	lookupLock      *sync.Mutex
	lookupMethod    func(r *http.Request) (*url.URL, error) // signature of normal Transport.Proxy lookup
}

func (c *proxyLookupCache) getProxyNoCache(req *http.Request) proxyLookupResult {
	// Look up in background, so we can set timeout
	// We do this since we have observed cases that don't return on Windows 10, presumably due to OS issue as described here:
	// https://developercommunity.visualstudio.com/content/problem/282756/intermittent-and-indefinite-wcf-hang-blocking-requ.html
	ch := make(chan proxyLookupResult)
	go func() {
		u, err := c.lookupMethod(req) // typically configured to call the result of ieproxy.GetProxyFunc
		ch <- proxyLookupResult{u, err}
	}()

	select {
	case v := <-ch:
		return v
	case <-time.After(time.Minute):
		return proxyLookupResult{nil, ProxyLookupTimeoutError}
	}
}

// getProxy returns the cached proxy, or looks it up if its not already cached
func (c *proxyLookupCache) getProxy(req *http.Request) (*url.URL, error) {
	var value proxyLookupResult
	var ok bool

	// key is the scheme+host portion of the URL
	key := url.URL{
		Scheme: req.URL.Scheme,
		User:   req.URL.User,
		Host:   req.URL.Host,
	}

	// if we've got it return it
	if value, ok = c.mapLoad(key); ok {
		return value.url, value.err
	}

	// else, look it up with the (potentially expensive) lookup function
	// Because the function is potentially expensive (and because we've see very rare lockups in it,
	// as per https://developercommunity.visualstudio.com/content/problem/282756/intermittent-and-indefinite-wcf-hang-blocking-requ.html)
	// only let one thread do the lookup
	c.lookupLock.Lock()
	defer c.lookupLock.Unlock()

	if value, ok = c.mapLoad(key); !ok { // to avoid unnecessary extra lookups
		value = c.getProxyNoCache(req)
		c.m.Store(key, value)
	}

	return value.url, value.err
}

func (c *proxyLookupCache) mapLoad(key url.URL) (proxyLookupResult, bool) {
	value, ok := c.m.Load(key)
	if ok {
		return value.(proxyLookupResult), true
	} else {
		return proxyLookupResult{}, false
	}

}

//TODO: consider unit tests for the above
