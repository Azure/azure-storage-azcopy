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
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type ProxyLookupFunc func(req *http.Request) (*url.URL, error) // signature of normal Transport.Proxy lookup

var GlobalProxyLookup ProxyLookupFunc

func init() {
	c := &proxyLookupCache{
		m:               &sync.Map{},
		refreshInterval: time.Minute * 5, // this is plenty, given the usual retry policies in AzCopy span a much longer time period in the total retry sequence
		lookupTimeout:   time.Minute,     // equals the documented max allowable execution time for WinHttpGetProxyForUrl
		lookupLock:      &sync.Mutex{},
		lookupMethod:    GetProxyFunc(),
	}

	ev := GetEnvironmentVariable(EEnvironmentVariable.CacheProxyLookup())
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
// It's here, rather than contributed to mattn.go-ieproxy because it's not general-purpose.
// In particular, it assumes that there will not be lots and lots of different hosts in the cache,
// and so it has no ability to clear them or reduce the size of the cache, and it runs one
// permanent GR per cache entry. That assumption makes sense in AzCopy, but is not correct in general (e.g.
// if used in something with usage patterns like a web browser).
// TODO: should we one day find a better solution, so it can be contributed to mattn.go-ieproxy instead of done here?
//
//	or maybe just the code in getProxyNoCache could be contributed there?
//
// TODO: consider that one consequence of the current lack of integration with mattn.go-ieproxy is that pipelines created by
//
//	pipeline.NewPipeline don't use proxyLookupCache at all.  However, that only affects the enumeration portion of our code,
//	for Azure Files and ADLS Gen2. The issues that proxyLookupCache solves have not been reported there. The issues matter in
//	the STE, where request counts are much higher (and there, we always do use this cache, because we make our own pipelines).
type proxyLookupCache struct {
	m               *sync.Map // is optimized for caches that only grow (as is the case here)
	refreshInterval time.Duration
	lookupTimeout   time.Duration
	lookupLock      *sync.Mutex
	lookupMethod    ProxyLookupFunc
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
	case <-time.After(c.lookupTimeout):
		return proxyLookupResult{nil, ProxyLookupTimeoutError}
		// Note: in testing the the real app, this code path wasn't triggered. Not sure if its just luck that in many Win10 test runs,
		// with hundreds of thousands of files each, this didn't trigger - even though the underlying issue did trigger
		// on about 25% of similar test runs prior to this code being added. Maybe just luck, or maybe something about spinning up
		// the separate goroutine here as "magically" prevented the issue.
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
	// Because the function is potentially expensive(ish) and because we only want to kick off one refresh GR per key,
	// we use a lock here
	c.lookupLock.Lock()
	defer c.lookupLock.Unlock()

	if value, ok = c.mapLoad(key); !ok {
		value = c.getProxyNoCache(req)
		c.m.Store(key, value)

		if value.err == nil && value.url != nil {
			// print out a friendly message to let the cx know we've detected a proxy
			// only do it when we first cached the result so that the message is not printed for every request
			GetLifecycleMgr().OnInfo(fmt.Sprintf("Proxy detected: %s -> %s", key.String(), value.url.String()))
		}

		go c.endlessTimedRefresh(key, req)
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

// timedRefresh runs an endless loop, refreshing the given key
// on a coarse-grained interval.  This is in case something changes in the user's network
// configuration. E.g. they switch between wifi and wired if on a laptop.  Shouldn't be common
// with AzCopy, but could happen so we may as well cater for it.
func (c *proxyLookupCache) endlessTimedRefresh(key url.URL, representativeFullRequest *http.Request) {
	if c.refreshInterval == 0 {
		return
	}

	for {
		time.Sleep(c.refreshInterval)

		// compare old value with new value
		// old value must already exist for this routine to be running
		oldValue, _ := c.mapLoad(key)

		newValue := c.getProxyNoCache(representativeFullRequest)
		c.m.Store(key, newValue)

		if newValue.err == nil && newValue.url != nil {
			// print out a friendly message to let the cx know we've detected a changed proxy if:
			// 1. the old value had an error and new value doesn't
			// 2. the proxy url has changed
			if oldValue.err != nil || oldValue.url == nil || oldValue.url.String() != newValue.url.String() {
				GetLifecycleMgr().OnInfo(fmt.Sprintf("Proxy detected: %s -> %s", key.String(), newValue.url.String()))
			}
		}
	}
}
