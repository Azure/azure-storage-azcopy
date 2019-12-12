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
	chk "gopkg.in/check.v1"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type proxyLookupCacheSuite struct{}

var _ = chk.Suite(&proxyLookupCacheSuite{})

func (s *proxyLookupCacheSuite) TestCacheIsUsed(c *chk.C) {
	fakeMu := &sync.Mutex{} // avoids race condition in test code
	var fakeResult *url.URL
	var fakeError error

	pc := &proxyLookupCache{
		m:             &sync.Map{},
		lookupTimeout: time.Minute,
		lookupLock:    &sync.Mutex{},
		lookupMethod: func(req *http.Request) (*url.URL, error) {
			fakeMu.Lock()
			defer fakeMu.Unlock()
			return fakeResult, fakeError
		},
	}

	// fill the cache with 3 entries, one of which has an error
	fakeMu.Lock()
	fakeResult, fakeError = url.Parse("http://fooproxy")
	fakeMu.Unlock()
	fooRequest, _ := http.NewRequest("GET", "http://foo.com/a", nil)
	fooResult1, err := pc.getProxy(fooRequest)
	c.Check(err, chk.IsNil)
	c.Check(fooResult1.String(), chk.Equals, "http://fooproxy")

	fakeMu.Lock()
	fakeResult, fakeError = url.Parse("http://barproxy")
	fakeMu.Unlock()
	barRequest, _ := http.NewRequest("GET", "http://bar.com/a", nil)
	barResult1, err := pc.getProxy(barRequest)
	c.Check(err, chk.IsNil)
	c.Check(barResult1.String(), chk.Equals, "http://barproxy")

	fakeMu.Lock()
	fakeResult, fakeError = url.Parse("http://this will give a parsing error")
	fakeMu.Unlock()
	erroringRequest, _ := http.NewRequest("GET", "http://willerror.com/a", nil)
	_, expectedErr := pc.getProxy(erroringRequest)
	c.Check(expectedErr, chk.NotNil)

	// set dummy values for next lookup, so we can be sure that lookups don't happen (i.e. we don't get these values, so we know we hit the cache)
	fakeMu.Lock()
	fakeResult, _ = url.Parse("http://thisShouldNeverBeReturnedBecauseResultsAreAlreadyCached")
	fakeMu.Unlock()
	fakeError = nil

	// lookup URLs with same host portion, but different paths. Expect cache hits.
	fooRequest, _ = http.NewRequest("GET", "http://foo.com/differentPathFromBefore", nil)
	fooResult2, err := pc.getProxy(fooRequest)
	c.Check(err, chk.IsNil)
	c.Check(fooResult2.String(), chk.Equals, fooResult1.String())

	barRequest, _ = http.NewRequest("GET", "http://bar.com/differentPathFromBefore", nil)
	barResult2, err := pc.getProxy(barRequest)
	c.Check(err, chk.IsNil)
	c.Check(barResult2.String(), chk.Equals, barResult1.String())

	erroringRequest, _ = http.NewRequest("GET", "http://willerror.com/differentPathFromBefore", nil)
	_, expectedErr = pc.getProxy(erroringRequest)
	c.Check(expectedErr, chk.NotNil)
}

func (s *proxyLookupCacheSuite) TestCacheEntriesGetRefreshed(c *chk.C) {
	fakeMu := &sync.Mutex{} // avoids race condition in test code
	var fakeResult *url.URL
	var fakeError error

	pc := &proxyLookupCache{
		m:               &sync.Map{},
		lookupLock:      &sync.Mutex{},
		refreshInterval: time.Second, // much shorter than normal, for testing
		lookupTimeout:   time.Minute,
		lookupMethod: func(req *http.Request) (*url.URL, error) {
			fakeMu.Lock()
			defer fakeMu.Unlock()
			return fakeResult, fakeError
		},
	}

	// load the cache
	fakeMu.Lock()
	fakeResult, fakeError = url.Parse("http://fooproxy")
	fakeMu.Unlock()
	fooRequest, _ := http.NewRequest("GET", "http://foo.com/a", nil)
	fooResult1, err := pc.getProxy(fooRequest)
	c.Check(err, chk.IsNil)
	c.Check(fooResult1.String(), chk.Equals, "http://fooproxy")

	// prime the refresh to actually produce a change
	fakeMu.Lock()
	fakeResult, fakeError = url.Parse("http://updatedFooProxy")
	fakeMu.Unlock()

	// wait while refresh runs
	time.Sleep(time.Second * 2)

	// read from cache, and check we get the update result
	fooResult2, err := pc.getProxy(fooRequest)
	c.Check(err, chk.IsNil)
	c.Check(fooResult2.String(), chk.Equals, "http://updatedFooProxy")
}

func (s *proxyLookupCacheSuite) TestUseOfLookupMethodHasTimout(c *chk.C) {
	pc := &proxyLookupCache{
		m:             &sync.Map{},
		lookupLock:    &sync.Mutex{},
		lookupTimeout: time.Second, // very short, since this is the timeout we are testing in this test
		lookupMethod: func(req *http.Request) (*url.URL, error) {
			time.Sleep(time.Hour * 24) // "never" return, since we want the timeout to take effect
			return nil, nil
		},
	}

	fooRequest, _ := http.NewRequest("GET", "http://foo.com/a", nil)
	tuple := pc.getProxyNoCache(fooRequest)
	c.Check(tuple.err, chk.Equals, ProxyLookupTimeoutError)
}
