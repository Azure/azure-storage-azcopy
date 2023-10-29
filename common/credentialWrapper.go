// Copyright © 2017 Microsoft <wastore@microsoft.com>
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
	"context"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
)

// credWrapper wraps cred so that the frequent calls to GetToken are
// responed by cache in this struct, reducing the costly operation on cred.
// This is okay because we always request token with same scope.
type credWrapper struct {
	cred   azcore.TokenCredential
	rwLock sync.RWMutex

	// These fields should be protected
	token           azcore.AccessToken
	nextRefreshTime time.Time
}

func NewCredWrapper(cred azcore.TokenCredential) azcore.TokenCredential {
	return &credWrapper{cred: cred, nextRefreshTime: time.Now().Add(-time.Minute)}
}

func (c *credWrapper) GetToken(ctx context.Context, options policy.TokenRequestOptions) (azcore.AccessToken, error) {
	c.rwLock.RLock()

	if time.Until(c.nextRefreshTime) <= 0 {
		//Give up read lock first.
		c.rwLock.RUnlock()

		c.rwLock.Lock()
		defer c.rwLock.Unlock()

		// If somebody else has refreshed, just return
		if time.Until(c.nextRefreshTime) > 0 {
			return c.token, nil
		}

		newToken, err := c.cred.GetToken(ctx, options)
		if err != nil {
			return azcore.AccessToken{}, nil
		}

		c.token = newToken
		c.nextRefreshTime = time.Now().Add(time.Until(c.token.ExpiresOn) / time.Duration(2))

		return newToken, nil
	}

	defer c.rwLock.RUnlock()
	return c.token, nil
}
