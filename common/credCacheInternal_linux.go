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
	"errors"
	"fmt"
	"sync"
)

// CredCacheInternalIntegration manages credential caches with Gnome keyring.
// Note: This should be only used for internal integration.
type CredCacheInternalIntegration struct {
	accountName string
	serviceName string
	keyName     string
	keyring     gnomeKeyring
	lock        sync.Mutex
}

// NewCredCacheInternalIntegration creates a cred cache.
func NewCredCacheInternalIntegration(options CredCacheOptions) *CredCacheInternalIntegration {
	return &CredCacheInternalIntegration{
		keyName:     options.KeyName,
		serviceName: options.ServiceName,
		accountName: options.AccountName,
		keyring:     gnomeKeyring{},
	}
}

// HasCachedToken returns if there is cached token for current executing user.
func (c *CredCacheInternalIntegration) HasCachedToken() (bool, error) {
	c.lock.Lock()
	has, err := c.hasCachedTokenInternal()
	c.lock.Unlock()
	return has, err
}

// RemoveCachedToken deletes the cached token.
func (c *CredCacheInternalIntegration) RemoveCachedToken() error {
	c.lock.Lock()
	err := c.removeCachedTokenInternal()
	c.lock.Unlock()
	return err
}

// SaveToken saves an oauth token.
func (c *CredCacheInternalIntegration) SaveToken(token OAuthTokenInfo) error {
	c.lock.Lock()
	err := c.saveTokenInternal(token)
	c.lock.Unlock()
	return err
}

// LoadToken gets the cached oauth token.
func (c *CredCacheInternalIntegration) LoadToken() (*OAuthTokenInfo, error) {
	c.lock.Lock()
	token, err := c.loadTokenInternal()
	c.lock.Unlock()
	return token, err
}

///////////////////////////////////////////////////////////////////////////////////////////////
// This internal method pattern is applied to avoid defer locks.
// The reason is:
// We use locks to protect shared state from being accessed from multiple threads/goroutines at the same time.
// If a bug is in this method that causes a panic,
// then the defer will unlock another thread/goroutine allowing it to access the shared state.
// BUT, if a panic happened, the shared state is hard to be decide whether in a good or corrupted state.
// So currently let the other threads/goroutines hang forever instead of letting them access the potentially corrupted shared state.
// Once having bad state, more bad state gets injected into app and figuring out how it happened and how to recover from it is near impossible.
// On the other hand, hanging threads is MUCH easier to detect and devs can fix the bug in code to make sure that the panic doesn't happen in the first place.
///////////////////////////////////////////////////////////////////////////////////////////////

// hasCachedTokenInternal returns if there is cached token in token manager.
func (c *CredCacheInternalIntegration) hasCachedTokenInternal() (bool, error) {
	if _, err := c.keyring.Get(c.serviceName, c.accountName); err != nil { //nolint:staticcheck
		return false, fmt.Errorf("failed to find token from gnome keyring, %v", err)
	}

	return true, nil
}

// removeCachedTokenInternal deletes all the cached token.
func (c *CredCacheInternalIntegration) removeCachedTokenInternal() error {
	// By design, not useful currently.
	return errors.New("Not implemented")
}

// loadTokenInternal restores a Token object from file cache.
//
//nolint:staticcheck
func (c *CredCacheInternalIntegration) loadTokenInternal() (*OAuthTokenInfo, error) {
	data, err := c.keyring.Get(c.serviceName, c.accountName)
	if err != nil {
		return nil, fmt.Errorf("failed to get token from gnome keyring, %v", err)
	}

	token, err := jsonToTokenInfo([]byte(data))
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal token during loading token, %v", err)
	}
	return token, nil
}

// saveTokenInternal persists an oauth token on disk.
func (c *CredCacheInternalIntegration) saveTokenInternal(token OAuthTokenInfo) error {
	// By design, not useful currently.
	return errors.New("Not implemented")
}
