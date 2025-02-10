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
	"runtime"
	"sync"
	"syscall"

	"github.com/wastore/keyctl" // forked form "github.com/jsipprell/keyctl", todo: make a release to ensure stability
)

// CredCache manages credential caches.
// Use keyring in Linux OS. Session keyring is chosen,
// the session hooks key should be created since user first login (i.e. by pam).
// So the session is inherited by processes created from login session.
// When user logout, the session keyring is recycled.
type CredCache struct {
	keyName string // the Name of key would be cached in keyring, composed with current UID, in case user su
	lock    sync.Mutex

	key       *keyctl.Key
	isPermSet bool // state used to ensure key has been set permission correctly
}

// NewCredCache creates a cred cache.
func NewCredCache(options CredCacheOptions) *CredCache {
	c := &CredCache{
		keyName: options.KeyName,
	}

	runtime.SetFinalizer(c, func(CredCache *CredCache) {
		if !CredCache.isPermSet && CredCache.key != nil {
			// Indicates Permission is by default ProcessAll, which is not safe and try to recycle the key.
			// Note: there is no method to grant permission during adding key,
			// this mechanism is added to ensure key exists only if its permission is set properly.
			unlinkErr := CredCache.key.Unlink()
			if unlinkErr != nil {
				panic(errors.New("failed to set permission, and cannot unlink key, please logout current login session for safety consideration"))
			}
		}
	})

	return c
}

// HasCachedToken returns if there is cached token for current executing user.
func (c *CredCache) HasCachedToken() (bool, error) {
	c.lock.Lock()
	has, err := c.hasCachedTokenInternal()
	c.lock.Unlock()
	return has, err
}

// RemoveCachedToken deletes the cached token.
func (c *CredCache) RemoveCachedToken() error {
	c.lock.Lock()
	err := c.removeCachedTokenInternal()
	c.lock.Unlock()
	return err
}

// SaveToken saves an oauth token.
func (c *CredCache) SaveToken(token OAuthTokenInfo) error {
	c.lock.Lock()
	err := c.saveTokenInternal(token)
	c.lock.Unlock()
	return err
}

// LoadToken gets the cached oauth token.
func (c *CredCache) LoadToken() (*OAuthTokenInfo, error) {
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

// hasCachedTokenInternal returns if there is cached token in session key ring for current login session.
func (c *CredCache) hasCachedTokenInternal() (bool, error) {
	keyring, err := keyctl.SessionKeyring()
	if err != nil {
		return false, err
	}
	_, err = keyring.Search(c.keyName)
	// TODO: better logging what's cause for token caching failure
	// e.g. Error message: "required key not available"
	// the source library could be updated to use keyctl_search
	if err != nil {
		return false, err
	} else {
		return true, nil
	}
}

// removeCachedTokenInternal deletes the cached token in session key ring.
func (c *CredCache) removeCachedTokenInternal() error {
	keyring, err := keyctl.SessionKeyring()
	if err != nil {
		return fmt.Errorf("failed to get keyring during removing cached token, %v", err)
	}
	key, err := keyring.Search(c.keyName)
	if err != nil {
		if err == syscall.ENOKEY {
			return fmt.Errorf("no cached token found for current user")
		}
		return fmt.Errorf("no cached token found for current user, %v", err)
	}
	err = key.Unlink()
	if err != nil {
		return fmt.Errorf("failed to remove cached token, %v", err)
	}

	c.isPermSet = false
	c.key = nil

	return nil
}

// saveTokenInternal saves an oauth token in session key ring.
func (c *CredCache) saveTokenInternal(token OAuthTokenInfo) error {
	c.isPermSet = false
	c.key = nil

	b, err := token.toJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal during saving token, %v", err)
	}
	keyring, err := keyctl.SessionKeyring()
	if err != nil {
		return fmt.Errorf("failed to get keyring during saving token, %v", err)
	}
	k, err := keyring.Add(c.keyName, b)
	if err != nil {
		return fmt.Errorf("failed to save key, %v", err)
	}
	c.key = k

	// Set permissions to only current user.
	err = keyctl.SetPerm(k, keyctl.PermUserAll)
	if err != nil {
		// which indicates Permission is by default ProcessAll
		unlinkErr := k.Unlink()
		if unlinkErr != nil {
			panic(errors.New("failed to set permission, and cannot unlink key, please logout current login session for safety consideration"))
		}
		return fmt.Errorf("failed to set permission for cached token, %v", err)
	}

	c.isPermSet = true

	return nil
}

// loadTokenInternal gets an oauth token from session key ring.
func (c *CredCache) loadTokenInternal() (*OAuthTokenInfo, error) {
	keyring, err := keyctl.SessionKeyring()
	if err != nil {
		return nil, fmt.Errorf("failed to get keyring during loading token, %v", err)
	}
	key, err := keyring.Search(c.keyName)
	if err != nil {
		return nil, fmt.Errorf("failed to find cached token during loading token, %v", err)
	}
	data, err := key.Get()
	if err != nil {
		return nil, fmt.Errorf("failed to load token, %v", err)
	}
	token, err := jsonToTokenInfo(data)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal token during loading key, %v", err)
	}
	return token, nil
}
