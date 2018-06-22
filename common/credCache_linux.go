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
	"os"
	"runtime"
	"strconv"
	"sync"
	// todo: make a fork on this repo, and use the forked repo
	"github.com/jsipprell/keyctl"
)

// CredCache manages credential caches.
// Use keyring in Linux OS. Session keyring is choosed,
// the session hooks key should be created since user first login (i.e. by pam).
// So the session is inherited by processes created from login session.
// When user logout, the session keyring is recycled.
type CredCache struct {
	state          string // reserved for use
	cachedTokenKey string // the name of key would be cached in keyring, composed with current UID, in case user su
	lock           sync.Mutex

	key       *keyctl.Key
	isPermSet bool // state used to ensure key has been set permission correctly
}

const cachedTokenKeySuffix = "AzCopyOAuthTokenCache"

// NewCredCache creates a cred cache.
func NewCredCache(state string) *CredCache {
	c := &CredCache{
		state:          state,
		cachedTokenKey: strconv.Itoa(os.Geteuid()) + cachedTokenKeySuffix,
	}

	runtime.SetFinalizer(c, func(CredCache *CredCache) {
		if CredCache.isPermSet == false && CredCache.key != nil {
			// Indicates Permission is by default ProcessAll, which is not safe and try to recycle the key.
			// Note: there is no method to grant permission during adding key,
			// this mechanism is added to ensure key exists only if its permission is set properly.
			unlinkErr := CredCache.key.Unlink()
			if unlinkErr != nil {
				panic(errors.New("Fail to set key permission, and cannot recycle key, please logout current session for safety consideration."))
			}
		}
	})

	return c
}

// HasCachedToken returns if there is cached token in session key ring for current login session.
func (c *CredCache) HasCachedToken() (bool, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	keyring, err := keyctl.SessionKeyring()
	if err != nil {
		return false, err
	}
	_, err = keyring.Search(c.cachedTokenKey)
	// TODO: better logging what's cause the has cache token failure
	// e.g. Error message: "required key not available"
	if err != nil {
		return false, err
	} else {
		return true, nil
	}
}

// RemoveCachedToken delete the cached token in session key ring.
func (c *CredCache) RemoveCachedToken() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	keyring, err := keyctl.SessionKeyring()
	if err != nil {
		return fmt.Errorf("fail to get keyring during removing cached token, %v", err)
	}
	key, err := keyring.Search(c.cachedTokenKey)
	if err != nil {
		return fmt.Errorf("fail to find cached token, %v", err)
	}
	err = key.Unlink()
	if err != nil {
		return fmt.Errof("fail to remove cached token, %v", err)
	}

	c.isPermSet = false
	c.key = nil

	return nil
}

// SaveToken saves an oauth token in session key ring.
func (c *CredCache) SaveToken(token OAuthTokenInfo) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.isPermSet = false
	c.key = nil

	b, err := token.ToJSON()
	if err != nil {
		return fmt.Errorf("fail to marshal during save token, %v", err)
	}
	keyring, err := keyctl.SessionKeyring()
	if err != nil {
		return fmt.Errorf("fail to get keyring during save token, %v", err)
	}
	k, err := keyring.Add(c.cachedTokenKey, b)
	if err != nil {
		return fmt.Errorf("fail to save key, %v", err)
	}
	c.key = k

	// Set permissions to only current user.
	err = keyctl.SetPerm(k, keyctl.PermUserAll)
	if err != nil {
		// which indicates Permission is by default ProcessAll
		unlinkErr := k.Unlink()
		if unlinkErr != nil {
			panic(errors.New("fail to set key permission, and cannot recycle key, please logout current session for safety consideration."))
		}
		return fmt.Errorf("fail to set permission for key, %v", err)
	}

	c.isPermSet = true

	return nil
}

// LoadToken gets an oauth token from session key ring.
func (c *CredCache) LoadToken() (*OAuthTokenInfo, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	keyring, err := keyctl.SessionKeyring()
	if err != nil {
		return nil, fmt.Errorf("fail to get key, %v", err)
	}
	key, err := keyring.Search(c.cachedTokenKey)
	if err != nil {
		return nil, fmt.Errorf("fail to get key, %v", err)
	}
	data, err := key.Get()
	if err != nil {
		return nil, fmt.Errorf("fail to get key, %v", err)
	}
	token, err := JSONToTokenInfo(data)
	if err != nil {
		return nil, fmt.Errorf("fail to unmarshal token during get key, %v", err)
	}
	return token, nil
}
