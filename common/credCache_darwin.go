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
	"github.com/keybase/go-keychain"
	"sync"
)

// For SSH environment, user need unlock login keychain once, to enable AzCopy to Add/Update/Retrieve/Delete key.
// For Mac UI or terminal environment, unlock is not mandatory.
// For more details about Apple keychain support: https://developer.apple.com/documentation/security/keychain_services?language=objc
// CredCache manages credential caches.
type CredCache struct {
	serviceName string
	accountName string
	lock        sync.Mutex

	// KeyChain settings
	kcSecClass       keychain.SecClass
	kcSynchronizable keychain.Synchronizable
	kcAccessible     keychain.Accessible
	// kcAccessGroup, bypass AccessGroup setting, as Azcopy is a green software don't need install currently.
	// for more details, refer to https://developer.apple.com/documentation/security/ksecattraccessgroup?language=objc
}

func NewCredCache(options CredCacheOptions) *CredCache {
	return &CredCache{
		serviceName: options.ServiceName,
		accountName: options.AccountName,
		kcSecClass:  keychain.SecClassGenericPassword,
		// do not synchronize this through ICloud.
		// for more details, refer to https://developer.apple.com/documentation/security/ksecattrsynchronizable?language=objc
		kcSynchronizable: keychain.SynchronizableNo,
		// using AccessibleAfterFirstUnlockThisDeviceOnly, so user can login once, and execute commands silently.
		// for more details, refer to https://developer.apple.com/documentation/security/ksecattraccessible?language=objc
		// and https://developer.apple.com/documentation/security/keychain_services/keychain_items/restricting_keychain_item_accessibility?language=objc
		kcAccessible: keychain.AccessibleAfterFirstUnlockThisDeviceOnly,
	}
}

// keychain is used for internal integration as well.
var NewCredCacheInternalIntegration = NewCredCache

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

// hasCachedTokenInternal returns if there is cached token in token manager for current executing user.
func (c *CredCache) hasCachedTokenInternal() (bool, error) {
	query := keychain.NewItem()
	query.SetSecClass(c.kcSecClass)
	query.SetService(c.serviceName)
	query.SetAccount(c.accountName)
	query.SetMatchLimit(keychain.MatchLimitOne)
	query.SetReturnAttributes(true)
	results, err := keychain.QueryItem(query)
	if err != nil {
		err = handleGenericKeyChainSecError(err)
		return false, err
	}
	if len(results) < 1 {
		return false, nil
	}
	if len(results) > 1 {
		return false, errors.New("invalid state, more than one cached token found")
	}
	return true, nil
}

// removeCachedTokenInternal delete the cached token.
func (c *CredCache) removeCachedTokenInternal() error {
	err := keychain.DeleteGenericPasswordItem(c.serviceName, c.accountName)
	if err != nil {
		err = handleGenericKeyChainSecError(err)

		if err == keychain.ErrorItemNotFound {
			return fmt.Errorf("no cached token found for current user")
		}

		return fmt.Errorf("failed to remove cached token, %v", err)
	}
	return nil
}

// saveTokenInternal saves an oauth token in keychain(use user's default keychain, i.e. login keychain).
func (c *CredCache) saveTokenInternal(token OAuthTokenInfo) error {
	b, err := token.toJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal during saving token, %v", err)
	}
	item := keychain.NewItem()
	item.SetSecClass(c.kcSecClass)
	item.SetService(c.serviceName)
	item.SetAccount(c.accountName)
	item.SetData(b)
	item.SetSynchronizable(c.kcSynchronizable)
	item.SetAccessible(c.kcAccessible)

	err = keychain.AddItem(item)
	if err != nil {
		// Handle duplicate key error
		if err != keychain.ErrorDuplicateItem {
			err = handleGenericKeyChainSecError(err)
			return fmt.Errorf("failed to save token, %v", err)
		}

		// Update the key
		query := keychain.NewItem()
		query.SetSecClass(c.kcSecClass)
		query.SetService(c.serviceName)
		query.SetAccount(c.accountName)
		query.SetMatchLimit(keychain.MatchLimitOne)
		query.SetReturnData(true)
		err := keychain.UpdateItem(query, item)
		if err != nil {
			err = handleGenericKeyChainSecError(err)
			return fmt.Errorf("failed to save token, %v", err)
		}
	}
	return nil
}

// loadTokenInternal gets an oauth token from keychain.
func (c *CredCache) loadTokenInternal() (*OAuthTokenInfo, error) {
	query := keychain.NewItem()
	query.SetSecClass(c.kcSecClass)
	query.SetService(c.serviceName)
	query.SetAccount(c.accountName)
	query.SetMatchLimit(keychain.MatchLimitOne)
	query.SetReturnData(true)
	results, err := keychain.QueryItem(query)
	if err != nil {
		err = handleGenericKeyChainSecError(err)
		return nil, fmt.Errorf("failed to load token, %v", err)
	}
	if len(results) != 1 {
		return nil, errors.New("failed to find cached token during loading token")
	}
	data := results[0].Data
	token, err := jsonToTokenInfo(data)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal token during loading token, %v", err)
	}
	return token, nil
}

// handleGenericKeyChainSecError handles generic key chain sec errors.
func handleGenericKeyChainSecError(err error) error {
	if err == keychain.ErrorInteractionNotAllowed {
		return fmt.Errorf(
			"if you are using SSH, please run 'security unlock-keychain' to unlock default(login) keychain from SSH first, and then re-run your azcopy command. (OnError details: %v)", err)
	}

	return err
}
