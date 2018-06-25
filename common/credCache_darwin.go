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

	keychain "github.com/jiacfan/go-keychain" // forked and customized from github.com/keybase/go-keychain, todo: make a release to ensure stability
)

const cachedTokenKey = "AzCopyOAuthTokenCache"
const serviceName = "AzCopyV10"

// For SSH environment, user need unlock login keychain once, to enable AzCopy to Add/Update/Retrieve/Delete key.
// For Mac UI or terminal environment, unlock is not mandatory.
// For more details about Apple keychain support: https://developer.apple.com/documentation/security/keychain_services?language=objc
// CredCache manages credential caches.
type CredCache struct {
	state string
	lock  sync.Mutex

	// KeyChain settings
	kcSecClass       keychain.SecClass
	kcSynchronizable keychain.Synchronizable
	kcAccessible     keychain.Accessible
	// kcAccessGroup, bypass AccessGroup setting, as Azcopy is a green software don't need install currently.
	// for more details, refer to https://developer.apple.com/documentation/security/ksecattraccessgroup?language=objc
}

func NewCredCache(state string) *CredCache {
	return &CredCache{
		state:      state,
		kcSecClass: keychain.SecClassGenericPassword,
		// do not synchronize this through ICloud.
		// for more details, refer to https://developer.apple.com/documentation/security/ksecattrsynchronizable?language=objc
		kcSynchronizable: keychain.SynchronizableNo,
		// using AccessibleAfterFirstUnlockThisDeviceOnly, so user can login once, and execute commands silently.
		// for more details, refer to https://developer.apple.com/documentation/security/ksecattraccessible?language=objc
		// and https://developer.apple.com/documentation/security/keychain_services/keychain_items/restricting_keychain_item_accessibility?language=objc
		kcAccessible: keychain.AccessibleAfterFirstUnlockThisDeviceOnly,
	}
}

// HasCachedToken returns if there is cached token in token manager for current executing user.
func (c *CredCache) HasCachedToken() (bool, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	query := keychain.NewItem()
	query.SetSecClass(c.kcSecClass)
	query.SetService(serviceName)
	query.SetAccount(cachedTokenKey)
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

// RemoveCachedToken delete the cached token.
func (c *CredCache) RemoveCachedToken() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	err := keychain.DeleteGenericPasswordItem(serviceName, cachedTokenKey)
	if err != nil {
		err = handleGenericKeyChainSecError(err)
		return fmt.Errorf("failed to remove cached token, %v", err)
	}
	return nil
}

// SaveToken saves an oauth token in keychain(use user's default keychain, i.e. login keychain).
func (c *CredCache) SaveToken(token OAuthTokenInfo) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	b, err := token.ToJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal during saving token, %v", err)
	}
	item := keychain.NewItem()
	item.SetSecClass(c.kcSecClass)
	item.SetService(serviceName)
	item.SetAccount(cachedTokenKey)
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
		query.SetService(serviceName)
		query.SetAccount(cachedTokenKey)
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

// LoadToken gets an oauth token from keychain.
func (c *CredCache) LoadToken() (*OAuthTokenInfo, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	query := keychain.NewItem()
	query.SetSecClass(c.kcSecClass)
	query.SetService(serviceName)
	query.SetAccount(cachedTokenKey)
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
	token, err := JSONToTokenInfo(data)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal token during loading token, %v", err)
	}
	return token, nil
}

// handleGenericKeyChainSecError handles generic key chain sec errors.
func handleGenericKeyChainSecError(err error) error {
	if err == keychain.ErrorInteractionNotAllowed {
		return fmt.Errorf(
			"%v (Please check if default(login) keychain is locked, to use AzCopy from SSH in Mac OS, please unlock default(login) keychain from SSH first)",
			err)
	}

	return err
}
