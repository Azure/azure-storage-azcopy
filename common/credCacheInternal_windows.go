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
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/danieljoos/wincred"
)

// CredCacheInternalIntegration manages credential caches with Windows credential manager.
// To ensure big secrets can be stored and read, it use a segmented pattern to save/load tokens when necessary.
type CredCacheInternalIntegration struct {
	keyName string
	lock    sync.Mutex
}

// NewCredCacheInternalIntegration creates a cred cache.
func NewCredCacheInternalIntegration(options CredCacheOptions) *CredCacheInternalIntegration {
	return &CredCacheInternalIntegration{
		keyName: options.KeyName,
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

const errNotFound = "Element not found."

// hasCachedTokenInternal returns if there is cached token in token manager.
func (c *CredCacheInternalIntegration) hasCachedTokenInternal() (bool, error) {
	_, err := wincred.GetGenericCredential(c.keyName)
	if err != nil {
		if err.Error() == errNotFound {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

// removeCachedTokenInternal deletes all the cached token.
func (c *CredCacheInternalIntegration) removeCachedTokenInternal() error {
	// By design, not useful currently.
	return errors.New("Not implemented")
}

// segmentTokenInfo is used to present information about segmented token saved in credential manager.
type segmentedTokenHeader struct {
	SegmentNum string `json:"SegmentNum"`
	SHA256Hash string `json:"SHA256Hash"`
}

// loadTokenInternal restores a Token object from file cache.
func (c *CredCacheInternalIntegration) loadTokenInternal() (*OAuthTokenInfo, error) {
	cred, err := wincred.GetGenericCredential(c.keyName)
	if err != nil {
		return nil, err
	}

	tokenInfo := cred.CredentialBlob

	// Check whether it's in segmented pattern by trying to parse the root token with segmentedTokenHeader.
	// If it's not in segmented pattern, treat the token as a non-segmented one.
	var segmentedTokenHeader segmentedTokenHeader
	if err := json.Unmarshal(tokenInfo, &segmentedTokenHeader); err == nil {
		// The generic tokenInfo is in segmented pattern as the segmentedTokenHeader can be parsed properly

		var buffer bytes.Buffer
		partNum, err := strconv.Atoi(segmentedTokenHeader.SegmentNum)
		if err != nil || partNum <= 0 {
			return nil, fmt.Errorf("segmented token broken, cannot get number of segments or the value is <= 0, %v", err)
		}
		for i := 0; i < partNum; i++ {
			subKey := c.keyName + "/" + strconv.Itoa(i)
			if subCred, err := wincred.GetGenericCredential(subKey); err != nil {
				return nil, fmt.Errorf("segmented token broken, failed to read %s, %v", subKey, err)
			} else {
				buffer.Write(subCred.CredentialBlob)
			}
		}
		tokenInfo = buffer.Bytes()

		// Do sha256 validation, ensuring the token is consistent
		sha256OfReadTokenInfo := fmt.Sprintf("%x", sha256.Sum256(tokenInfo)) // returns 32 byte checksum
		if sha256OfReadTokenInfo != segmentedTokenHeader.SHA256Hash {
			return nil, fmt.Errorf(
				"segmented token broken, SHA256 hash mismatch, expected: %s, get: %s",
				segmentedTokenHeader.SHA256Hash, sha256OfReadTokenInfo)
		}
	}

	token, err := jsonToTokenInfo(tokenInfo)
	if err != nil {
		return nil, err
	}

	return token, nil
}

// saveTokenInternal persists an oauth token on disk.
func (c *CredCacheInternalIntegration) saveTokenInternal(token OAuthTokenInfo) error {
	// By design, not useful currently.
	return errors.New("Not implemented")
}
