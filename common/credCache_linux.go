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
	// todo: make a fork on this repo, and use the forked repo
	"github.com/jsipprell/keyctl"
)

type credCache struct{}

const cachedTokenKey = "AzCopyOAuthTokenCache"

// HasCachedToken returns if there is cached token in token manager for current executing user.
func (c *credCache) HasCachedToken() (bool, error) {
	keyring, err := keyctl.SessionKeyring()
	if err != nil {
		return false, err
	}
	key, err := keyring.Search(cachedTokenKey)
	if err != nil {
		return false, err
	} else {
		return true, nil
	}
}

// RemoveCachedToken delete the cached token.
func (c *credCache) RemoveCachedToken() error {
	keyring, err := keyctl.SessionKeyring()
	if err != nil {
		return err
	}
	key, err := keyring.Search(cachedTokenKey)
	if err != nil {
		return err
	}
	err := key.Unlink()
	if err != nil {
		return err
	}

	return nil
}

func (c *credCache) SaveToken(token OAuthTokenInfo) error {
	b, err := token.ToJSON()
	if err != nil {
		return err
	}
	keyring, err := keyctl.SessionKeyring()
	if err != nil {
		return err
	}
	_, err := keyring.Add(cachedTokenKey, b)
	if err != nil {
		return err
	}
	return nil
}

func (credCache) LoadToken() (*OAuthTokenInfo, error) {
	keyring, err := keyctl.SessionKeyring()
	if err != nil {
		return nil, err
	}
	key, err := keyring.Search(cachedTokenKey)
	if err != nil {
		return nil, err
	}
	data, err := key.Get()
	if err != nil {
		return nil, err
	}
	token, err := JSONToTokenInfo(data)
	if err != nil {
		return nil, err
	}
	return token, nil
}
