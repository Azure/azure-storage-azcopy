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
	"fmt"
	"os/exec"
	"strings"

	keychain "github.com/jiacfan/go-keychain"
)

type credCache struct{}

const cachedTokenKey = "AzCopyOAuthTokenCache"
const serviceName = "AzCopyV10"

// HasCachedToken returns if there is cached token in token manager for current executing user.
func (c *credCache) HasCachedToken() (bool, error) {
	_, err := keychain.Find(serviceName, cachedTokenKey)
	if err != nil {
		return false, err
	}
	return true, nil
}

// RemoveCachedToken delete the cached token.
func (c *credCache) RemoveCachedToken() error {
	err := keychain.Remove(serviceName, cachedTokenKey)
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
	err := keychain.Add(serviceName, cachedTokenKey, string(b))
	if err != nil {
		return err
	}
}

func (c *credCache) LoadToken() (*OAuthTokenInfo, error) {
	data, err := keychain.Find(serviceName, cachedTokenKey)
	if err != nil {
		return nil, err
	}
	token, err := JSONToTokenInfo(([]byte) data)
	if err != nil {
		return nil, err
	}
	return token, nil
}
