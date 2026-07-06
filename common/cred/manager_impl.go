// Copyright © 2024 Microsoft <wastore@microsoft.com>
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

package cred

import (
	"context"
	"errors"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
)

type managerImpl struct {
	newLoginLock *sync.RWMutex
	newLogins    *memKeyring

	rings []Keyring
}

func (m *managerImpl) ListCredentials() ([]TokenHeader, error) {
	var out []TokenHeader

	for _, v := range m.rings {
		if ring, ok := v.(EnumerableKeyring); ok {
			toAdd, err := ring.ListTokens()
			if err != nil {
				return nil, err
			}

			out = append(out, toAdd...)
		}
	}

	return out, nil
}

// NewManager returns a new Manager instance.
// Keyrings are searched FIFO. Writes are handled FCFS. Only one RWKeyring should be provided (realistically, probably GetOSKeyring).
// I.e. [ GetIntegrationKeyring(), GetEnvironmentKeyring(), GetOSKeyring() ] would only ever write to the OS keyring, but would pull from "external" keyrings first.
func NewManager(keyrings ...Keyring) Manager {
	return &managerImpl{
		newLoginLock: &sync.RWMutex{},
		newLogins:    &memKeyring{identities: make(map[string]token)},
		rings:        keyrings,
	}
}

// GetCredentials returns the credential matching the nickname, in the order
// returned by registered keyrings.
func (m *managerImpl) GetCredentials(nickname string) (azcore.TokenCredential, error) {
	if nickname == "" {
		nickname = DefaultNickname
	}

	for _, v := range m.rings {
		token, ok := v.GetToken(nickname)
		if !ok {
			continue
		}

		// For TokenStore tokens, hand the originating keyring as the
		// parent so GetToken can re-fetch a fresh access token when the
		// cached one expires.
		if ts, ok := token.tokenImpl.(*tokenInfoTokenStore); ok {
			ts.parent = v
		}

		cred, err := m.resolveCredential(&token)
		if err != nil {
			continue
		}

		return cred, nil
	}

	return nil, errors.New("no credential found for nickname")
}

// resolveCredential turns a token into a working azcore.TokenCredential.
func (m *managerImpl) resolveCredential(token *token) (azcore.TokenCredential, error) {
	// Device code with no auth record: authenticate on demand.
	if userLogin, ok := token.tokenImpl.(*tokenInfoUserLogin); ok {
		if userLogin.InteractionType == enum.EInteractiveLoginType.Device() && userLogin.AuthRecord == nil {
			cred, err := token.tokenImpl.getTokenCredential(token.TokenHeader)
			if err != nil {
				return nil, err
			}

			authToken, ok := cred.(AuthenticateToken)
			if !ok {
				return nil, errors.New("device code credential does not support Authenticate")
			}

			record, err := authToken.Authenticate(context.TODO(), &policy.TokenRequestOptions{
				EnableCAE: true,
				Scopes:    DefaultAuthenticateScopes,
			})
			if err != nil {
				return nil, err
			}

			userLogin.AuthRecord = &record
			token.tokenImpl = userLogin

			token.cachedToken = cred
			return cred, nil
		}
	}

	cred, err := token.tokenImpl.getTokenCredential(token.TokenHeader)
	if err != nil {
		return nil, err
	}
	token.cachedToken = cred
	return cred, nil
}

func (m *managerImpl) DoLogin(opts LoginTokenOptions, ctx context.Context) (azcore.TokenCredential, error) {
	token := newLoginToken(opts)

	cred, err := token.tokenImpl.getTokenCredential(token.TokenHeader)
	if err != nil {
		return nil, err
	}

	if userLogin, ok := token.tokenImpl.(*tokenInfoUserLogin); ok {
		authToken, ok := cred.(AuthenticateToken)
		if !ok {
			return nil, errors.New("user login credential does not support Authenticate")
		}

		record, err := authToken.Authenticate(ctx, &policy.TokenRequestOptions{
			EnableCAE: true,
			Scopes:    DefaultAuthenticateScopes,
		})
		if err != nil {
			return nil, err
		}

		userLogin.AuthRecord = &record
		token.tokenImpl = userLogin

		if token.Tenant == DefaultTenantID {
			token.Tenant = userLogin.AuthRecord.TenantID
		}
	}

	token.cachedToken = cred

	if opts.SaveCredential {
		if err := m.saveToken(token); err != nil {
			return nil, err
		}
	}

	return cred, nil
}

// saveToken searches through Keyrings for a RWKeyring, and places in the first available one. There shouldn't be multiple specified.
func (m *managerImpl) saveToken(info token) error {
	for _, v := range m.rings {
		rwKeyring, ok := v.(RWKeyring)
		if !ok {
			continue
		}
		return rwKeyring.SaveToken(info)
	}
	return errors.New("no writable keyring available to save token")
}

// DeleteCredentials deletes the token matching the nickname across every writable Keyring.
func (m *managerImpl) DeleteCredentials(nickname string) bool {
	if nickname == "" {
		nickname = DefaultNickname
	}

	deleted := false
	for _, v := range m.rings {
		rwKeyring, ok := v.(RWKeyring)
		if !ok {
			continue
		}

		if rwKeyring.DeleteToken(nickname) {
			deleted = true
		}
	}

	return deleted
}

func (m *managerImpl) GetKeyrings() []Keyring {
	return m.rings
}
