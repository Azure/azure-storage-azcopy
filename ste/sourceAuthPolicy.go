// Copyright © Microsoft <wastore@microsoft.com>
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

package ste

import (
	"net/http"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
)

// sourceAuthPolicy should be used as a per-retry policy
// when source is authenticated via oAuth.
type sourceAuthPolicy struct {
	cred  azcore.TokenCredential
	token *azcore.AccessToken
	lock  sync.RWMutex
}

const copySourceAuthHeader = "x-ms-copy-source-authorization"
const minimumTokenValidDuration = time.Minute * 5

func NewSourceAuthPolicy(cred azcore.TokenCredential) policy.Policy {
	return &sourceAuthPolicy{cred: cred}
}

func (s *sourceAuthPolicy) Do(req *policy.Request) (*http.Response, error) {
	if len(req.Raw().Header[copySourceAuthHeader]) == 0 { // nolint:staticcheck
		return req.Next()
	}

	// s.cred is common.ScopedCredential, options gets ignored. This is done so
	// that common.ScopedCredential is tagged as azcore.TokenCredential interface
	options := policy.TokenRequestOptions{Scopes: nil, EnableCAE: true}
	s.lock.RLock()
	if s.token == nil || time.Until(s.token.ExpiresOn) < minimumTokenValidDuration {
		s.lock.RUnlock()
		s.lock.Lock()
		// If someone else has updated the token while we waited
		// above, we dont have to refresh again
		if s.token == nil || time.Until(s.token.ExpiresOn) < minimumTokenValidDuration {
			tk, err := s.cred.GetToken(req.Raw().Context(), options)
			if err != nil {
				s.lock.Unlock()
				return nil, err
			}
			s.token = &tk
		}
		req.Raw().Header[copySourceAuthHeader] = []string{"Bearer " + s.token.Token}
		s.lock.Unlock()
	} else {
		req.Raw().Header[copySourceAuthHeader] = []string{"Bearer " + s.token.Token}
		s.lock.RUnlock()
	}

	return req.Next()
}
