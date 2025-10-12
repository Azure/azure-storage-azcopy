// Copyright © Microsoft <wastore@microsoft.com>
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
	"context"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
)

// This is to be used only for testing reasons
func getE2ETokenCredential(a *assert.Assertions) azcore.TokenCredential {
	if os.Getenv("NEW_E2E_ENVIRONMENT") != "AzurePipeline" {
		tenantId := os.Getenv("AZCOPY_E2E_TENANT_ID")
		appID := os.Getenv("AZCOPY_E2E_APPLICATION_ID")
		clientSecret := os.Getenv("AZCOPY_E2E_CLIENT_SECRET")
		a.NotZero(tenantId)
		a.NotZero(appID)
		a.NotZero(clientSecret)
		cred, err := azidentity.NewClientSecretCredential(tenantId, appID, clientSecret, nil)
		a.NoError(err)
		return cred
	} else {
		tenantId := os.Getenv("tenantId")
		cred, err := azidentity.NewDefaultAzureCredential(&azidentity.DefaultAzureCredentialOptions{TenantID: tenantId})
		a.NoError(err)
		return cred
	}
}

func TestSrcAuthPolicy(t *testing.T) {
	a := assert.New(t)
	cred := getE2ETokenCredential(a)
	cred = common.NewScopedCredential(cred, common.ECredentialType.OAuthToken())
	srcAuthPolicy := NewSourceAuthPolicy(cred)
	req, err := runtime.NewRequest(context.Background(), http.MethodGet, "https://127.0.0.1/")
	a.NoError(err)

	// 1. Do not set copySourceAuthHeader and verify the policy does not add it.
	_, _ = srcAuthPolicy.Do(req)
	a.Zero(req.Raw().Header[copySourceAuthHeader]) // nolint:staticcheck

	// 2. Set copySourceAuthHeader to an invalid string and verify the policy modifies it.
	req.Raw().Header[copySourceAuthHeader] = []string{"InvalidString"} // nolint:staticcheck
	_, _ = srcAuthPolicy.Do(req)
	a.True(strings.HasPrefix(req.Raw().Header[copySourceAuthHeader][0], "Bearer ")) // nolint:staticcheck

	// 3. Verify that when token is not expired, policy sets the same token again
	oldToken := req.Raw().Header[copySourceAuthHeader][0]
	_, _ = srcAuthPolicy.Do(req)
	a.Equal(oldToken, req.Raw().Header[copySourceAuthHeader][0]) // nolint:staticcheck

	//4. forcefully expire the token and verify a new token is obtained
	s := srcAuthPolicy.(*sourceAuthPolicy)
	expTime := time.Now()
	s.token.ExpiresOn = time.Now()
	time.Sleep(10 * time.Second)
	_, _ = srcAuthPolicy.Do(req)

	// Disabled below statement, sometimes calling GetToken frequently results in same token
	// being received again. In order to verify that we've a different token, we'll compare
	// expiry time, and infer that GetToken has been called.
	// a.NotEqual(oldToken, req.Raw().Header[copySourceAuthHeader][0]) // nolint:staticcheck
	a.NotEqual(expTime, s.token.ExpiresOn)
}

func TestSrcAuthPolicyMultipleRefresh(t *testing.T) {
	a := assert.New(t)
	cred := getE2ETokenCredential(a)
	cred = common.NewScopedCredential(cred, common.ECredentialType.OAuthToken())
	srcAuthPolicy := NewSourceAuthPolicy(cred)

	// 5. When multiple goroutines request a token to be refreshed, token should be refreshed once
	req := make([]*policy.Request, 50)
	var err error
	for i := 0; i < 50; i++ {
		req[i], err = runtime.NewRequest(context.Background(), http.MethodGet, "https://127.0.0.1")
		req[i].Raw().Header[copySourceAuthHeader] = []string{"InvalidString"} // nolint:staticcheck
		a.NoError(err)
	}

	var wg sync.WaitGroup
	ch := make(chan bool) // this channel will release flood gates so that all below routines
	// execute in a high parallelism

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(r *policy.Request) {
			<-ch
			srcAuthPolicy.Do(r)
			wg.Done()
		}(req[i])
	}
	close(ch)

	wg.Wait()

	tokenString := req[0].Raw().Header[copySourceAuthHeader][0] // nolint:staticcheck

	for i := 0; i < 50; i++ {
		a.Equal(req[i].Raw().Header[copySourceAuthHeader][0], tokenString) // nolint:staticcheck
	}
}
