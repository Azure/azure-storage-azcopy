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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/stretchr/testify/assert"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"
)

const tokenInfoJson = `{
					"access_token": "dummy_access_token",
					"refresh_token": "dummy_refresh_token",
					"expires_in": 0,
					"expires_on": 0,
					"not_before": 0,
					"resource": "dummy_resource",
					"token_type": "dummy_token_type",
					"_tenant": "dummy_tenant",
					"_ad_endpoint": "dummy_ad_endpoint",
					"_token_refresh_source": %v,
					"_application_id": "dummy_application_id",
					"IdentityInfo": {
						"_identity_client_id": "dummy_identity_client_id",
						"_identity_object_id": "dummy_identity_object_id",
						"_identity_msi_res_id": "dummy_identity_msi_res_id"
					},
					"SPNInfo": {
						"_spn_secret": "dummy_spn_secret",
						"_spn_cert_path": "dummy_spn_cert_path"
					}
				}`

var oauthTokenInfo = OAuthTokenInfo{
	Tenant:                  "dummy_tenant",
	ActiveDirectoryEndpoint: "dummy_ad_endpoint",
	LoginType:               255,
	ApplicationID:           "dummy_application_id",
	IdentityInfo: IdentityInfo{
		ClientID: "dummy_identity_client_id",
		ObjectID: "dummy_identity_object_id",
		MSIResID: "dummy_identity_msi_res_id",
	},
	SPNInfo: SPNInfo{
		Secret:   "dummy_spn_secret",
		CertPath: "dummy_spn_cert_path",
	},
}

func formatTokenInfo(value interface{}) string {
	var formattedValue string
	switch v := value.(type) {
	case string:
		formattedValue = fmt.Sprintf("\"%s\"", v)
	case int:
		formattedValue = strconv.Itoa(v)
	default:
		formattedValue = fmt.Sprintf("%v", v)
	}
	return fmt.Sprintf(tokenInfoJson, formattedValue)
}

func TestUserOAuthTokenManager_GetTokenInfo(t *testing.T) {
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		uotm    *UserOAuthTokenManager
		args    args
		setup   func(t *testing.T)
		want    *OAuthTokenInfo
		wantErr bool
	}{
		{
			name: "This UT tests if AutoLoginType filled is parsed properly from string to uint8 data type",
			uotm: &UserOAuthTokenManager{},
			args: args{
				ctx: context.Background(),
			},
			setup: func(t *testing.T) {
				tokenInfo := formatTokenInfo("TokenStore")
				fmt.Println(tokenInfo)

				// Set the environment variable AZCOPY_OAUTH_TOKEN_INFO
				err := os.Setenv("AZCOPY_OAUTH_TOKEN_INFO", tokenInfo)
				if err != nil {
					t.Fatalf("Failed to set environment variable: %v", err)
				}
			},
			want:    &oauthTokenInfo,
			wantErr: false,
		},
		{
			name: "This UT tests if AutoLoginType filled is assigned properly to uint8 data type",
			uotm: &UserOAuthTokenManager{},
			args: args{
				ctx: context.Background(),
			},
			setup: func(t *testing.T) {
				tokenInfo := formatTokenInfo(255)
				fmt.Println(tokenInfo)

				// Set the environment variable AZCOPY_OAUTH_TOKEN_INFO
				err := os.Setenv("AZCOPY_OAUTH_TOKEN_INFO", tokenInfo)
				if err != nil {
					t.Fatalf("Failed to set environment variable: %v", err)
				}
			},
			want:    &oauthTokenInfo,
			wantErr: false,
		},
		{
			name: "This UT tests if _token_refresh_source fails to parse due to invalid string type",
			uotm: &UserOAuthTokenManager{},
			args: args{
				ctx: context.Background(),
			},
			setup: func(t *testing.T) {
				tokenInfo := formatTokenInfo("2gt5")

				// Set the environment variable AZCOPY_OAUTH_TOKEN_INFO
				err := os.Setenv("AZCOPY_OAUTH_TOKEN_INFO", tokenInfo)
				if err != nil {
					t.Fatalf("Failed to set environment variable: %v", err)
				}
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "This UT tests if _token_refresh_source fails to parse due to value out of uint8 range",
			uotm: &UserOAuthTokenManager{},
			args: args{
				ctx: context.Background(),
			},
			setup: func(t *testing.T) {
				tokenInfo := formatTokenInfo(847)

				// Set the environment variable AZCOPY_OAUTH_TOKEN_INFO
				err := os.Setenv("AZCOPY_OAUTH_TOKEN_INFO", tokenInfo)
				if err != nil {
					t.Fatalf("Failed to set environment variable: %v", err)
				}
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			tt.setup(t)
			got, err := tt.uotm.GetTokenInfo(tt.args.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("UserOAuthTokenManager.GetTokenInfo() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != nil && reflect.DeepEqual(got, tt.want) {
				t.Errorf("UserOAuthTokenManager.GetTokenInfo() = %v, want %v", got, tt.want)
			}
		})
	}
}

type TestCredCache struct {
	stashed *OAuthTokenInfo
}

func (d *TestCredCache) HasCachedToken() (bool, error) {
	return d.stashed != nil, nil
}

func (d *TestCredCache) LoadToken() (*OAuthTokenInfo, error) {
	if d.stashed == nil {
		return nil, errors.New("no cached token found")
	}

	return d.stashed, nil
}

func (d *TestCredCache) SaveToken(info OAuthTokenInfo) error {
	d.stashed = &info
	return nil
}

func (d *TestCredCache) RemoveCachedToken() error {
	if d.stashed == nil {
		return errors.New("no cached token found")
	}

	d.stashed = nil
	return nil
}

func TestTokenStoreCredentialHang(t *testing.T) {
	tok := &azcore.AccessToken{
		Token:     "asdf",
		ExpiresOn: time.Now().Add(minimumTokenValidDuration * 2), // we want to hit that if statement at the start and get it into the read lock
	}

	tsc := &TokenStoreCredential{
		token: tok,
		credCache: &TestCredCache{
			stashed: &OAuthTokenInfo{
				Token: Token{
					AccessToken: "foobar",
					ExpiresOn:   json.Number(fmt.Sprint(tok.ExpiresOn.Unix())),
				},
			},
		},
	}

	// Prior to this PR, we'd get locked into a read state doing this, because the if statement didn't contain a way out of the read lock.
	outTok, err := tsc.GetToken(context.Background(), policy.TokenRequestOptions{})
	assert.NoError(t, err)
	assert.Equal(t, tok.Token, outTok.Token)

	// we shouldn't get blocked here, otherwise we have problems.
	assert.Equal(t, true, tsc.lock.TryLock())
	tsc.lock.Unlock()

	tok.ExpiresOn = time.Now() // now it should refresh

	// We shouldn't get caught here at all
	outTok, err = tsc.GetToken(context.Background(), policy.TokenRequestOptions{})
	assert.NoError(t, err)
	assert.Equal(t, "foobar", outTok.Token) // ensure it refreshed

	// we shouldn't get blocked here, otherwise we have problems.
	assert.Equal(t, true, tsc.lock.TryLock())
	tsc.lock.Unlock()
}
