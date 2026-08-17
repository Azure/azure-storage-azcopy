package cred

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

// TestMarshalTokenInfo round-trips the TokenInfo struct through JSON's marshaller to ensure the values are equivalent.
func TestMarshalTokenInfo(t *testing.T) {
	tenantA := uuid.NewString()
	appID := uuid.NewString()

	sampleTokens := map[string]token{
		"foobar": {
			TokenHeader: TokenHeader{
				Tenant:                  tenantA,
				Nickname:                "foobar",
				ActiveDirectoryEndpoint: "https://login.windows.net/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/authorize",
				LoginType:               enum.EAutoLoginType.Interactive(),
			},
			tokenImpl: &tokenInfoUserLogin{
				ApplicationID: appID,
				AuthRecord: &azidentity.AuthenticationRecord{
					Authority:     DefaultActiveDirectoryEndpoint,
					ClientID:      appID,
					HomeAccountID: "",
					TenantID:      tenantA,
					Username:      "",
					Version:       "1.0",
				},
				InteractionType: enum.EInteractiveLoginType.Browser(),
			},
		},
		tenantA: {
			TokenHeader: TokenHeader{
				Tenant:                  tenantA,
				Nickname:                tenantA,
				ActiveDirectoryEndpoint: "https://login.windows.net/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/authorize",
				LoginType:               enum.EAutoLoginType.SPN(),
			},
			tokenImpl: tokenInfoSPN{
				ApplicationID: appID,
				Secret:        uuid.NewString(),
				Cert:          "asdf",
			},
		},
		"tokenstore": {
			TokenHeader: TokenHeader{
				Tenant:                  tenantA,
				Nickname:                "tokenstore",
				ActiveDirectoryEndpoint: "https://login.microsoftonline.com",
				LoginType:               enum.EAutoLoginType.TokenStore(),
			},
			tokenImpl: &tokenInfoTokenStore{
				Token:     "the-actual-access-token",
				ExpiresOn: time.Unix(1700000000, 0).UTC(),
			},
		},
	}

	//fullBuf, _ := json.Marshal(sampleTokens)

	a := assert.New(t)

	for _, v := range sampleTokens {
		buf, err := json.Marshal(v)
		a.NoError(err)
		var out token
		err = json.Unmarshal(buf, &out)
		a.NoError(err)

		a.Equal(v, out)
	}
}

// TestTokenStoreCredential checks that a TokenStore token can produce a working
// credential that hands back the access token it was loaded with.
func TestTokenStoreCredential(t *testing.T) {
	a := assert.New(t)

	original := token{
		TokenHeader: TokenHeader{
			Tenant:                  "tenant",
			Nickname:                "tokenstore",
			ActiveDirectoryEndpoint: "https://login.microsoftonline.com",
			LoginType:               enum.EAutoLoginType.TokenStore(),
		},
		tokenImpl: &tokenInfoTokenStore{
			Token:     "the-actual-access-token",
			ExpiresOn: time.Unix(2000000000, 0).UTC(), // year 2033, safely in the future
		},
	}

	cred, err := original.tokenImpl.getTokenCredential(original.TokenHeader, context.Background())
	a.NoError(err)
	a.NotNil(cred)

	at, err := cred.GetToken(context.TODO(), policy.TokenRequestOptions{})
	a.NoError(err)
	a.Equal("the-actual-access-token", at.Token)
	a.Equal(time.Unix(2000000000, 0).UTC(), at.ExpiresOn)
}

// TestTokenStoreRefetchExpired verifies that an expired TokenStore credential
// re-reads from its parent keyring to pick up a refreshed access token.
func TestTokenStoreRefetchExpired(t *testing.T) {
	a := assert.New(t)

	expiredExpiry := time.Time{}             // zero time is definitely in the past
	freshExpiry := time.Now().Add(time.Hour) // ridiculous forward time
	nickname := "tokenstore"

	// Parent keyring has a fresh version of the same token.
	parent := NewMemKeyring(map[string]Token{
		nickname: &token{
			TokenHeader: TokenHeader{
				Tenant:                  "tenant",
				Nickname:                nickname,
				ActiveDirectoryEndpoint: "https://login.microsoftonline.com",
				LoginType:               enum.EAutoLoginType.TokenStore(),
			},
			tokenImpl: &tokenInfoTokenStore{
				Token:     "fresh-access-token",
				ExpiresOn: freshExpiry,
			},
		},
	})

	rwParent, ok := parent.(Keyring)
	a.True(ok)

	original := &tokenInfoTokenStore{
		parent: rwParent,
		Token:  "stale-access-token",
		// set the nickname via getTokenCredential below
		ExpiresOn: expiredExpiry,
	}

	cred, err := original.getTokenCredential(TokenHeader{Nickname: nickname}, nil)
	a.NoError(err)

	at, err := cred.GetToken(context.TODO(), policy.TokenRequestOptions{})
	a.NoError(err)
	a.Equal("fresh-access-token", at.Token, "should pull fresh token from parent keyring")
	a.Equal(freshExpiry, at.ExpiresOn)
}
