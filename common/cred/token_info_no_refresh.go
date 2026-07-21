package cred

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
)

type tokenInfoNoRefresh struct {
	Token     string
	ExpiresOn time.Time
}

func (t *tokenInfoNoRefresh) tokenImpl() {}

func (t *tokenInfoNoRefresh) fromLoginTokenOptions(opts NewTokenOptions) tokenImpl {
	return &tokenInfoNoRefresh{}
}

func (t *tokenInfoNoRefresh) getTokenCredential(header TokenHeader, ctx context.Context) (azcore.TokenCredential, error) {
	return t, nil
}

func (t *tokenInfoNoRefresh) fromCompat(compat compatTokenInfo) tokenImpl {
	return t
}

func (t *tokenInfoNoRefresh) GetToken(ctx context.Context, options policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{
		Token:     t.Token,
		ExpiresOn: t.ExpiresOn,
	}, nil
}

func (t *tokenInfoNoRefresh) MarshalJSON() ([]byte, error) {
	type alias struct {
		Token     string `json:"access_token"`
		ExpiresOn int64  `json:"expires_on"`
	}
	return json.Marshal(alias{
		Token:     t.Token,
		ExpiresOn: t.ExpiresOn.Unix(),
	})
}

func (t *tokenInfoNoRefresh) UnmarshalJSON(buf []byte) error {
	type alias struct {
		Token     string `json:"access_token"`
		ExpiresOn int64  `json:"expires_on"`
	}
	var a alias
	if err := json.Unmarshal(buf, &a); err != nil {
		return err
	}
	t.Token = a.Token
	t.ExpiresOn = time.Unix(a.ExpiresOn, 0).UTC()
	return nil
}
