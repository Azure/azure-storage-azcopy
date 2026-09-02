package token_providers

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
)

type fetcherCredential struct {
	fetchToken func() (azcore.AccessToken, error)
}

func (t *fetcherCredential) GetToken(ctx context.Context, options policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return t.fetchToken()
}
