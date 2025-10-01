package main

import (
	"errors"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/e2etest"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"net/url"
	"reflect"
	"strings"
)

type GeneratorConfig struct {
	AuthInfo struct {
		AuthSource struct {
			// Use the PS cred auth. Requires powershell 7 and Az Powershell installed.
			PSInherit bool `env:"PS_INHERIT,required"`
			// Use the AZ CLI auth. Requires Az CLI installed.
			CLIInherit bool `env:"CLI_INHERIT,required"`
			// Use an account key. Not recommended.
			AcctKey string `env:"ACCT_KEY,required"`
		} `env:",required,mutually_exclusive"`
		// Specify the tenant ID being used to authorize.
		TenantID string `env:"TENANT_ID"`
	} `env:",required"`

	RetryOptions struct {
		// An override switch similar to AzCopy's
		HttpRetryCodes string `env:"HTTP_RETRY_CODES"`
	}
}

var accountCache *e2etest.AzureAccountResourceManager

func GetAccountResourceManager(uri string) (e2etest.AccountResourceManager, error) {
	if accountCache != nil {
		return accountCache, nil
	}

	opts := azcore.ClientOptions{
		Retry: policy.RetryOptions{
			MaxRetries:    ste.UploadMaxTries,
			TryTimeout:    ste.UploadTryTimeout,
			RetryDelay:    ste.UploadRetryDelay,
			MaxRetryDelay: ste.UploadMaxRetryDelay,
			ShouldRetry:   ste.GetShouldRetry(nil),
		},
	}

	var out e2etest.AccountResourceManager

	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("failed to parse uri: %w", err)
	}

	// gross and bad, do as I say not as I do
	acctName := u.Host[:strings.IndexRune(u.Host, '.')]

	if acctKey := genConfig.AuthInfo.AuthSource.AcctKey; acctKey != "" {
		out = &e2etest.AzureAccountResourceManager{
			InternalAccountName: acctName,
			InternalAccountKey:  acctKey,
			InternalAccountType: e2etest.EAccountType.Standard(),
		}
	} else {
		tc, err := genConfig.GetTokenCredential()
		if err != nil {
			return nil, fmt.Errorf("failed to get account resource manager: invalid tokencredential: %w", err)
		}

		out = &OAuthAccountResourceManager{
			serviceClientOptions: opts,
			cred:                 tc,
			accountName:          acctName,
		}
	}

	return out, nil
}

func (g GeneratorConfig) GetTokenCredential() (azcore.TokenCredential, error) {
	ai := g.AuthInfo
	switch {
	case ai.AuthSource.CLIInherit:
		ret, err := azidentity.NewAzureCLICredential(&azidentity.AzureCLICredentialOptions{
			TenantID: ai.TenantID,
		})

		return ret, err
	case ai.AuthSource.PSInherit:
		ret, err := common.NewPowershellContextCredential(&common.PowershellContextCredentialOptions{
			TenantID: ai.TenantID,
		})

		return ret, err
	default:
		return nil, errors.New("invalid authSource")
	}
}

var genConfig GeneratorConfig

func init() {
	err := e2etest.ReadConfig(reflect.ValueOf(&genConfig).Elem(), "GeneratorConfig", e2etest.EnvTag{Required: true})
	if err != nil {
		panic("failed reading config: " + err.Error())
	}
}
