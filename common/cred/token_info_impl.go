package cred

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity/cache"
	"github.com/Azure/azure-storage-azcopy/v10/common/cred/token_providers"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/Azure/azure-storage-azcopy/v10/common/ternary"

	// importing the cache module registers the cache implementation for the current platform
	_ "github.com/Azure/azure-sdk-for-go/sdk/azidentity/cache"
)

// ===== Token Cache =====

const tokenCacheName = "AzCopyTokenCache"

var persistentCache = func() azidentity.Cache {
	var out azidentity.Cache

	if cache, err := cache.New(&cache.Options{
		Name: tokenCacheName,
	}); err == nil {
		out = cache
	} else {
		fmt.Println("Warning: Failed to initialize azidentity credential cache. Saving tokens may fail.")
	}

	return out
}()

// ===== Root token impl =====

func (t *token) GetTokenCredential() (azcore.TokenCredential, error) {
	return t.tokenImpl.getTokenCredential(t.TokenHeader)
}

func (t token) MarshalJSON() ([]byte, error) {
	type TokenImpl tokenImpl
	type rawStruct struct {
		TokenHeader
		AuthDetails TokenImpl
	}

	var toMarshal rawStruct
	toMarshal.AuthDetails = t.tokenImpl
	toMarshal.TokenHeader = t.TokenHeader

	return json.Marshal(toMarshal)
}

func (t *token) UnmarshalJSON(buf []byte) error {
	type rawStruct struct {
		TokenHeader
		AuthDetails json.RawMessage

		Persist bool
	}

	var rawToken rawStruct
	err := json.Unmarshal(buf, &rawToken)
	if err != nil {
		return fmt.Errorf("failed to unmarshal token header: %w", err)
	}

	// Copy header values
	t.TokenHeader = rawToken.TokenHeader

	// Create a pointer to a zero value of our intended type, then unmarshal to it.
	tokenImpl, err := unmarshalTokenImpl(rawToken.AuthDetails, rawToken.LoginType)
	if err != nil {
		return err
	}

	// Deref the pointer, then typecast to TokenImpl for compatibility
	t.tokenImpl = tokenImpl

	return nil
}

// ========== SPN impl ==========

func (t tokenInfoSPN) tokenImpl() {}

func (t tokenInfoSPN) fromLoginTokenOptions(opts LoginTokenOptions) tokenImpl {
	t.ApplicationID = opts.ApplicationID
	t.Cert = opts.CertificateData
	t.Secret = opts.ClientSecret
	return t
}

func (t tokenInfoSPN) getTokenCredential(header TokenHeader) (azcore.TokenCredential, error) {
	if t.Cert != "" {
		return t.getTokenCert(header)
	}

	return t.getTokenSecret(header)
}

func (t tokenInfoSPN) getTokenCert(header TokenHeader) (azcore.TokenCredential, error) {
	var certData []byte
	if _, err := os.Stat(t.Cert); err == nil {
		certData, err = os.ReadFile(t.Cert)
		if err != nil {
			return nil, err
		}
	} else {
		certData = []byte(t.Cert)
	}

	certs, key, err := azidentity.ParseCertificates(certData, []byte(t.Secret))
	if err != nil {
		return nil, err
	}

	return azidentity.NewClientCertificateCredential(
		header.Tenant,
		t.ApplicationID,
		certs,
		key,
		&azidentity.ClientCertificateCredentialOptions{
			ClientOptions: azcore.ClientOptions{
				Cloud:     cloud.Configuration{ActiveDirectoryAuthorityHost: header.ActiveDirectoryEndpoint},
				Transport: newAzcopyHTTPClient(),
			},
			DisableInstanceDiscovery: false,
			SendCertificateChain:     true,
		},
	)
}

func (t tokenInfoSPN) getTokenSecret(header TokenHeader) (azcore.TokenCredential, error) {
	return azidentity.NewClientSecretCredential(
		header.Tenant,
		t.ApplicationID,
		t.Secret,
		&azidentity.ClientSecretCredentialOptions{
			ClientOptions: azcore.ClientOptions{
				Cloud:     cloud.Configuration{ActiveDirectoryAuthorityHost: header.ActiveDirectoryEndpoint},
				Transport: newAzcopyHTTPClient(),
			},
			DisableInstanceDiscovery: false,
		},
	)
}

func (t tokenInfoSPN) fromCompat(compat compatTokenInfo) tokenImpl {
	t.ApplicationID = compat.ApplicationID

	// Since we accept either CertData or CertPath in the same field now, pick whichever is filled.
	t.Cert = ternary.Iff(
		compat.SPNInfo.CertData != "",
		compat.SPNInfo.CertData,
		compat.SPNInfo.CertPath)

	t.Secret = compat.SPNInfo.Secret

	return t
}

// ========== MSI impl ==========

func (t tokenInfoManagedIdentity) tokenImpl() {}

func (t tokenInfoManagedIdentity) fromLoginTokenOptions(opts LoginTokenOptions) tokenImpl {
	t.ClientID = opts.IdentityClientID
	t.ObjectID = opts.IdentityObjectID
	t.MSIResID = opts.IdentityResourceID
	return t
}

func (t tokenInfoManagedIdentity) getTokenCredential(header TokenHeader) (azcore.TokenCredential, error) {
	var id azidentity.ManagedIDKind
	if t.ClientID != "" {
		id = azidentity.ClientID(t.ClientID)
	} else if t.MSIResID != "" {
		id = azidentity.ResourceID(t.MSIResID)
	} else {
		return nil, fmt.Errorf("object ID is deprecated and no longer supported for managed identity. Please use client ID or resource ID instead")
	}

	return azidentity.NewManagedIdentityCredential(&azidentity.ManagedIdentityCredentialOptions{
		ClientOptions: azcore.ClientOptions{
			Cloud:     cloud.Configuration{ActiveDirectoryAuthorityHost: header.ActiveDirectoryEndpoint},
			Transport: newAzcopyHTTPClient(),
		},
		ID: id,
	})
}

func (t tokenInfoManagedIdentity) fromCompat(compat compatTokenInfo) tokenImpl {
	t.ClientID = compat.IdentityInfo.ClientID
	t.ObjectID = compat.IdentityInfo.ObjectID
	t.MSIResID = compat.IdentityInfo.MSIResID

	return t
}

// ========== AzCLI impl ==========

func (t tokenInfoCLI) tokenImpl() {}

func (t tokenInfoCLI) fromLoginTokenOptions(opts LoginTokenOptions) tokenImpl { return t }

func (t tokenInfoCLI) getTokenCredential(header TokenHeader) (azcore.TokenCredential, error) {
	return azidentity.NewAzureCLICredential(&azidentity.AzureCLICredentialOptions{TenantID: header.Tenant})
}

func (t tokenInfoCLI) fromCompat(compat compatTokenInfo) tokenImpl {
	return t
}

// ========== PSCred impl ==========

func (t tokenInfoPSCred) tokenImpl() {}

func (t tokenInfoPSCred) fromLoginTokenOptions(opts LoginTokenOptions) tokenImpl { return t }

func (t tokenInfoPSCred) getTokenCredential(header TokenHeader) (azcore.TokenCredential, error) {
	return token_providers.NewPowershellContextCredential(&token_providers.PowershellContextCredentialOptions{TenantID: header.Tenant})
}

func (t tokenInfoPSCred) fromCompat(compat compatTokenInfo) tokenImpl {
	return t
}

// ========== User login impl ==========

func (t *tokenInfoUserLogin) tokenImpl() {}

func (t *tokenInfoUserLogin) fromLoginTokenOptions(opts LoginTokenOptions) tokenImpl {
	t.ApplicationID = opts.ApplicationID
	switch opts.LoginType {
	case enum.EAutoLoginType.Interactive():
		t.InteractionType = enum.EInteractiveLoginType.Browser()
	default:
		t.InteractionType = enum.EInteractiveLoginType.Device()
	}

	t.AuthRecord = &azidentity.AuthenticationRecord{}

	return t
}

func (t *tokenInfoUserLogin) getTokenCredential(header TokenHeader) (tc azcore.TokenCredential, err error) {
	var authorityHost *url.URL
	authorityHost, err = url.Parse(header.ActiveDirectoryEndpoint)
	if err != nil {
		return nil, err
	}

	if t.AuthRecord == nil {
		t.AuthRecord = &azidentity.AuthenticationRecord{}
	}

	switch t.InteractionType {
	case enum.EInteractiveLoginType.Device():
		return azidentity.NewDeviceCodeCredential(&azidentity.DeviceCodeCredentialOptions{
			TenantID:                       header.Tenant,
			ClientID:                       t.ApplicationID,
			DisableAutomaticAuthentication: true,
			AuthenticationRecord:           *t.AuthRecord,
			Cache:                          persistentCache,
			ClientOptions: azcore.ClientOptions{
				Cloud:     cloud.Configuration{ActiveDirectoryAuthorityHost: authorityHost.String()},
				Transport: newAzcopyHTTPClient(),
			},
		})
	case enum.EInteractiveLoginType.Browser():
		return azidentity.NewInteractiveBrowserCredential(&azidentity.InteractiveBrowserCredentialOptions{
			TenantID:             header.Tenant,
			ClientID:             t.ApplicationID,
			AuthenticationRecord: *t.AuthRecord,
			Cache:                persistentCache,
			ClientOptions: azcore.ClientOptions{
				Cloud:     cloud.Configuration{ActiveDirectoryAuthorityHost: authorityHost.String()},
				Transport: newAzcopyHTTPClient(),
			},
		})
	default:
		return nil, fmt.Errorf("unknown interactive login type: %s", t.InteractionType)
	}
}

func (t *tokenInfoUserLogin) fromCompat(compat compatTokenInfo) tokenImpl {
	t.ApplicationID = compat.ApplicationID
	t.AuthRecord = compat.DeviceCodeInfo
	t.InteractionType = enum.EInteractiveLoginType.Device() // Browser was never supported in prior versions

	return t
}

// ========== Workload impl ==========

func (t tokenInfoWorkload) tokenImpl() {}

func (t tokenInfoWorkload) fromLoginTokenOptions(opts LoginTokenOptions) tokenImpl { return t }

func (t tokenInfoWorkload) getTokenCredential(header TokenHeader) (azcore.TokenCredential, error) {
	return azidentity.NewWorkloadIdentityCredential(&azidentity.WorkloadIdentityCredentialOptions{
		ClientOptions: azcore.ClientOptions{
			Transport: newAzcopyHTTPClient(),
		},
		TenantID: header.Tenant,
	})
}

func (t tokenInfoWorkload) fromCompat(compat compatTokenInfo) tokenImpl {
	return t
}

// ========== TokenStore impl ==========

const tokenStoreMinimumValidDuration = time.Minute * 5

func (t *tokenInfoTokenStore) tokenImpl() {}

func (t tokenInfoTokenStore) fromLoginTokenOptions(opts LoginTokenOptions) tokenImpl { return &t }

func (t *tokenInfoTokenStore) getTokenCredential(header TokenHeader) (azcore.TokenCredential, error) {
	t.nickname = header.Nickname
	return t, nil
}

func (t *tokenInfoTokenStore) GetToken(ctx context.Context, options policy.TokenRequestOptions) (azcore.AccessToken, error) {
	t.mu.RLock()
	if time.Until(t.ExpiresOn) > tokenStoreMinimumValidDuration {
		defer t.mu.RUnlock()
		return azcore.AccessToken{
			Token:     t.Token,
			ExpiresOn: t.ExpiresOn,
		}, nil
	}
	t.mu.RUnlock()

	t.mu.Lock()
	defer t.mu.Unlock()

	// double-check after taking the write lock
	if time.Until(t.ExpiresOn) > tokenStoreMinimumValidDuration {
		return azcore.AccessToken{
			Token:     t.Token,
			ExpiresOn: t.ExpiresOn,
		}, nil
	}

	if t.parent == nil || t.nickname == "" {
		return azcore.AccessToken{}, errors.New("token expired, no parent keyring or nickname available to refresh")
	}

	fresh, ok := t.parent.GetToken(t.nickname)
	if !ok {
		return azcore.AccessToken{}, errors.New("token expired, no fresh token found in parent keyring")
	}

	ts, ok := fresh.tokenImpl.(*tokenInfoTokenStore)
	if !ok {
		return azcore.AccessToken{}, errors.New("token expired, no fresh token found in parent keyring")
	}

	t.Token = ts.Token
	t.ExpiresOn = ts.ExpiresOn
	return azcore.AccessToken{
		Token:     t.Token,
		ExpiresOn: t.ExpiresOn,
	}, nil
}

func (t *tokenInfoTokenStore) fromCompat(compat compatTokenInfo) tokenImpl {
	t.Token = compat.AccessToken
	t.ExpiresOn = compat.Expires()
	return t
}

// MarshalJSON encodes ExpiresOn as a raw unix-seconds number.
func (t *tokenInfoTokenStore) MarshalJSON() ([]byte, error) {
	type alias struct {
		Token     string `json:"token"`
		ExpiresOn int64  `json:"expires_on"`
	}
	return json.Marshal(alias{
		Token:     t.Token,
		ExpiresOn: t.ExpiresOn.Unix(),
	})
}

// UnmarshalJSON decodes ExpiresOn from a raw unix-seconds number.
func (t *tokenInfoTokenStore) UnmarshalJSON(buf []byte) error {
	type alias struct {
		Token     string `json:"token"`
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
