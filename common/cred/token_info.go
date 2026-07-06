package cred

import (
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
)

type TokenHeader struct {
	Tenant string `json:"tenant_id"`
	// Nickname, if empty, should be treated as having the value of Tenant by keyrings/token managers.
	Nickname                string             `json:"nickname,omitempty"`
	ActiveDirectoryEndpoint string             `json:"active_directory_endpoint"`
	LoginType               enum.AutoLoginType `json:"token_refresh_source"`
}

type tokenInfoSPN struct {
	ApplicationID string `json:"application_id"`
	// Secret is either a raw client secret, or a certificate password.
	Secret string `json:"spn_secret"`
	// Cert accepts either a raw certificate, or a path to a certificate.
	Cert string `json:"spn_cert"`
}

type tokenInfoManagedIdentity struct {
	ClientID string `json:"identity_client_id"`
	ObjectID string `json:"identity_object_id"`
	MSIResID string `json:"identity_msi_res_id"`
}

type tokenInfoUserLogin struct {
	ApplicationID   string                           `json:"application_id"`
	AuthRecord      *azidentity.AuthenticationRecord `json:"authentication_record,omitempty"`
	InteractionType enum.InteractiveLoginType        `json:"interaction_type"`
}

type tokenInfoPSCred struct{}

type tokenInfoCLI struct{}

type tokenInfoWorkload struct{}

type tokenInfoTokenStore struct {
	mu sync.RWMutex

	// used to refresh token
	parent   Keyring
	nickname string

	Token     string    `json:"-"`
	ExpiresOn time.Time `json:"-"`
}

type token struct {
	cachedToken azcore.TokenCredential

	TokenHeader
	tokenImpl
}
