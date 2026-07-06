package cred

import (
	"encoding/json"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/Azure/azure-storage-azcopy/v10/common/ternary"
	"github.com/Azure/go-autorest/autorest/date"
)

// compatTokenInfo is the classic OAuth token info struct, intended for compatability with prior scripting setups for AzCopy.
type compatTokenInfo struct {
	azcore.TokenCredential `json:"-"`
	compatToken
	Tenant                  string             `json:"_tenant"`
	ActiveDirectoryEndpoint string             `json:"_ad_endpoint"`
	LoginType               enum.AutoLoginType `json:"_token_refresh_source"`
	ApplicationID           string             `json:"_application_id"`
	IdentityInfo            compatIdentityInfo
	SPNInfo                 compatSPNInfo
	ClientID                string                           `json:"_client_id"`
	DeviceCodeInfo          *azidentity.AuthenticationRecord `json:"_authentication_record,omitempty"`
	Persist                 bool                             `json:"_persist"`
}

// compatToken is the old token struct.
type compatToken struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`

	ExpiresIn json.Number `json:"expires_in"`
	ExpiresOn json.Number `json:"expires_on"`
	NotBefore json.Number `json:"not_before"`

	Resource string `json:"resource"`
	Type     string `json:"token_type"`
}

// Expires returns the time.Time when the token expires.
func (t compatToken) Expires() time.Time {
	s, err := t.ExpiresOn.Float64()
	if err != nil {
		s = -3600
	}

	expiration := date.NewUnixTimeFromSeconds(s)

	return time.Time(expiration).UTC()
}

// compatIdentityInfo contains info for MSI.
type compatIdentityInfo struct {
	ClientID string `json:"_identity_client_id"`
	ObjectID string `json:"_identity_object_id"`
	MSIResID string `json:"_identity_msi_res_id"`
}

// compatSPNInfo contains info for authenticating with Service Principal Names
type compatSPNInfo struct {
	// Secret is used for two purposes: The certificate secret, and a client secret.
	// The secret is persisted to the JSON file because AAD does not issue a refresh token.
	// Thus, the original secret is needed to refresh.
	Secret   string `json:"_spn_secret"`
	CertPath string `json:"_spn_cert_path"`
	CertData string `json:"_spn_cert_data"`
}

func (c compatTokenInfo) Upgrade() token {
	out := token{
		TokenHeader: TokenHeader{
			Tenant:                  c.Tenant,
			Nickname:                ternary.Iff(c.Tenant != "", c.Tenant, "*"),
			ActiveDirectoryEndpoint: c.ActiveDirectoryEndpoint,
			LoginType:               c.LoginType,
		},
	}

	out.tokenImpl = newTokenImpl(c.LoginType).fromCompat(c)

	return out
}
