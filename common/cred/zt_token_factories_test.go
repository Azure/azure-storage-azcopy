package cred

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/stretchr/testify/assert"
)

func tokenImplFromToken(t *testing.T, tok Token) tokenImpl {
	t.Helper()
	inner, ok := tok.(*token)
	if !ok {
		t.Fatal("Token must be *token")
	}
	return inner.tokenImpl
}

func TestSPNTokenOptions(t *testing.T) {
	a := assert.New(t)

	tok := SPNTokenOptions{
		TenantID:        "my-tenant",
		AADEndpoint:     "https://login.microsoftonline.us",
		LoginType:       enum.EAutoLoginType.SPN(),
		ApplicationID:   "app-id",
		CertificateData: "cert-data",
		ClientSecret:    "client-secret",
	}.NewToken()

	spn, ok := tokenImplFromToken(t, tok).(tokenInfoSPN)
	a.True(ok, "expected tokenInfoSPN")
	a.Equal("app-id", spn.ApplicationID)
	a.Equal("cert-data", spn.Cert)
	a.Equal("client-secret", spn.Secret)

	h := tok.Header()
	a.Equal("my-tenant", h.Tenant)
	a.Empty(h.Nickname, "Nickname is not set by token factories")
	a.Equal("https://login.microsoftonline.us", h.ActiveDirectoryEndpoint)
	a.Equal(enum.EAutoLoginType.SPN(), h.LoginType)
}

func TestSPNTokenOptions_Defaults(t *testing.T) {
	a := assert.New(t)

	tok := SPNTokenOptions{
		LoginType: enum.EAutoLoginType.SPN(),
	}.NewToken()

	h := tok.Header()
	a.Equal(DefaultTenantID, h.Tenant, "should use default tenant")
	a.Empty(h.Nickname, "Nickname is not set by token factories")
	a.Equal(DefaultActiveDirectoryEndpoint, h.ActiveDirectoryEndpoint, "should use default AAD endpoint")
}

func TestMSITokenOptions(t *testing.T) {
	a := assert.New(t)

	tok := MSITokenOptions{
		TenantID:           "my-tenant",
		LoginType:          enum.EAutoLoginType.MSI(),
		IdentityClientID:   "client-id",
		IdentityObjectID:   "object-id",
		IdentityResourceID: "resource-id",
	}.NewToken()

	msi, ok := tokenImplFromToken(t, tok).(tokenInfoManagedIdentity)
	a.True(ok, "expected tokenInfoManagedIdentity")
	a.Equal("client-id", msi.ClientID)
	a.Equal("object-id", msi.ObjectID)
	a.Equal("resource-id", msi.MSIResID)
}

func TestMSITokenOptions_Empty(t *testing.T) {
	a := assert.New(t)

	tok := MSITokenOptions{
		LoginType: enum.EAutoLoginType.MSI(),
	}.NewToken()

	msi, ok := tokenImplFromToken(t, tok).(tokenInfoManagedIdentity)
	a.True(ok)
	a.Empty(msi.ClientID)
	a.Empty(msi.ObjectID)
	a.Empty(msi.MSIResID)
}

func TestUserLoginTokenOptions_Device(t *testing.T) {
	a := assert.New(t)

	tok := UserLoginTokenOptions{
		LoginType:       enum.EAutoLoginType.Device(),
		ApplicationID:   "app-id",
		InteractionType: enum.EInteractiveLoginType.Device(),
	}.NewToken()

	ul, ok := tokenImplFromToken(t, tok).(*tokenInfoUserLogin)
	a.True(ok, "expected *tokenInfoUserLogin")
	a.Equal("app-id", ul.ApplicationID)
	a.Equal(enum.EInteractiveLoginType.Device(), ul.InteractionType)
	a.NotNil(ul.AuthRecord)
	a.Equal(&azidentity.AuthenticationRecord{}, ul.AuthRecord, "AuthRecord should be empty but non-nil")
}

func TestUserLoginTokenOptions_Browser(t *testing.T) {
	a := assert.New(t)

	tok := UserLoginTokenOptions{
		LoginType:       enum.EAutoLoginType.Interactive(),
		ApplicationID:   "app-id",
		InteractionType: enum.EInteractiveLoginType.Browser(),
	}.NewToken()

	ul, ok := tokenImplFromToken(t, tok).(*tokenInfoUserLogin)
	a.True(ok, "expected *tokenInfoUserLogin")
	a.Equal("app-id", ul.ApplicationID)
	a.Equal(enum.EInteractiveLoginType.Browser(), ul.InteractionType)
	a.NotNil(ul.AuthRecord)
}

func TestAzureCLITokenOptions(t *testing.T) {
	a := assert.New(t)

	tok := AzureCLITokenOptions{
		LoginType: enum.EAutoLoginType.AzCLI(),
	}.NewToken()

	_, ok := tokenImplFromToken(t, tok).(tokenInfoCLI)
	a.True(ok, "expected tokenInfoCLI")
}

func TestPSCredTokenOptions(t *testing.T) {
	a := assert.New(t)

	tok := PSCredTokenOptions{
		LoginType: enum.EAutoLoginType.PsCred(),
	}.NewToken()

	_, ok := tokenImplFromToken(t, tok).(tokenInfoPSCred)
	a.True(ok, "expected tokenInfoPSCred")
}

func TestWorkloadTokenOptions(t *testing.T) {
	a := assert.New(t)

	tok := WorkloadTokenOptions{
		LoginType: enum.EAutoLoginType.Workload(),
	}.NewToken()

	_, ok := tokenImplFromToken(t, tok).(tokenInfoWorkload)
	a.True(ok, "expected tokenInfoWorkload")
}

func TestNewTokenOptionsInterface(t *testing.T) {
	a := assert.New(t)

	var f NewTokenOptions

	f = SPNTokenOptions{LoginType: enum.EAutoLoginType.SPN()}
	a.NotNil(f)
	tok := f.NewToken()
	_, ok := tok.(*token)
	a.True(ok, "must return a *token")

	f = MSITokenOptions{LoginType: enum.EAutoLoginType.MSI()}
	a.NotNil(f)

	f = UserLoginTokenOptions{LoginType: enum.EAutoLoginType.Device(), InteractionType: enum.EInteractiveLoginType.Device()}
	a.NotNil(f)

	f = AzureCLITokenOptions{LoginType: enum.EAutoLoginType.AzCLI()}
	a.NotNil(f)

	f = PSCredTokenOptions{LoginType: enum.EAutoLoginType.PsCred()}
	a.NotNil(f)

	f = WorkloadTokenOptions{LoginType: enum.EAutoLoginType.Workload()}
	a.NotNil(f)

	// LoginNewTokenOptions also implements the interface
	f = LoginNewTokenOptions{LoginType: enum.EAutoLoginType.SPN()}
	a.NotNil(f)
}

func TestLoginNewTokenOptionsDispatch_SPN(t *testing.T) {
	a := assert.New(t)

	opts := LoginNewTokenOptions{
		LoginType:        enum.EAutoLoginType.SPN(),
		ApplicationID:    "app-id",
		CertificateData:  "cert-data",
		ClientSecret:     "client-secret",
		TenantID:         "my-tenant",
		AADEndpoint:      "https://login.microsoftonline.us",
		SaveCredential:   true,
	}

	tok := opts.NewToken()
	spn, ok := tokenImplFromToken(t, tok).(tokenInfoSPN)
	a.True(ok, "expected tokenInfoSPN")
	a.Equal("app-id", spn.ApplicationID)
	a.Equal("cert-data", spn.Cert)
	a.Equal("client-secret", spn.Secret)

	h := tok.Header()
	a.Equal("my-tenant", h.Tenant)
	a.Empty(h.Nickname)
	a.Equal("https://login.microsoftonline.us", h.ActiveDirectoryEndpoint)
	a.Equal(enum.EAutoLoginType.SPN(), h.LoginType)
}

func TestLoginNewTokenOptionsDispatch_MSI(t *testing.T) {
	a := assert.New(t)

	opts := LoginNewTokenOptions{
		LoginType:          enum.EAutoLoginType.MSI(),
		IdentityClientID:   "client-id",
		IdentityObjectID:   "object-id",
		IdentityResourceID: "resource-id",
	}

	tok := opts.NewToken()
	msi, ok := tokenImplFromToken(t, tok).(tokenInfoManagedIdentity)
	a.True(ok, "expected tokenInfoManagedIdentity")
	a.Equal("client-id", msi.ClientID)
	a.Equal("object-id", msi.ObjectID)
	a.Equal("resource-id", msi.MSIResID)
}

func TestLoginNewTokenOptionsDispatch_Device(t *testing.T) {
	a := assert.New(t)

	opts := LoginNewTokenOptions{
		LoginType:     enum.EAutoLoginType.Device(),
		ApplicationID: "app-id",
	}

	tok := opts.NewToken()
	ul, ok := tokenImplFromToken(t, tok).(*tokenInfoUserLogin)
	a.True(ok, "expected *tokenInfoUserLogin")
	a.Equal("app-id", ul.ApplicationID)
	a.Equal(enum.EInteractiveLoginType.Device(), ul.InteractionType)
	a.NotNil(ul.AuthRecord)
}

func TestLoginNewTokenOptionsDispatch_Interactive(t *testing.T) {
	a := assert.New(t)

	opts := LoginNewTokenOptions{
		LoginType:     enum.EAutoLoginType.Interactive(),
		ApplicationID: "app-id",
	}

	tok := opts.NewToken()
	ul, ok := tokenImplFromToken(t, tok).(*tokenInfoUserLogin)
	a.True(ok, "expected *tokenInfoUserLogin")
	a.Equal("app-id", ul.ApplicationID)
	a.Equal(enum.EInteractiveLoginType.Browser(), ul.InteractionType)
	a.NotNil(ul.AuthRecord)
}

func TestLoginNewTokenOptionsDispatch_AzCLI(t *testing.T) {
	a := assert.New(t)

	opts := LoginNewTokenOptions{
		LoginType: enum.EAutoLoginType.AzCLI(),
	}

	tok := opts.NewToken()
	_, ok := tokenImplFromToken(t, tok).(tokenInfoCLI)
	a.True(ok, "expected tokenInfoCLI")
}

func TestLoginNewTokenOptionsDispatch_PsCred(t *testing.T) {
	a := assert.New(t)

	opts := LoginNewTokenOptions{
		LoginType: enum.EAutoLoginType.PsCred(),
	}

	tok := opts.NewToken()
	_, ok := tokenImplFromToken(t, tok).(tokenInfoPSCred)
	a.True(ok, "expected tokenInfoPSCred")
}

func TestLoginNewTokenOptionsDispatch_Workload(t *testing.T) {
	a := assert.New(t)

	opts := LoginNewTokenOptions{
		LoginType: enum.EAutoLoginType.Workload(),
	}

	tok := opts.NewToken()
	_, ok := tokenImplFromToken(t, tok).(tokenInfoWorkload)
	a.True(ok, "expected tokenInfoWorkload")
}

func TestLoginNewTokenOptionsDispatch_TokenStore(t *testing.T) {
	a := assert.New(t)

	opts := LoginNewTokenOptions{
		LoginType: enum.EAutoLoginType.TokenStore(),
	}

	tok := opts.NewToken()
	_, ok := tokenImplFromToken(t, tok).(*tokenInfoTokenStore)
	a.True(ok, "expected *tokenInfoTokenStore (fallback via newTokenImpl)")
}

func TestLoginNewTokenOptionsDispatch_NoRefresh(t *testing.T) {
	a := assert.New(t)

	opts := LoginNewTokenOptions{
		LoginType: enum.EAutoLoginType.NoRefresh(),
	}

	tok := opts.NewToken()
	_, ok := tokenImplFromToken(t, tok).(*tokenInfoNoRefresh)
	a.True(ok, "expected *tokenInfoNoRefresh (fallback via newTokenImpl)")
}

func TestLoginNewTokenOptions_Defaults(t *testing.T) {
	a := assert.New(t)

	opts := LoginNewTokenOptions{
		LoginType: enum.EAutoLoginType.SPN(),
	}

	tok := opts.NewToken()
	h := tok.Header()
	a.Equal(DefaultTenantID, h.Tenant, "should use default tenant")
	a.Empty(h.Nickname, "Nickname is not set by token factories")
	a.Equal(DefaultActiveDirectoryEndpoint, h.ActiveDirectoryEndpoint, "should use default AAD endpoint")
	a.Equal(enum.EAutoLoginType.SPN(), h.LoginType)
}

func TestLoginNewTokenOptionsDispatch_SPN_EmptyFields(t *testing.T) {
	a := assert.New(t)

	opts := LoginNewTokenOptions{
		LoginType: enum.EAutoLoginType.SPN(),
	}

	tok := opts.NewToken()
	spn, ok := tokenImplFromToken(t, tok).(tokenInfoSPN)
	a.True(ok)
	a.Empty(spn.ApplicationID)
	a.Empty(spn.Cert)
	a.Empty(spn.Secret)
}

func TestLoginNewTokenOptionsDispatch_MSI_OnlyClientID(t *testing.T) {
	a := assert.New(t)

	opts := LoginNewTokenOptions{
		LoginType:        enum.EAutoLoginType.MSI(),
		IdentityClientID: "only-client-id",
	}

	tok := opts.NewToken()
	msi, ok := tokenImplFromToken(t, tok).(tokenInfoManagedIdentity)
	a.True(ok)
	a.Equal("only-client-id", msi.ClientID)
	a.Empty(msi.ObjectID)
	a.Empty(msi.MSIResID)
}
