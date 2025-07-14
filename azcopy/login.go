package azcopy

import "github.com/Azure/azure-storage-azcopy/v10/common"

type LoginOptions struct {
	TenantID    string
	AADEndpoint string
	LoginType   common.AutoLoginType

	// Managed Identity Options
	IdentityClientID   string
	IdentityResourceID string
	IdentityObjectID   string

	// Service Principal Options
	ApplicationID       string
	ClientSecret        string
	CertificatePath     string
	CertificatePassword string

	PersistToken bool // Whether to persist the token in the credential cache
}

type LoginResponse struct {
}

func (c Client) Login(opts LoginOptions) (LoginResponse, error) {
	resp := LoginResponse{}
	uotm := GetUserOAuthTokenManagerInstance()

	// Persist the token to cache, if login fulfilled successfully.
	switch opts.LoginType {
	case common.EAutoLoginType.SPN():
		if opts.CertificatePath != "" {
			return resp, uotm.CertLogin(opts.TenantID, opts.AADEndpoint, opts.CertificatePath, opts.CertificatePassword, opts.ApplicationID, opts.PersistToken)
		} else {
			return resp, uotm.SecretLogin(opts.TenantID, opts.AADEndpoint, opts.ClientSecret, opts.ApplicationID, opts.PersistToken)
		}
	case common.EAutoLoginType.MSI():
		return resp, uotm.MSILogin(common.IdentityInfo{
			ClientID: opts.IdentityClientID,
			ObjectID: opts.IdentityObjectID,
			MSIResID: opts.IdentityResourceID,
		}, opts.PersistToken)
	case common.EAutoLoginType.AzCLI():
		return resp, uotm.AzCliLogin(opts.TenantID, opts.PersistToken)
	case common.EAutoLoginType.PsCred():
		return resp, uotm.PSContextToken(opts.TenantID, opts.PersistToken)
	case common.EAutoLoginType.Workload():
		return resp, uotm.WorkloadIdentityLogin(opts.PersistToken)
	default:
		return resp, uotm.UserLogin(opts.TenantID, opts.AADEndpoint, opts.PersistToken)
		// User fulfills login in browser, and there would be message in browser indicating whether login fulfilled successfully.
	}
}
