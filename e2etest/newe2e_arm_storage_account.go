package e2etest

import (
	"github.com/Azure/azure-storage-blob-go/azblob"
	"net/http"
	"net/url"
)

// ARMStorageAccount implements an API to interface with a singular Azure Storage account via the Storage Resource Provider's REST APIs.
// https://learn.microsoft.com/en-us/rest/api/storagerp/storage-accounts
type ARMStorageAccount struct {
	*ARMResourceGroup
	AccountName string
}

func (sa *ARMStorageAccount) ManagementURI() url.URL {
	baseURI := sa.ARMResourceGroup.ManagementURI()
	newURI := baseURI.JoinPath("providers/Microsoft.Storage/storageAccounts", sa.AccountName)

	return *newURI
}

func (sa *ARMStorageAccount) PerformRequest(baseURI url.URL, reqSettings ARMRequestSettings, target interface{}) (armResp *ARMAsyncResponse, err error) {
	if reqSettings.Query == nil {
		reqSettings.Query = make(url.Values)
	}

	if !reqSettings.Query.Has("api-version") {
		reqSettings.Query.Add("api-version", "2023-01-01") // Attach default query
	}

	return sa.ARMClient.PerformRequest(baseURI, reqSettings, target)
}

// ARMStorageAccountCreateParams is the request body for https://learn.microsoft.com/en-us/rest/api/storagerp/storage-accounts/create?tabs=HTTP#storageaccount
type ARMStorageAccountCreateParams struct {
	Kind             azblob.AccountKindType             `json:"kind"`
	Location         string                             `json:"location"`
	Sku              ARMStorageAccountSKU               `json:"sku"`
	ExtendedLocation *ARMExtendedLocation               `json:"extendedLocation,omitempty"`
	Identity         *ARMStorageAccountIdentity         `json:"identity,omitempty"`
	Properties       *ARMStorageAccountCreateProperties `json:"properties,omitempty"`
}

// ARMStorageAccountCreateProperties implements a portion of ARMStorageAccountCreateParams.
// https://learn.microsoft.com/en-us/rest/api/storagerp/storage-accounts/create?tabs=HTTP#storageaccount
type ARMStorageAccountCreateProperties struct { // ARMUnimplementedStruct(s) are used as filler, if needed for one-offs
	AccessTier                            *azblob.AccessTierType `json:"accessTier,omitempty"`
	AllowBlobPublicAccess                 *bool                  `json:"allowBlobPublicAccess,omitempty"`
	AllowCrossTenantReplication           *bool                  `json:"allowCrossTenantReplication,omitempty"`
	AllowSharedKeyAccess                  *bool                  `json:"allowSharedKeyAccess,omitempty"`
	AllowedCopyScope                      ARMUnimplementedStruct `json:"allowedCopyScope,omitempty"`
	AzureFilesIdentityBasedAuthentication ARMUnimplementedStruct `json:"azureFilesIdentityBasedAuthentication,omitempty"`
	CustomDomain                          ARMUnimplementedStruct `json:"customDomain,omitempty"`
	DefaultToOAuthAuthentication          *bool                  `json:"defaultToOAuthAuthentication,omitempty"`
	DNSEndpointType                       ARMUnimplementedStruct `json:"dnsEndpointType,omitempty"`
	Encryption                            ARMUnimplementedStruct `json:"encryption,omitempty"`                     // todo cpk?
	ImmutableStorageWithVersioning        ARMUnimplementedStruct `json:"immutableStorageWithVersioning,omitempty"` // todo version level WORM
	IsHnsEnabled                          *bool                  `json:"isHnsEnabled,omitempty"`
	IsLocalUserEnabled                    *bool                  `json:"isLocalUserEnabled,omitempty"`
	IsNfsV3Enabled                        *bool                  `json:"isNfsV3Enabled,omitempty"`
	IsSftpEnabled                         *bool                  `json:"isSftpEnabled,omitempty"`
	KeyPolicy                             ARMUnimplementedStruct `json:"keyPolicy,omitempty"`
	LargeFileSharesState                  *string                `json:"largeFileSharesState,omitempty"` // "Enabled" or "Disabled"
	MinimumTLSVersion                     *string                `json:"minimumTLSVersion,omitempty"`
	NetworkACLs                           ARMUnimplementedStruct `json:"networkAcls,omitempty"`
	PublicNetworkAccess                   *string                `json:"publicNetworkAccess,omitempty"` // "Enabled" or "Disabled"
	RoutingPreference                     ARMUnimplementedStruct `json:"routingPreference,omitempty"`
	SASPolicy                             ARMUnimplementedStruct `json:"sasPolicy,omitempty"`
	SupportsHttpsOnly                     *bool                  `json:"supportsHttpsOnly,omitempty"`
	Tags                                  map[string]string      `json:"tags"`
}

func (sa *ARMStorageAccount) Create(params ARMStorageAccountCreateParams) (*ARMStorageAccountProperties, error) {
	var out ARMStorageAccountProperties
	_, err := sa.PerformRequest(sa.ManagementURI(), ARMRequestSettings{
		Method: http.MethodPut,
		Body:   params,
	}, &out)
	return &out, err
}

func (sa *ARMStorageAccount) Delete() error {
	_, err := sa.PerformRequest(sa.ManagementURI(), ARMRequestSettings{
		Method: http.MethodDelete,
	}, nil)
	return err
}

const (
	ARMStorageAccountExpandGeoReplicationStats = "geoReplicationStats"
	ARMStorageAccountExpandBlobRestoreStatus   = "blobRestoreStatus"
)

// GetProperties pulls storage account properties; expand uses the above constants
func (sa *ARMStorageAccount) GetProperties(expand []string) (*ARMStorageAccountProperties, error) {
	query := make(url.Values)
	if expand != nil {
		query["$expand"] = expand
	}

	var out ARMStorageAccountProperties
	_, err := sa.PerformRequest(sa.ManagementURI(), ARMRequestSettings{
		Method: http.MethodGet,
	}, &out)
	return &out, err
}

// =========== Shared Types ===========

type ARMStorageAccountProperties struct {
	ExtendedLocation ARMExtendedLocation       `json:"extendedLocation"`
	ID               string                    `json:"id"`
	Identity         ARMStorageAccountIdentity `json:"identity"`
	Kind             azblob.AccountKindType    `json:"kind"`
	Location         string                    `json:"location"`
	Name             string                    `json:"name"`
	Properties       struct {
		AccessTier                            azblob.AccessTierType    `json:"accessTier"`
		AccountMigrationInProcess             bool                     `json:"accountMigrationInProcess"`
		AllowBlobPublicAccess                 bool                     `json:"allowBlobPublicAccess"`
		AllowCrossTenantReplication           bool                     `json:"allowCrossTenantReplication"`
		AllowSharedKeyAccess                  bool                     `json:"allowSharedKeyAccess"`
		AllowedCopyScope                      ARMUnimplementedStruct   `json:"allowedCopyScope"`
		AzureFilesIdentityBasedAuthentication ARMUnimplementedStruct   `json:"azureFilesIdentityBasedAuthentication"`
		BlobRestoreStatus                     ARMUnimplementedStruct   `json:"blobRestoreStatus"`
		CreationTime                          string                   `json:"creationTime"`
		CustomDomain                          ARMUnimplementedStruct   `json:"customDomain"`
		DefaultToOAuthAuthentication          bool                     `json:"defaultToOAuthAuthentication"`
		DNSEndpointType                       ARMUnimplementedStruct   `json:"dnsEndpointType"`
		Encryption                            ARMUnimplementedStruct   `json:"encryption"` // todo: maybe needed for CPK?
		FailoverInProgress                    bool                     `json:"failoverInProgress"`
		GeoReplicationStats                   ARMUnimplementedStruct   `json:"geoReplicationStats"`
		ImmutableStorageWithVersioning        ARMUnimplementedStruct   `json:"immutableStorageWithVersioning"` // todo: needed for testing version-level WORM
		IsHNSEnabled                          bool                     `json:"isHNSEnabled"`
		IsLocalUserEnabled                    bool                     `json:"isLocalUserEnabled"`
		IsNFSV3Enabled                        bool                     `json:"isNfsV3Enabled"`
		IsSFTPEnabled                         bool                     `json:"isSftpEnabled"`
		IsSKUConversionBlocked                bool                     `json:"isSkuConversionBlocked"`
		KeyCreationTime                       ARMUnimplementedStruct   `json:"keyCreationTime"`
		KeyPolicy                             ARMUnimplementedStruct   `json:"keyPolicy"`            // todo: CPK?
		LargeFileSharesState                  string                   `json:"largeFileSharesState"` // "Enabled" or "Disabled"
		LastGeoFailoverTime                   string                   `json:"lastGeoFailoverTime"`
		MinimumTLSVersion                     ARMUnimplementedStruct   `json:"minimumTLSVersion"`
		NetworkACLs                           ARMUnimplementedStruct   `json:"networkAcls"`
		PrimaryEndpoints                      ARMUnimplementedStruct   `json:"primaryEndpoints"`
		PrimaryLocation                       string                   `json:"primaryLocation"`
		PrivateEndpointConnections            []ARMUnimplementedStruct `json:"privateEndpointConnections"`
		ProvisioningState                     string                   `json:"provisioningState"`
		PublicNetworkAccess                   string                   `json:"publicNetworkAccess"` // "Enabled" or "Disabled"
		RoutingPreference                     ARMUnimplementedStruct   `json:"routingPreference"`
		SASPolicy                             ARMUnimplementedStruct   `json:"sasPolicy"`
		SecondaryEndpoints                    ARMUnimplementedStruct   `json:"secondaryEndpoints"` // todo: could test azcopy's ability to fail over?
		SecondaryLocation                     string                   `json:"secondaryLocation"`
		StatusOfPrimary                       string                   `json:"statusOfPrimary"`
		StatusOfSecondary                     string                   `json:"statusOfSecondary"`
		StorageAccountSkuConversionStatus     ARMUnimplementedStruct   `json:"storageAccountSkuConversionStatus"`
		SupportsHTTPSTrafficOnly              bool                     `json:"supportsHttpsTrafficOnly"`
	} `json:"properties"`
	Sku  ARMStorageAccountSKU `json:"sku"`
	Tags map[string]string    `json:"tags"`
	Type string               `json:"type"`
}

type ARMStorageAccountSKU struct {
	Name string `json:"name"`
	Tier string `json:"tier"`
}

var (
	// SKU names https://learn.microsoft.com/en-us/rest/api/storagerp/storage-accounts/create?tabs=HTTP#skuname
	ARMStorageAccountSKUPremiumLRS     = ARMStorageAccountSKU{"Premium_LRS", "Premium"}
	ARMStorageAccountSKUPremiumZRS     = ARMStorageAccountSKU{"Premium_ZRS", "Premium"}
	ARMStorageAccountSKUStandardGRS    = ARMStorageAccountSKU{"Standard_GRS", "Standard"}
	ARMStorageAccountSKUStandardGZRS   = ARMStorageAccountSKU{"Standard_GZRS", "Standard"}
	ARMStorageAccountSKUStandardLRS    = ARMStorageAccountSKU{"Standard_LRS", "Standard"}
	ARMStorageAccountSKUStandardRAGRS  = ARMStorageAccountSKU{"Standard_RAGRS", "Standard"}
	ARMStorageAccountSKUStandardRAGZRS = ARMStorageAccountSKU{"Standard_RAGZRS", "Standard"}
	ARMStorageAccountSKUStandardZRS    = ARMStorageAccountSKU{"Standard_ZRS", "Standard"}
)

type ARMExtendedLocation struct {
	Name string `json:"name"`
	Type string `json:"type"` // Can be "EdgeZone" or empty
}

type ARMStorageAccountIdentity struct {
	PrincipalID            string                                           `json:"principalId"`
	TenantID               string                                           `json:"tenantId"`
	Type                   string                                           `json:"type"` // https://learn.microsoft.com/en-us/rest/api/storagerp/storage-accounts/create?tabs=HTTP#identitytype
	UserAssignedIdentities map[string]ARMStorageAccountUserAssignedIdentity `json:"userAssignedIdentities"`
}

type ARMStorageAccountUserAssignedIdentity struct {
	ClientID    string `json:"clientId"`
	PrincipalID string `json:"principalId"`
}
