package e2etest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/azure-storage-azcopy/v10/common"
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

// GetResourceManager should not be called repeatedly; it makes calls to REST APIs and does not cache.
func (sa *ARMStorageAccount) GetResourceManager() (*AzureAccountResourceManager, error) {
	keyList, err := sa.GetKeys()
	if err != nil {
		return nil, fmt.Errorf("failed to get account keys: %w", err)
	}

	var acctKey string
	for _, v := range keyList.Keys { // todo: fallback to RO key
		if v.Permissions == ARMStorageAccountKeyPermissionFull || v.Permissions == "" {
			acctKey = v.Value
			break
		}
	}

	if acctKey == "" {
		return nil, fmt.Errorf("failed to find suitable account key; did you intentionally make it RO")
	}

	props, err := sa.GetProperties(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to poll account properties: %w", err)
	}

	var acctType AccountType
	switch {
	case strings.EqualFold(props.Sku.Tier, "Premium"):
		switch props.Kind {
		case service.AccountKindBlockBlobStorage: // both use the same kind
			acctType = common.Iff(props.Properties.IsHNSEnabled, EAccountType.PremiumHNSEnabled(), EAccountType.PremiumBlockBlobs())
		case service.AccountKindFileStorage:
			acctType = EAccountType.PremiumFileShares()
		case service.AccountKindStorageV2:
			acctType = EAccountType.PremiumPageBlobs()
		}
	case props.Properties.IsHNSEnabled:
		acctType = EAccountType.HierarchicalNamespaceEnabled()
	case strings.EqualFold(props.Sku.Tier, "Standard"):
		acctType = EAccountType.Standard()
	//	// Classic comes from Microsoft.ClassicStorage/storageAccounts, so, not possible here.
	//	// Managed Disks also won't appear here.
	default:
		return nil, fmt.Errorf("failed to assign an appropriate account type")
	}

	return &AzureAccountResourceManager{
		InternalAccountName: sa.AccountName,
		InternalAccountKey:  acctKey,
		InternalAccountType: acctType,
		ArmClient:           sa,
	}, nil
}

func (sa *ARMStorageAccount) PrepareRequest(reqSettings *ARMRequestSettings) {
	if reqSettings.Query == nil {
		reqSettings.Query = make(url.Values)
	}

	if !reqSettings.Query.Has("api-version") {
		reqSettings.Query.Add("api-version", "2023-01-01") // Attach default query
	}
}

// ARMStorageAccountCreateParams is the request body for https://learn.microsoft.com/en-us/rest/api/storagerp/storage-accounts/create?tabs=HTTP#storageaccount
type ARMStorageAccountCreateParams struct {
	Kind             service.AccountKind                `json:"kind"`
	Location         string                             `json:"location"`
	Sku              ARMStorageAccountSKU               `json:"sku"`
	ExtendedLocation *ARMExtendedLocation               `json:"extendedLocation,omitempty"`
	Identity         *ARMStorageAccountIdentity         `json:"identity,omitempty"`
	Properties       *ARMStorageAccountCreateProperties `json:"properties,omitempty"`
}

// ARMStorageAccountCreateProperties implements a portion of ARMStorageAccountCreateParams.
// https://learn.microsoft.com/en-us/rest/api/storagerp/storage-accounts/create?tabs=HTTP#storageaccount
type ARMStorageAccountCreateProperties struct { // json.RawMessage(s) are used as filler, if needed for one-offs
	AccessTier                            *blob.AccessTier  `json:"accessTier,omitempty"`
	AllowBlobPublicAccess                 *bool             `json:"allowBlobPublicAccess,omitempty"`
	AllowCrossTenantReplication           *bool             `json:"allowCrossTenantReplication,omitempty"`
	AllowSharedKeyAccess                  *bool             `json:"allowSharedKeyAccess,omitempty"`
	AllowedCopyScope                      json.RawMessage   `json:"allowedCopyScope,omitempty"`
	AzureFilesIdentityBasedAuthentication json.RawMessage   `json:"azureFilesIdentityBasedAuthentication,omitempty"`
	CustomDomain                          json.RawMessage   `json:"customDomain,omitempty"`
	DefaultToOAuthAuthentication          *bool             `json:"defaultToOAuthAuthentication,omitempty"`
	DNSEndpointType                       json.RawMessage   `json:"dnsEndpointType,omitempty"`
	Encryption                            json.RawMessage   `json:"encryption,omitempty"`                     // todo cpk?
	ImmutableStorageWithVersioning        json.RawMessage   `json:"immutableStorageWithVersioning,omitempty"` // todo version level WORM
	IsHnsEnabled                          *bool             `json:"isHnsEnabled,omitempty"`
	IsLocalUserEnabled                    *bool             `json:"isLocalUserEnabled,omitempty"`
	IsNfsV3Enabled                        *bool             `json:"isNfsV3Enabled,omitempty"`
	IsSftpEnabled                         *bool             `json:"isSftpEnabled,omitempty"`
	KeyPolicy                             json.RawMessage   `json:"keyPolicy,omitempty"`
	LargeFileSharesState                  *string           `json:"largeFileSharesState,omitempty"` // "Enabled" or "Disabled"
	MinimumTLSVersion                     *string           `json:"minimumTLSVersion,omitempty"`
	NetworkACLs                           json.RawMessage   `json:"networkAcls,omitempty"`
	PublicNetworkAccess                   *string           `json:"publicNetworkAccess,omitempty"` // "Enabled" or "Disabled"
	RoutingPreference                     json.RawMessage   `json:"routingPreference,omitempty"`
	SASPolicy                             json.RawMessage   `json:"sasPolicy,omitempty"`
	SupportsHttpsOnly                     *bool             `json:"supportsHttpsOnly,omitempty"`
	Tags                                  map[string]string `json:"tags"`
}

func (sa *ARMStorageAccount) Create(params ARMStorageAccountCreateParams) (*ARMStorageAccountProperties, error) {
	var out ARMStorageAccountProperties
	_, err := PerformRequest(sa, ARMRequestSettings{
		Method: http.MethodPut,
		Body:   params,
	}, &out)
	return &out, err
}

func (sa *ARMStorageAccount) Delete() error {
	_, err := PerformRequest[any](sa, ARMRequestSettings{
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
	_, err := PerformRequest(sa, ARMRequestSettings{
		Method: http.MethodGet,
	}, &out)
	return &out, err
}

func (sa *ARMStorageAccount) GetKeys() (*ARMStorageAccountListKeysResult, error) { // Kerberos keys can be listed, but AzCopy doesn't currently support this.
	var resp ARMStorageAccountListKeysResult

	_, err := PerformRequest(sa, ARMRequestSettings{
		Method:        http.MethodPost,
		PathExtension: "listKeys",
	}, &resp)
	return &resp, err
}

// =========== Shared Types ===========

type ARMStorageAccountProperties struct {
	ExtendedLocation ARMExtendedLocation       `json:"extendedLocation"`
	ID               string                    `json:"id"`
	Identity         ARMStorageAccountIdentity `json:"identity"`
	Kind             service.AccountKind       `json:"kind"`
	Location         string                    `json:"location"`
	Name             string                    `json:"name"`
	Properties       struct {
		AccessTier                            blob.AccessTier   `json:"accessTier"`
		AccountMigrationInProcess             bool              `json:"accountMigrationInProcess"`
		AllowBlobPublicAccess                 bool              `json:"allowBlobPublicAccess"`
		AllowCrossTenantReplication           bool              `json:"allowCrossTenantReplication"`
		AllowSharedKeyAccess                  bool              `json:"allowSharedKeyAccess"`
		AllowedCopyScope                      json.RawMessage   `json:"allowedCopyScope"`
		AzureFilesIdentityBasedAuthentication json.RawMessage   `json:"azureFilesIdentityBasedAuthentication"`
		BlobRestoreStatus                     json.RawMessage   `json:"blobRestoreStatus"`
		CreationTime                          string            `json:"creationTime"`
		CustomDomain                          json.RawMessage   `json:"customDomain"`
		DefaultToOAuthAuthentication          bool              `json:"defaultToOAuthAuthentication"`
		DNSEndpointType                       json.RawMessage   `json:"dnsEndpointType"`
		Encryption                            json.RawMessage   `json:"encryption"` // todo: maybe needed for CPK?
		FailoverInProgress                    bool              `json:"failoverInProgress"`
		GeoReplicationStats                   json.RawMessage   `json:"geoReplicationStats"`
		ImmutableStorageWithVersioning        json.RawMessage   `json:"immutableStorageWithVersioning"` // todo: needed for testing version-level WORM
		IsHNSEnabled                          bool              `json:"isHNSEnabled"`
		IsLocalUserEnabled                    bool              `json:"isLocalUserEnabled"`
		IsNFSV3Enabled                        bool              `json:"isNfsV3Enabled"`
		IsSFTPEnabled                         bool              `json:"isSftpEnabled"`
		IsSKUConversionBlocked                bool              `json:"isSkuConversionBlocked"`
		KeyCreationTime                       json.RawMessage   `json:"keyCreationTime"`
		KeyPolicy                             json.RawMessage   `json:"keyPolicy"`            // todo: CPK?
		LargeFileSharesState                  string            `json:"largeFileSharesState"` // "Enabled" or "Disabled"
		LastGeoFailoverTime                   string            `json:"lastGeoFailoverTime"`
		MinimumTLSVersion                     json.RawMessage   `json:"minimumTLSVersion"`
		NetworkACLs                           json.RawMessage   `json:"networkAcls"`
		PrimaryEndpoints                      json.RawMessage   `json:"primaryEndpoints"`
		PrimaryLocation                       string            `json:"primaryLocation"`
		PrivateEndpointConnections            []json.RawMessage `json:"privateEndpointConnections"`
		ProvisioningState                     string            `json:"provisioningState"`
		PublicNetworkAccess                   string            `json:"publicNetworkAccess"` // "Enabled" or "Disabled"
		RoutingPreference                     json.RawMessage   `json:"routingPreference"`
		SASPolicy                             json.RawMessage   `json:"sasPolicy"`
		SecondaryEndpoints                    json.RawMessage   `json:"secondaryEndpoints"` // todo: could test azcopy's ability to fail over?
		SecondaryLocation                     string            `json:"secondaryLocation"`
		StatusOfPrimary                       string            `json:"statusOfPrimary"`
		StatusOfSecondary                     string            `json:"statusOfSecondary"`
		StorageAccountSkuConversionStatus     json.RawMessage   `json:"storageAccountSkuConversionStatus"`
		SupportsHTTPSTrafficOnly              bool              `json:"supportsHttpsTrafficOnly"`
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

type ARMStorageAccountListKeysResult struct {
	Keys []ARMStorageAccountKey `json:"keys"`
}

type ARMStorageAccountKey struct {
	CreationTime string `json:"creationTime"`
	KeyName      string `json:"keyName"`
	Permissions  string `json:"permissions"`
	Value        string `json:"value"`
}

const (
	ARMStorageAccountKeyPermissionReadOnly = "Read"
	ARMStorageAccountKeyPermissionFull     = "Full"
)
