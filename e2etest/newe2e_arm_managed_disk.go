package e2etest

import (
	"net/http"
	"net/url"
	"path"
)

type ARMManagedDisk struct {
	ParentSubject[*ARMResourceGroup]
	DiskName string
}

func (md *ARMManagedDisk) CanonicalPath() string {
	return path.Join(md.ParentSubject.CanonicalPath(), "providers/Microsoft.Compute/disks", md.DiskName)
}

func (md *ARMManagedDisk) PrepareRequest(reqSettings *ARMRequestSettings) {
	if reqSettings.Query == nil {
		reqSettings.Query = make(url.Values)
	}

	if !reqSettings.Query.Has("api-version") {
		reqSettings.Query.Add("api-version", "2021-12-01") // Attach default query
	}
}

type ARMManagedDiskCreateOrUpdateParams struct { // https://learn.microsoft.com/en-us/rest/api/compute/disks/create-or-update?tabs=HTTP#request-body
	Location         string                      `json:"location"`
	ExtendedLocation *ARMExtendedLocation        `json:"extendedLocation,omitempty"`
	Sku              *ARMManagedDiskSku          `json:"sku,omitempty"`
	Properties       ARMManagedDiskPutProperties `json:"properties"` // Has mandatory field CreationData
}

type ARMManagedDiskPutProperties struct {
	CreationData                 ARMManagedDiskCreationData `json:"creationData"`
	BurstingEnabled              *bool                      `json:"burstingEnabled,omitempty"`
	CompletionPercent            string                     `json:"completionPercent"`
	DataAccessAuthMode           *string                    `json:"dataAccessAuthMode,omitempty"` // AzureActiveDirectory or None
	DiskAccessId                 *string                    `json:"diskAccessId,omitempty"`
	DiskIOPSReadWrite            *uint64                    `json:"diskIOPSReadWrite,omitempty"`
	DiskIOPSReadOnly             *uint64                    `json:"diskIOPSReadOnly,omitempty"`
	DiskMBpsReadOnly             *uint64                    `json:"diskMBpsReadOnly,omitempty"`
	DiskMBpsReadWrite            *uint64                    `json:"diskMBpsReadWrite,omitempty"`
	DiskSizeGB                   *uint                      `json:"diskSizeGB,omitempty"` // Mandatory if CreationData.CreateOption is empty
	Encryption                   ARMUnimplementedStruct     `json:"encryption,omitempty"` // todo?
	EncryptionSettingsCollection ARMUnimplementedStruct     `json:"encryptionSettingsCollection,omitempty"`
	HyperVGeneration             *string                    `json:"hyperVGeneration,omitempty"` // V1 or V2
	MaxShares                    *uint                      `json:"maxShares,omitempty"`        // Max VM attachments
	NetworkAccessPolicy          ARMUnimplementedStruct     `json:"networkAccessPolicy,omitempty"`
	OSType                       *string                    `json:"OSType,omitempty"`              // "Linux" or "Windows"
	PublicNetworkAccess          *string                    `json:"publicNetworkAccess,omitempty"` // "Enabled" or "Disabled"
	PurchasePlan                 ARMUnimplementedStruct     `json:"purchasePlan,omitempty"`
	SecurityProfile              ARMUnimplementedStruct     `json:"securityProfile,omitempty"`
	SupportedCapabilities        ARMUnimplementedStruct     `json:"supportedCapabilities,omitempty"`
	SupportsHibernation          *bool                      `json:"supportsHibernation,omitempty"`
	Tier                         *string                    `json:"tier,omitempty"` // Perf tier https://azure.microsoft.com/en-us/pricing/details/managed-disks/ does not apply to ultra
}

type ARMManagedDiskCreationData struct {
	CreateOption          *string                `json:"createOption"`
	LogicalSectorSize     *uint                  `json:"logicalSectorSize,omitempty"`     // 512-4096; 4096 is default
	GalleryImageReference ARMUnimplementedStruct `json:"galleryImageReference,omitempty"` // ImageDiskReference
	SecurityDataUri       *string                `json:"securityDataUri,omitempty"`       // ImportSecure
	SourceResourceId      *string                `json:"sourceResourceId,omitempty"`      // Copy
	SourceUniqueId        *string                `json:"sourceUniqueId,omitempty"`        // also Copy?
	SourceUri             *string                `json:"sourceUri,omitempty"`             // Import
	StorageAccountId      *string                `json:"storageAccountId,omitempty"`      // Required on Import
	UploadSizeBytes       *uint64                `json:"uploadSizeBytes,omitempty"`
}

const (
	ARMManagedDiskCreateOptionAttach               = "Attach"
	ARMManagedDiskCreateOptionCopy                 = "Copy"
	ARMManagedDiskCreateOptionCopyStart            = "CopyStart"
	ARMManagedDiskCreateOptionEmpty                = "Empty"
	ARMManagedDiskCreateOptionFromImage            = "FromImage"
	ARMManagedDiskCreateOptionImport               = "Import"
	ARMManagedDiskCreateOptionImportSecure         = "ImportSecure"
	ARMManagedDiskCreateOptionRestore              = "Restore"
	ARMManagedDiskCreateOptionUpload               = "Upload"
	ARMManagedDiskCreateOptionUploadPreparedSecure = "UploadPreparedSecure"
)

func (md *ARMManagedDisk) CreateOrUpdate(params ARMManagedDiskCreateOrUpdateParams) (*ARMManagedDiskInfo, error) {
	var out ARMManagedDiskInfo
	_, err := PerformRequest(md, ARMRequestSettings{ // https://learn.microsoft.com/en-us/rest/api/compute/disks/create-or-update?tabs=HTTP
		Method: http.MethodPut,
		Body:   params,
	}, &out)

	return &out, err
}

func (md *ARMManagedDisk) Delete() error {
	_, err := PerformRequest[any](md, ARMRequestSettings{
		Method: http.MethodDelete,
	}, nil)

	return err
}

func (md *ARMManagedDisk) Get() (*ARMManagedDiskInfo, error) { // https://learn.microsoft.com/en-us/rest/api/compute/disks/get?tabs=HTTP
	var out ARMManagedDiskInfo
	_, err := PerformRequest(md, ARMRequestSettings{
		Method: http.MethodGet,
	}, &out)

	return &out, err
}

type ARMManagedDiskGrantAccessParams struct {
	AccessLevel              string `json:"access"` // "Read" or "Write"
	DurationInSeconds        uint64 `json:"durationInSeconds"`
	GetSecureVMGuestStateSAS *bool  `json:"getSecureVMGuestStateSAS,omitempty"`
}

type ARMManagedDiskAccessURI struct {
	AccessSAS             string `json:"accessSAS"`
	SecurityDataAccessSAS string `json:"securityDataAccessSAS"`
}

func (md *ARMManagedDisk) GrantAccess(params ARMManagedDiskGrantAccessParams) (*ARMManagedDiskAccessURI, error) { // https://learn.microsoft.com/en-us/rest/api/compute/disks/grant-access?tabs=HTTP
	var out ARMManagedDiskAccessURI
	_, err := PerformRequest(md, ARMRequestSettings{
		Method:        http.MethodGet,
		Body:          params,
		PathExtension: "beginGetAccess",
	}, &out)
	return &out, err
}

func (md *ARMManagedDisk) RevokeAccess() error {
	_, err := PerformRequest[any](md, ARMRequestSettings{
		Method: http.MethodPost,
	}, nil)
	return err
}

// ========== Shared Structs ==========

type ARMManagedDiskSku struct {
	Name string `json:"name"`
	Tier string `json:"tier"`
}

var (
	ARMManagedDiskSkuPremiumLrs     = ARMManagedDiskSku{Name: "Premium_LRS"}
	ARMManagedDiskSkuPremiumZrs     = ARMManagedDiskSku{Name: "Premium_ZRS"}
	ARMManagedDiskSkuStandardSsdLrs = ARMManagedDiskSku{Name: "StandardSSD_LRS"}
	ARMManagedDiskSkuStandardSsdZrs = ARMManagedDiskSku{Name: "StandardSSD_ZRS"}
	ARMManagedDiskSkuStandardLrs    = ARMManagedDiskSku{Name: "Standard_LRS"}
	ARMManagedDiskSkuUltraSsdLrs    = ARMManagedDiskSku{Name: "UltraSSD_LRS"}
)

type ARMManagedDiskInfo struct {
	Id                string                      `json:"id"`
	Location          string                      `json:"location"`
	ManagedBy         string                      `json:"managedBy"`
	ManagedByExtended []string                    `json:"managedByExtended"`
	Name              string                      `json:"name"`
	ExtendedLocation  ARMExtendedLocation         `json:"extendedLocation"`
	Sku               ARMManagedDiskSku           `json:"sku"`
	Tags              map[string]string           `json:"tags"`
	Zones             []string                    `json:"zones"`
	Properties        ARMManagedDiskPutProperties `json:"properties"` // Has mandatory field CreationData
}
