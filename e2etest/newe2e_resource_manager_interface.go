package e2etest

import (
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/pageblob"
	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"time"
)

type ResourceManager interface {
	Location() common.Location
	Level() cmd.LocationLevel
}

type RemoteResourceManager interface {
	ValidAuthTypes() ExplicitCredentialTypes
	ResourceClient() any // Should be wrangled by caller, check internalClient field of resource manager
}

// ExplicitCredentialTypes defines a more explicit enum for credential types as AzCopy's internal definition is very loose (e.g. Anonymous can be public or SAS); accepts the URI as-is.
// ExplicitCredentialTypes is a set of bitflags indicating intended credential types for a test and available credential types for resources
type ExplicitCredentialTypes uint8

const EExplicitCredentialType ExplicitCredentialTypes = 0

func (ExplicitCredentialTypes) None() ExplicitCredentialTypes       { return 0 } // Used if the goal is to trigger an auth failure
func (ExplicitCredentialTypes) PublicAuth() ExplicitCredentialTypes { return 1 }
func (ExplicitCredentialTypes) SASToken() ExplicitCredentialTypes   { return 1 << 1 }
func (ExplicitCredentialTypes) OAuth() ExplicitCredentialTypes      { return 1 << 2 }
func (ExplicitCredentialTypes) AcctKey() ExplicitCredentialTypes    { return 1 << 3 }
func (ExplicitCredentialTypes) GCP() ExplicitCredentialTypes        { return 1 << 4 }
func (ExplicitCredentialTypes) S3() ExplicitCredentialTypes         { return 1 << 5 }

func (e ExplicitCredentialTypes) Includes(x ExplicitCredentialTypes) bool {
	return e&x == x
}

func (e ExplicitCredentialTypes) With(x ...ExplicitCredentialTypes) ExplicitCredentialTypes {
	out := e
	for _, v := range x {
		out |= v
	}
	return out
}

type PropertiesAvailability uint8

const (
	PropertiesAvailabilityNone PropertiesAvailability = iota
	PropertiesAvailabilityReadOnly
	PropertiesAvailabilityReadWrite
)

/*
Resource managers implement the most generic common expectation of features across services.
If you get more complex, you may want to use GetTypeOrAssert[T] or GetTypeOrZero[T] to wrangle the underlying resource manager,
or wrangle to RemoteResourceManager and call ResourceClient() to pull the actual client.

Check newe2e_resource_managers_*.go for the implementation(s) of resource managers.
*/

// AccountResourceManager manages an account.
type AccountResourceManager interface {
	AccountName() string
	AccountType() AccountType
	AvailableServices() []common.Location
	GetService(Asserter, common.Location) ServiceResourceManager
}

type ServiceResourceManager interface {
	ResourceManager
	RemoteResourceManager

	ListContainers(a Asserter) []string
	GetContainer(string) ContainerResourceManager
	IsHierarchical() bool
}

type ContainerResourceManager interface {
	ResourceManager

	ContainerName() string
	Create(a Asserter)
	Delete(a Asserter)
	// ListObjects treats prefixOrDirectory as a prefix when in a non-hierarchical service, and as a directory in a hierarchical service.
	// The map will be the real path, relative to container root, not to prefix/directory.
	ListObjects(a Asserter, prefixOrDirectory string, recursive bool) map[string]ObjectProperties
	GetObject(a Asserter, path string, eType common.EntityType) ObjectResourceManager
}

type ObjectResourceManager interface {
	ResourceManager

	EntityType() common.EntityType
	Create(a Asserter, body ObjectContentContainer, properties ObjectProperties)

	// ListChildren will fail if EntityType is not a folder and the service is hierarchical.
	// The map will be relative to the object.
	ListChildren(a Asserter, recursive bool) map[string]ObjectProperties

	GetProperties(a Asserter) ObjectProperties

	SetHTTPHeaders(a Asserter, h contentHeaders)
	SetMetadata(a Asserter, metadata common.Metadata)
	SetObjectProperties(a Asserter, props ObjectProperties)
}

type ObjectProperties struct {
	EntityType  common.EntityType
	HTTPHeaders contentHeaders
	Metadata    common.Metadata

	BlobProperties   BlobProperties
	BlobFSProperties BlobFSProperties
	FileProperties   FileProperties
}

type BlobProperties struct {
	Type                *blob.BlobType
	Tags                map[string]string // "Tags"
	BlockBlobAccessTier *blob.AccessTier
	PageBlobAccessTier  *pageblob.PremiumPageBlobAccessTier
}

type BlobFSProperties struct {
	Permissions *string
	Owner       *string
	Group       *string
	ACL         *string
}

type FileProperties struct {
	FileAttributes    *string
	FileChangeTime    *time.Time
	FileCreationTime  *time.Time
	FileLastWriteTime *time.Time
	FilePermissions   *string
}
