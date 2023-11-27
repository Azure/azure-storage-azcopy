package e2etest

import (
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/pageblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"
	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"io"
	"time"
)

type ResourceManager interface {
	Location() common.Location
	Level() cmd.LocationLevel

	// URI gets the resource "URI", either a local path or a remote URL.
	// withSas is a debatably included flag, but can either be ignored or asserted against.
	// for our primary Azure resources, it's always applicable. Doesn't make sense for GCP/S3/Local though.
	// SAS tokens will automagically be generated for the next 24 hours, I (Adele) don't think the test suite should be running for longer.
	URI(a Asserter, withSas bool) string

	// Parent specifies the parent resource manager, for the purposes of building a tree.
	// Can return nil, indicating this is the root of the tree.
	Parent() ResourceManager
	// Account specifies the parent account.
	// Can return nil, indicating there is no associated account
	Account() AccountResourceManager
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

func (e ExplicitCredentialTypes) Count() int {
	if e == 0 {
		return 0
	}

	out := 0
	validTypes := []ExplicitCredentialTypes{ // todo: automate with reflection
		EExplicitCredentialType.PublicAuth(),
		EExplicitCredentialType.SASToken(),
		EExplicitCredentialType.OAuth(),
		EExplicitCredentialType.AcctKey(),
		EExplicitCredentialType.GCP(),
		EExplicitCredentialType.S3(),
	}

	for _, v := range validTypes {
		if e&v == v {
			out++
		}
	}

	return out
}

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
	// Create should ignore errors of existing containers. It is expected to attempt to track container creation.
	Create(a Asserter, props ContainerProperties)
	// GetProperties polls container properties.
	GetProperties(a Asserter) ContainerProperties
	// Delete should ignore errors of nonexistent containers
	Delete(a Asserter)
	// ListObjects treats prefixOrDirectory as a prefix when in a non-hierarchical service, and as a directory in a hierarchical service.
	// The map will be the real path, relative to container root, not to prefix/directory.
	ListObjects(a Asserter, prefixOrDirectory string, recursive bool) map[string]ObjectProperties
	// GetObject scales up to a target ObjectResourceManager
	GetObject(a Asserter, path string, eType common.EntityType) ObjectResourceManager
	// Exists determines if the container in question exists
	Exists() bool
}

type ContainerProperties struct {
	// Used by multiple services, makes sense to have as a general option.
	// When specified by user: Nil = unspecified/unverified
	// When returned by manager: Nil = unsupported
	Metadata common.Metadata

	// BlobContainerProperties is shared with BlobFS, because they're the same resource and share parameters
	BlobContainerProperties BlobContainerProperties
	FileContainerProperties FileContainerProperties
}

type BlobContainerProperties struct {
	Access       *container.PublicAccessType
	CPKScopeInfo *container.CPKScopeInfo
}

type FileContainerProperties struct {
	AccessTier       *share.AccessTier
	EnabledProtocols *string
	Quota            *int32
	RootSquash       *share.RootSquash
}

type ObjectResourceManager interface {
	ResourceManager

	EntityType() common.EntityType
	// Create attempts to create an object. Should overwrite objects if they already exist. It is expected to attempt to track object creation.
	Create(a Asserter, body ObjectContentContainer, properties ObjectProperties)
	// Delete attempts to delete an object. NotFound type errors are ignored.
	Delete(a Asserter)

	// ListChildren will fail if EntityType is not a folder and the service is hierarchical.
	// The map will be relative to the object.
	ListChildren(a Asserter, recursive bool) map[string]ObjectProperties

	GetProperties(a Asserter) ObjectProperties

	SetHTTPHeaders(a Asserter, h contentHeaders)
	SetMetadata(a Asserter, metadata common.Metadata)
	SetObjectProperties(a Asserter, props ObjectProperties)

	Download(a Asserter) io.ReadSeeker

	// Exists determines if the object in question exists
	Exists() bool
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
	Tags                map[string]string
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
