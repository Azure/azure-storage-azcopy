package e2etest

import (
	"io"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/lease"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/pageblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"
	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type GetURIOptions struct {
	RemoteOpts RemoteURIOpts
	LocalOpts  LocalURIOpts
	AzureOpts  AzureURIOpts
	// The wildcard string to append to the end of a resource URI.
	Wildcard string
}

type RemoteURIOpts struct {
	// Defaults as https
	Scheme string
}

type AzureURIOpts struct {
	// Must be manually specified
	WithSAS bool
	// Defaults to a resource-level specific minimally permissioned SAS token.
	SASValues GenericSignatureValues
}

type LocalURIOpts struct {
	PreferUNCPath bool
}

type ResourceManager interface {
	Location() common.Location
	Level() cmd.LocationLevel

	// URI gets the resource "URI", either a local path or a remote URL.
	// This should panic if things fail or make no sense, as a sanity check to the test author.
	URI(opts ...GetURIOptions) string

	// Parent specifies the parent resource manager, for the purposes of building a tree.
	// Can return nil, indicating this is the root of the tree.
	Parent() ResourceManager
	// Account specifies the parent account.
	// Can return nil, indicating there is no associated account
	Account() AccountResourceManager

	/*Canon specifies an object's canonical location in the resource tree created by a test.

	A Canon string is a `/` delimited list of parents up to the final element, representing the resource itself.
	The format goes
	<account>/<location>/<container>/<object>

	For locations where an account does not exist (e.g. local), substitute account with "accountless".
	e.g. accountless/Local/<tmpdirname>/<object>

	For flat namespaces, e.g. raw blob, / is ignored past objects, as it gets no more granular.
	*/
	Canon() string
}

type RemoteResourceManager interface {
	ResourceManager

	// ValidAuthTypes specifies acceptable auth types to use
	ValidAuthTypes() ExplicitCredentialTypes
	// DefaultAuthType should return the resource's logical default auth type (e.g. SAS)
	DefaultAuthType() ExplicitCredentialTypes
	// WithSpecificAuthType should return a copy of itself with a specific auth type, intended for RunAzCopy
	WithSpecificAuthType(cred ExplicitCredentialTypes, a Asserter, opts ...CreateAzCopyTargetOptions) AzCopyTarget
	// ResourceClient attempts to return the native resource client, and should be wrangled by a caller knowing what they're expecting.
	ResourceClient() any
}

func TryApplySpecificAuthType(rm ResourceManager, cred ExplicitCredentialTypes, a Asserter, opts ...CreateAzCopyTargetOptions) AzCopyTarget {
	if rrm, ok := rm.(RemoteResourceManager); ok {
		return rrm.WithSpecificAuthType(cred, a, opts...)
	}

	return CreateAzCopyTarget(rm, EExplicitCredentialType.None(), a, opts...)
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

func (e ExplicitCredentialTypes) String() string {
	if e == 0 {
		return "None"
	}

	out := "{"
	enumStr := []string{
		"PublicAuth",
		"SASToken",
		"OAuth",
		"AccountKey",
		"GCP",
		"S3",
	}

	for idx, str := range enumStr {
		if e.Includes(1 << idx) {
			if len(out) > 1 {
				out += ","
			}

			out += str
		}
	}

	out += "}"

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
	AvailableAuthTypes() ExplicitCredentialTypes
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
	ContainerName() string
	ObjectName() string
	// Create attempts to create an object. Should overwrite objects if they already exist.
	// It is expected to attempt to track object creation.
	// It is also expected to create parents, if required.
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
	HardlinkedFileName() string
	ReadLink(a Asserter) string
}

type ObjectProperties struct {
	EntityType       common.EntityType
	HTTPHeaders      contentHeaders
	Metadata         common.Metadata
	LastModifiedTime *time.Time

	BlobProperties     BlobProperties
	BlobFSProperties   BlobFSProperties
	FileProperties     FileProperties
	FileNFSProperties  *FileNFSProperties
	FileNFSPermissions *FileNFSPermissions
	SymlinkedFileName  string
	HardLinkedFileName string
}

type BlobProperties struct {
	Type                *blob.BlobType
	Tags                map[string]string
	BlockBlobAccessTier *blob.AccessTier
	PageBlobAccessTier  *pageblob.PremiumPageBlobAccessTier
	VersionId           *string
	LeaseState          *lease.StateType
	LeaseDuration       *lease.DurationType
	LeaseStatus         *lease.StatusType
	ArchiveStatus       *blob.ArchiveStatus
}

type BlobFSProperties struct {
	Permissions *string
	Owner       *string
	Group       *string
	ACL         *string
}

type FileProperties struct {
	FileAttributes *string
	// ChangeTime, though available on the Files service is not writeable locally, or queryable.
	// We also just do not persist it, even on Files at the moment.
	// Hence, we do not test ChangeTime.
	FileCreationTime  *time.Time
	FileLastWriteTime *time.Time
	FilePermissions   *string
	LastModifiedTime  *time.Time
}

type FileNFSProperties struct {
	FileCreationTime  *time.Time
	FileLastWriteTime *time.Time
}

type FileNFSPermissions struct {
	Owner    *string
	Group    *string
	FileMode *string
}

func (f FileProperties) hasCustomTimes() bool {
	return f.FileCreationTime != nil || f.FileLastWriteTime != nil
}
