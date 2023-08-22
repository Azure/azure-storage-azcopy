package e2etest

import (
	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

type ResourceManager interface {
	Location() common.Location
	Level() cmd.LocationLevel
}

type RemoteResourceManager interface {
	ValidAuthTypes() ExplicitCredentialTypes
	ResourceClient() any // Should be wrangled by caller
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

type AzureAccountResourceManager interface {
	GetServiceResourceManager(Location common.Location) ServiceResourceManager
	AccountType() AccountType
}

type ServiceResourceManager interface {
	RemoteResourceManager
	ResourceManager
	CreateContainer(name string, options *CreateContainerOptions) (ContainerResourceManager, error)
	DeleteContainer(name string, options *DeleteContainerOptions) error
	GetContainer(name string) ContainerResourceManager
	/*
		todo: do we need to handle container properties for any tests?
		For Files, we can just treat "/" as root and pull the SMB properties that way.
		But that's working around the issue. Should we choose to handle it properly? Do we think we'll need it in the future?
	*/
}

type CreateContainerOptions struct {
	// ResourceSpecificOptions provides a space for a struct intended to be read uniquely by a specific intended ContainerResourceManager.
	// The ServiceResourceManager should document clearly what struct is required in the documentation of each function.
	ResourceSpecificOptions any
}

type DeleteContainerOptions struct {
	// ResourceSpecificOptions provides a space for a struct intended to be read uniquely by a specific intended ContainerResourceManager.
	// The ServiceResourceManager should document clearly what struct is required in the documentation of each function.
	ResourceSpecificOptions any
}

type ContainerResourceManager interface {
	// Local directories will be treated as ContainerResourceManager(s), and as such ContainerResourceManager(s) aren't inherently RemoteResourceManager(s).
	ResourceManager
	Create(path string, entityType common.EntityType, options *CreateObjectOptions) error
	Read(path string, options *ReadObjectOptions) ([]byte, error)
	GetProperties(path string, options *GetObjectPropertiesOptions) (GenericObjectProperties, error)
	SetProperties(path string, props GenericObjectProperties, options *SetObjectPropertiesOptions) error
	Delete(path string, options *DeleteObjectProperties) error
}

type CreateObjectOptions struct {
	// Overwrite determines if we're replacing the content entirely
	Overwrite bool
	// Defaults to NewRandomObjectContainer(1024)
	Content ObjectContentContainer

	// Optionals
	Metadata map[string]string
	Headers  *contentHeaders

	// ResourceSpecificOptions provides a space for a struct intended to be read uniquely by a specific intended ContainerResourceManager.
	// The ContainerResourceManager should document clearly what struct is required in the documentation of each function.
	ResourceSpecificOptions any
}

// For now, Read will just be plainly implemented.
type ReadObjectOptions struct {
	offset, count int64 // default 0, eof

	// ResourceSpecificOptions provides a space for a struct intended to be read uniquely by a specific intended ContainerResourceManager.
	// The ContainerResourceManager should document clearly what struct is required in the documentation of each function.
	ResourceSpecificOptions any
}

type GenericObjectProperties struct {
	headers  common.ResourceHTTPHeaders
	metadata common.Metadata

	ResourceSpecificProperties any // Filled with e.g. BlobObjectProperties
	OriginalResponse           any // Original response struct for extended handling; not required for setproperties
}

// BlobObjectProperties is a struct to fill GenericObjectProperties' ResourceSpecificProperties.
type BlobObjectProperties struct {
	BlobType   common.BlobType
	AccessTier azblob.AccessTierType

	// Read-only
	LeaseStatus   azblob.LeaseStatusType
	LeaseDuration azblob.LeaseDurationType
	LeaseState    azblob.LeaseStateType
	ArchiveStatus azblob.ArchiveStatusType
}

// For now, Read will just be plainly implemented.
type GetObjectPropertiesOptions struct {
	// ResourceSpecificOptions provides a space for a struct intended to be read uniquely by a specific intended ContainerResourceManager.
	// The ContainerResourceManager should document clearly what struct is required in the documentation of each function.
	ResourceSpecificOptions any
}

type SetObjectPropertiesOptions struct {
	// ResourceSpecificOptions provides a space for a struct intended to be read uniquely by a specific intended ContainerResourceManager.
	// The ContainerResourceManager should document clearly what struct is required in the documentation of each function.
	ResourceSpecificOptions any
}

type DeleteObjectProperties struct {
	// ResourceSpecificOptions provides a space for a struct intended to be read uniquely by a specific intended ContainerResourceManager.
	// The ContainerResourceManager should document clearly what struct is required in the documentation of each function.
	ResourceSpecificOptions any
}
