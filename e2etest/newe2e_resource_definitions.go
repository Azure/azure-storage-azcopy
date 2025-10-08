package e2etest

import (
	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"github.com/google/uuid"
)

// ResourceDefinition itself exists to loosely accept the handful of relevant types as a part of CreateResource and ValidateResource.
type ResourceDefinition interface {
	// DefinitionTarget returns the location level this definition applies at.
	DefinitionTarget() azcopy.LocationLevel
	// GenerateAdoptiveParent is designed for scaling a definition down to a root resource for creation
	GenerateAdoptiveParent(a Asserter) ResourceDefinition
	// MatchAdoptiveChild scales a resource of the same level up one level to the real definition level
	MatchAdoptiveChild(a Asserter, target ResourceManager) (ResourceManager, ResourceDefinition)
	// ApplyDefinition manages tree traversal by itself. applicationFunctions should realistically be creation of the underlying resource or validation.
	ApplyDefinition(a Asserter, target ResourceManager, applicationFunctions map[azcopy.LocationLevel]func(Asserter, ResourceManager, ResourceDefinition))
	// ShouldExist determines whether the resource should exist.
	// This should be checked in functions called by ApplyDefinition, as it is intended to alter creation and validation behavior.
	ShouldExist() bool
}

type MatchedResourceDefinition[T ResourceManager] interface {
	ResourceDefinition

	resourceDefinition() T
}

type ResourceDefinitionService struct {
	//Location   common.Location // todo do we want/need this? does it make sense? CreateResource currently takes a ResourceManager, which can't target an AccountResourceManager because definitions differ
	Containers map[string]ResourceDefinitionContainer
}

func (r ResourceDefinitionService) GenerateAdoptiveParent(a Asserter) ResourceDefinition {
	a.HelperMarker().Helper()
	a.Error("Cannot generate account definition (yet)")

	return nil
}

func (r ResourceDefinitionService) MatchAdoptiveChild(a Asserter, target ResourceManager) (ResourceManager, ResourceDefinition) {
	a.HelperMarker().Helper()
	targetSvc, ok := target.(ServiceResourceManager)
	a.AssertNow("adoptive parent definitions must match the level of the target they're finding a child for", Equal{}, ok, true)
	a.AssertNow("adoptive parent definitions can only have one container", Equal{}, len(r.Containers), 1)

	for k, v := range r.Containers {
		return targetSvc.GetContainer(k), v
	}

	panic("sanity check: assertnow should have caught this")
}

func (r ResourceDefinitionService) ApplyDefinition(a Asserter, target ResourceManager, applicationFunctions map[azcopy.LocationLevel]func(Asserter, ResourceManager, ResourceDefinition)) {
	a.HelperMarker().Helper()
	a.AssertNow("target must match level", Equal{}, target.Level(), r.DefinitionTarget())
	serviceManager := target.(ServiceResourceManager)

	// Run the application function for services
	if applicationFunc, ok := applicationFunctions[r.DefinitionTarget()]; ok {
		applicationFunc(a, target, r)
	}

	for name, containerDef := range r.Containers {
		containerDef.ContainerName = &name
		containerManager := serviceManager.GetContainer(name)

		containerDef.ApplyDefinition(a, containerManager, applicationFunctions)
	}
}

func (r ResourceDefinitionService) DefinitionTarget() azcopy.LocationLevel {
	return azcopy.ELocationLevel.Service()
}

func (r ResourceDefinitionService) ShouldExist() bool {
	return true // Account can't not exist, won't be used in validation, etc.
}

func (r ResourceDefinitionService) resourceDefinition() ServiceResourceManager {
	panic("marker method")
}

type ResourceDefinitionContainer struct {
	// ContainerName is overwritten if used as a part of a parent definition
	ContainerName *string
	Properties    ContainerProperties

	Objects ObjectResourceMapping
	// ContainerShouldExist is true unless set to false. Useful in negative validation (e.g. remove)
	ContainerShouldExist *bool
}

func (r ResourceDefinitionContainer) GenerateAdoptiveParent(a Asserter) ResourceDefinition {
	cName := DerefOrDefault(r.ContainerName, uuid.NewString())

	return &ResourceDefinitionService{Containers: map[string]ResourceDefinitionContainer{
		cName: r,
	}}
}

func (r ResourceDefinitionContainer) MatchAdoptiveChild(a Asserter, target ResourceManager) (ResourceManager, ResourceDefinition) {
	a.HelperMarker().Helper()
	targetCont, ok := target.(ContainerResourceManager)

	objs := r.Objects.Flatten()
	a.AssertNow("adoptive parent definitions must match the level of the target they're finding a child for", Equal{}, ok, true)
	a.AssertNow("adoptive parent definitions can only have one container", Equal{}, len(objs), 1)

	for k, v := range objs {
		return targetCont.GetObject(a, k, v.EntityType), v
	}

	panic("sanity check: assertnow should have caught this")
}

func (r ResourceDefinitionContainer) ApplyDefinition(a Asserter, target ResourceManager, applicationFunctions map[azcopy.LocationLevel]func(Asserter, ResourceManager, ResourceDefinition)) {
	a.HelperMarker().Helper()
	a.AssertNow("target must match level", Equal{}, target.Level(), r.DefinitionTarget())
	containerManager := target.(ContainerResourceManager)

	// Run the application function for containers
	if applicationFunc, ok := applicationFunctions[r.DefinitionTarget()]; ok {
		applicationFunc(a, target, r)
	}

	if r.Objects == nil {
		return
	}

	for name, objectDef := range r.Objects.Flatten() {
		objectDef.ObjectName = &name
		objectManager := containerManager.GetObject(a, name, objectDef.EntityType)

		objectDef.ApplyDefinition(a, objectManager, applicationFunctions)
	}
}

func (r ResourceDefinitionContainer) resourceDefinition() ContainerResourceManager {
	panic("marker method")
}

func (r ResourceDefinitionContainer) DefinitionTarget() azcopy.LocationLevel {
	return azcopy.ELocationLevel.Container()
}

func (r ResourceDefinitionContainer) ShouldExist() bool {
	return r.ContainerShouldExist == nil || *r.ContainerShouldExist
}

type ResourceDefinitionObject struct {
	// ObjectName is overwritten if used as a part of a parent definition
	ObjectName *string

	ObjectProperties
	Body ObjectContentContainer
	// ObjectShouldExist is true unless set to false. Useful in negative validation (e.g. remove)
	ObjectShouldExist *bool

	// This is used only to pass the size of the object when making a list of expected objects
	Size string
}

func (r ResourceDefinitionObject) Clone() ResourceDefinitionObject {
	var md5 []byte
	if r.HTTPHeaders.contentMD5 != nil {
		md5 = make([]byte, len(r.HTTPHeaders.contentMD5))
		copy(md5, r.HTTPHeaders.contentMD5)
	}

	var body ObjectContentContainer
	if r.Body != nil {
		body = r.Body.Clone()
	}

	return ResourceDefinitionObject{
		ObjectName: ClonePointer(r.ObjectName),
		ObjectProperties: ObjectProperties{
			EntityType: r.EntityType,
			HTTPHeaders: contentHeaders{
				cacheControl:       ClonePointer(r.HTTPHeaders.cacheControl),
				contentDisposition: ClonePointer(r.HTTPHeaders.contentDisposition),
				contentEncoding:    ClonePointer(r.HTTPHeaders.contentEncoding),
				contentLanguage:    ClonePointer(r.HTTPHeaders.contentLanguage),
				contentType:        ClonePointer(r.HTTPHeaders.contentType),
				contentMD5:         md5,
			},
			Metadata: r.Metadata.Clone(),
			BlobProperties: BlobProperties{
				Type:                ClonePointer(r.BlobProperties.Type),
				Tags:                CloneMap(r.BlobProperties.Tags),
				BlockBlobAccessTier: ClonePointer(r.BlobProperties.BlockBlobAccessTier),
				PageBlobAccessTier:  ClonePointer(r.BlobProperties.PageBlobAccessTier),
				VersionId:           ClonePointer(r.BlobProperties.VersionId),
			},
			BlobFSProperties: BlobFSProperties{
				Permissions: ClonePointer(r.BlobFSProperties.Permissions),
				Owner:       ClonePointer(r.BlobFSProperties.Owner),
				Group:       ClonePointer(r.BlobFSProperties.Group),
				ACL:         ClonePointer(r.BlobFSProperties.ACL),
			},
			FileProperties: FileProperties{
				FileAttributes:    ClonePointer(r.FileProperties.FileAttributes),
				FileCreationTime:  ClonePointer(r.FileProperties.FileCreationTime),
				FileLastWriteTime: ClonePointer(r.FileProperties.FileLastWriteTime),
				FilePermissions:   ClonePointer(r.FileProperties.FilePermissions),
			},
		},
		Body:              body,
		ObjectShouldExist: ClonePointer(r.ObjectShouldExist),
	}
}

func (r ResourceDefinitionObject) GenerateAdoptiveParent(a Asserter) ResourceDefinition {
	oName := DerefOrDefault(r.ObjectName, uuid.NewString())

	return &ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			oName: r,
		},
	}
}

func (r ResourceDefinitionObject) MatchAdoptiveChild(a Asserter, target ResourceManager) (ResourceManager, ResourceDefinition) {
	a.HelperMarker().Helper()
	a.Error("objects have no semantic children")
	panic("sanity check: error should catch this")
}

func (r ResourceDefinitionObject) ApplyDefinition(a Asserter, target ResourceManager, applicationFunctions map[azcopy.LocationLevel]func(Asserter, ResourceManager, ResourceDefinition)) {
	a.HelperMarker().Helper()
	a.AssertNow("target must match level", Equal{}, target.Level(), r.DefinitionTarget())

	// Run the application function for containers
	if applicationFunc, ok := applicationFunctions[r.DefinitionTarget()]; ok {
		applicationFunc(a, target, r)
	}
}

func (r ResourceDefinitionObject) DefinitionTarget() azcopy.LocationLevel {
	return azcopy.ELocationLevel.Object()
}

func (r ResourceDefinitionObject) resourceDefinition() ObjectResourceManager {
	panic("marker method")
}

func (r ResourceDefinitionObject) ShouldExist() bool {
	return r.ObjectShouldExist == nil || *r.ObjectShouldExist
}
