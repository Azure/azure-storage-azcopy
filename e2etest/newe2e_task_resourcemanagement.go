package e2etest

import (
	"crypto/md5"
	"encoding/hex"
	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"io"
	"strings"
)

// ResourceTracker tracks resources
type ResourceTracker interface {
	TrackCreatedResource(manager ResourceManager)
	TrackCreatedAccount(account AccountResourceManager)
}

func TrackResourceCreation(a Asserter, rm any) {
	if t, ok := a.(ResourceTracker); ok {
		if arm, ok := rm.(AccountResourceManager); ok {
			t.TrackCreatedAccount(arm)
		} else if resMan, ok := rm.(ResourceManager); ok {
			t.TrackCreatedResource(resMan)
		}
	}
}

func CreateResource[T ResourceManager](a Asserter, base ResourceManager, def MatchedResourceDefinition[T]) T {
	definition := ResourceDefinition(def)

	a.AssertNow("Base resource and definition must not be null", Not{IsNil{}}, base, definition)
	a.AssertNow("Base resource must be at a equal or lower level than the resource definition", Equal{}, base.Level() <= definition.DefinitionTarget(), true)

	// Remember where we started so we can step up to there
	originalDefinition := definition
	_ = originalDefinition // use it so Go stops screaming

	// Get to the target level.
	for base.Level() < definition.DefinitionTarget() {
		/*
			Instead of scaling up to the definition, we'll scale the definition to the base.
			This means we don't have to keep tabs on the fact we'll need to create the container later,
			because the container creation is now an inherent part of the resource definition.
		*/
		definition = definition.GenerateAdoptiveParent(a)
	}

	// Create the resource(s)
	definition.ApplyDefinition(a, base, map[cmd.LocationLevel]func(Asserter, ResourceManager, ResourceDefinition){
		cmd.ELocationLevel.Container(): func(a Asserter, manager ResourceManager, definition ResourceDefinition) {
			manager.(ContainerResourceManager).Create(a, definition.(ResourceDefinitionContainer).Properties)
		},

		cmd.ELocationLevel.Object(): func(a Asserter, manager ResourceManager, definition ResourceDefinition) {
			objDef := definition.(ResourceDefinitionObject)

			if objDef.Body == nil {
				objDef.Body = NewZeroObjectContentContainer(0)
			}

			manager.(ObjectResourceManager).Create(a, objDef.Body, objDef.ObjectProperties)
		},
	})

	// Step up to where we need to be and return it
	matchingDef := definition
	matchingRes := base
	for matchingRes.Level() < originalDefinition.DefinitionTarget() {
		matchingRes, matchingDef = matchingDef.MatchAdoptiveChild(a, matchingRes)
	}

	return matchingRes.(T)
}

func ValidatePropertyPtr[T any](a Asserter, name string, expected, real *T) {
	if expected == nil {
		return
	}

	a.Assert(name+" must match", Equal{Deep: true}, expected, real)
}

func ValidateMetadata(a Asserter, expected, real common.Metadata) {
	if expected == nil {
		return
	}

	a.Assert("Metadata must match", Equal{Deep: true}, expected, real)
}

func ValidateTags(a Asserter, expected, real map[string]string) {
	if expected == nil {
		return
	}

	a.Assert("Tags must match", Equal{Deep: true}, expected, real)
}

func ValidateResource[T ResourceManager](a Asserter, target T, definition MatchedResourceDefinition[T], validateObjectContent bool) {
	a.AssertNow("Target resource and definition must not be null", Not{IsNil{}}, a, target, definition)
	a.AssertNow("Target resource must be at a equal level to the resource definition", Equal{}, target.Level(), definition.DefinitionTarget())

	if dryrunner, ok := a.(DryrunAsserter); ok && dryrunner.Dryrun() {
		return
	}

	definition.ApplyDefinition(a, target, map[cmd.LocationLevel]func(Asserter, ResourceManager, ResourceDefinition){
		cmd.ELocationLevel.Container(): func(a Asserter, manager ResourceManager, definition ResourceDefinition) {
			cRes := manager.(ContainerResourceManager)

			if !definition.ShouldExist() {
				a.AssertNow("container must not exist", Equal{}, cRes.Exists(), false)
				return
			}

			cProps := cRes.GetProperties(a)
			vProps := definition.(ResourceDefinitionContainer).Properties

			ValidateMetadata(a, vProps.Metadata, cProps.Metadata)

			if manager.Location() == common.ELocation.Blob() || manager.Location() == common.ELocation.BlobFS() {
				ValidatePropertyPtr(a, "Public access", vProps.BlobContainerProperties.Access, cProps.BlobContainerProperties.Access)
			}

			if manager.Location() == common.ELocation.File() {
				ValidatePropertyPtr(a, "Enabled protocols", vProps.FileContainerProperties.EnabledProtocols, cProps.FileContainerProperties.EnabledProtocols)
				ValidatePropertyPtr(a, "RootSquash", vProps.FileContainerProperties.RootSquash, cProps.FileContainerProperties.RootSquash)
				ValidatePropertyPtr(a, "AccessTier", vProps.FileContainerProperties.AccessTier, cProps.FileContainerProperties.AccessTier)
				ValidatePropertyPtr(a, "Quota", vProps.FileContainerProperties.Quota, cProps.FileContainerProperties.Quota)
			}
		},
		cmd.ELocationLevel.Object(): func(a Asserter, manager ResourceManager, definition ResourceDefinition) {
			objMan := manager.(ObjectResourceManager)
			objDef := definition.(ResourceDefinitionObject)

			if !objDef.ShouldExist() {
				a.AssertNow("object must not exist", Equal{}, objMan.Exists(), false)
				return
			}

			oProps := objMan.GetProperties(a)
			vProps := objDef.ObjectProperties

			if validateObjectContent && objMan.EntityType() == common.EEntityType.File() && objDef.Body != nil {
				objBody := objMan.Download(a)
				validationBody := objDef.Body.Reader()

				objHash := md5.New()
				valHash := md5.New()

				_, err := io.Copy(objHash, objBody)
				a.NoError("hash object body", err)
				_, err = io.Copy(valHash, validationBody)
				a.NoError("hash validation body", err)

				a.Assert("bodies differ in hash", Equal{Deep: true}, hex.EncodeToString(objHash.Sum(nil)), hex.EncodeToString(valHash.Sum(nil)))
			}

			// Properties
			ValidateMetadata(a, vProps.Metadata, oProps.Metadata)

			// HTTP headers
			ValidatePropertyPtr(a, "Cache control", vProps.HTTPHeaders.cacheControl, oProps.HTTPHeaders.cacheControl)
			ValidatePropertyPtr(a, "Content disposition", vProps.HTTPHeaders.contentDisposition, oProps.HTTPHeaders.contentDisposition)
			ValidatePropertyPtr(a, "Content encoding", vProps.HTTPHeaders.contentEncoding, oProps.HTTPHeaders.contentEncoding)
			ValidatePropertyPtr(a, "Content language", vProps.HTTPHeaders.contentLanguage, oProps.HTTPHeaders.contentLanguage)
			ValidatePropertyPtr(a, "Content type", vProps.HTTPHeaders.contentType, oProps.HTTPHeaders.contentType)

			switch manager.Location() {
			case common.ELocation.Blob():
				ValidatePropertyPtr(a, "Blob type", vProps.BlobProperties.Type, oProps.BlobProperties.Type)
				ValidateTags(a, vProps.BlobProperties.Tags, oProps.BlobProperties.Tags)
				ValidatePropertyPtr(a, "Block blob access tier", vProps.BlobProperties.BlockBlobAccessTier, oProps.BlobProperties.BlockBlobAccessTier)
				ValidatePropertyPtr(a, "Page blob access tier", vProps.BlobProperties.PageBlobAccessTier, oProps.BlobProperties.PageBlobAccessTier)
			case common.ELocation.File():
				ValidatePropertyPtr(a, "Attributes", vProps.FileProperties.FileAttributes, oProps.FileProperties.FileAttributes)
				ValidatePropertyPtr(a, "Creation time", vProps.FileProperties.FileCreationTime, oProps.FileProperties.FileCreationTime)
				ValidatePropertyPtr(a, "Last write time", vProps.FileProperties.FileLastWriteTime, oProps.FileProperties.FileLastWriteTime)
				ValidatePropertyPtr(a, "Permissions", vProps.FileProperties.FilePermissions, oProps.FileProperties.FilePermissions)
			case common.ELocation.BlobFS():
				ValidatePropertyPtr(a, "Permissions", vProps.BlobFSProperties.Permissions, oProps.BlobFSProperties.Permissions)
				ValidatePropertyPtr(a, "Owner", vProps.BlobFSProperties.Owner, oProps.BlobFSProperties.Owner)
				ValidatePropertyPtr(a, "Group", vProps.BlobFSProperties.Group, oProps.BlobFSProperties.Group)
				ValidatePropertyPtr(a, "ACL", vProps.BlobFSProperties.ACL, oProps.BlobFSProperties.ACL)
			}
		},
	})
}

type AzCopyOutputKey struct {
	Path       string
	VersionId  string
	SnapshotId string
}

func ValidateListOutput(a Asserter, stdout AzCopyStdout, expectedObjects map[AzCopyOutputKey]cmd.AzCopyListObject, expectedSummary *cmd.AzCopyListSummary) {
	if dryrunner, ok := a.(DryrunAsserter); ok && dryrunner.Dryrun() {
		return
	}

	a.AssertNow("stdout and expected objects must not be null", Not{IsNil{}}, a, stdout, expectedObjects)

	// Unmarshall stdout
	objects, summary, err := stdout.(*AzCopyListStdout).Unmarshal()
	a.AssertNow("error unmarshalling stdout", IsNil{}, err)

	// Validate
	a.Assert("number of objects must match", Equal{}, len(objects), len(expectedObjects))
	a.Assert("summary must match", Equal{Deep: true}, summary, expectedSummary)

	o := make([]any, len(objects))
	for i, v := range objects {
		o[i] = v
	}
	a.Assert("map of objects must match", MapContains[AzCopyOutputKey, cmd.AzCopyListObject]{TargetMap: expectedObjects,
		ValueToKVPair: func(obj cmd.AzCopyListObject) KVPair[AzCopyOutputKey, cmd.AzCopyListObject] {
			return KVPair[AzCopyOutputKey, cmd.AzCopyListObject]{Key: AzCopyOutputKey{Path: obj.Path, VersionId: obj.VersionId}, Value: obj}
		}}, o...)
}

func ValidateErrorOutput(a Asserter, stdout AzCopyStdout, errorMsg string) {
	if dryrunner, ok := a.(DryrunAsserter); ok && dryrunner.Dryrun() {
		return
	}
	for _, line := range stdout.(*AzCopyRawStdout).RawOutput {
		if strings.Contains(line, errorMsg) {
			return
		}
	}
	a.Error("expected error message not found in azcopy output")
}
