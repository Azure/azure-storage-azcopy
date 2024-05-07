package e2etest

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
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
	a.HelperMarker().Helper()

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

func ValidatePropertyPtr[T any](a Asserter, objectName, name string, expected, real *T) {
	if expected == nil {
		return
	}

	a.Assert(objectName+": "+name+" must match", Equal{Deep: true}, expected, real)
}

func ValidateMetadata(a Asserter, objectName string, expected, real common.Metadata) {
	if expected == nil {
		return
	}

	a.Assert(objectName+": Metadata must match", Equal{Deep: true}, expected, real)
}

func ValidateTags(a Asserter, objectName string, expected, real map[string]string) {
	if expected == nil {
		return
	}

	a.Assert(objectName+": Tags must match", Equal{Deep: true}, expected, real)
}

func ValidateResource[T ResourceManager](a Asserter, target T, definition MatchedResourceDefinition[T], validateObjectContent bool) {
	a.AssertNow("Target resource and definition must not be null", Not{IsNil{}}, a, target, definition)
	a.AssertNow("Target resource must be at a equal level to the resource definition", Equal{}, target.Level(), definition.DefinitionTarget())

	if dryrunner, ok := a.(DryrunAsserter); ok && dryrunner.Dryrun() {
		return
	}

	a.HelperMarker().Helper()

	definition.ApplyDefinition(a, target, map[cmd.LocationLevel]func(Asserter, ResourceManager, ResourceDefinition){
		cmd.ELocationLevel.Container(): func(a Asserter, manager ResourceManager, definition ResourceDefinition) {
			a.HelperMarker().Helper()
			cRes := manager.(ContainerResourceManager)

			exists := cRes.Exists()
			if definition.ShouldExist() != exists {
				a.Assert(cRes.ContainerName()+": object must "+common.Iff(definition.ShouldExist(), "not exist", "exist"), Equal{}, exists, false)
				return
			}
			if !definition.ShouldExist() {
				a.Assert("container "+cRes.ContainerName()+" must not exist", Equal{}, cRes.Exists(), false)
				return
			}

			cProps := cRes.GetProperties(a)
			vProps := definition.(ResourceDefinitionContainer).Properties

			ValidateMetadata(a, cRes.ContainerName(), vProps.Metadata, cProps.Metadata)

			if manager.Location() == common.ELocation.Blob() || manager.Location() == common.ELocation.BlobFS() {
				ValidatePropertyPtr(a, cRes.ContainerName(), "Public access", vProps.BlobContainerProperties.Access, cProps.BlobContainerProperties.Access)
			}

			if manager.Location() == common.ELocation.File() {
				ValidatePropertyPtr(a, cRes.ContainerName(), "Enabled protocols", vProps.FileContainerProperties.EnabledProtocols, cProps.FileContainerProperties.EnabledProtocols)
				ValidatePropertyPtr(a, cRes.ContainerName(), "RootSquash", vProps.FileContainerProperties.RootSquash, cProps.FileContainerProperties.RootSquash)
				ValidatePropertyPtr(a, cRes.ContainerName(), "AccessTier", vProps.FileContainerProperties.AccessTier, cProps.FileContainerProperties.AccessTier)
				ValidatePropertyPtr(a, cRes.ContainerName(), "Quota", vProps.FileContainerProperties.Quota, cProps.FileContainerProperties.Quota)
			}
		},
		cmd.ELocationLevel.Object(): func(a Asserter, manager ResourceManager, definition ResourceDefinition) {
			a.HelperMarker().Helper()
			objMan := manager.(ObjectResourceManager)
			objDef := definition.(ResourceDefinitionObject)

			exists := objMan.Exists()
			if objDef.ShouldExist() != exists {
				a.Assert(objMan.ObjectName()+": object must "+common.Iff(objDef.ShouldExist(), "not exist", "exist"), Equal{}, exists, false)
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
				a.NoError(objMan.ObjectName()+": hash object body", err)
				_, err = io.Copy(valHash, validationBody)
				a.NoError(objMan.ObjectName()+": hash validation body", err)

				a.Assert(objMan.ObjectName()+": bodies differ in hash", Equal{Deep: true}, hex.EncodeToString(objHash.Sum(nil)), hex.EncodeToString(valHash.Sum(nil)))
			}

			// Properties
			ValidateMetadata(a, objMan.ObjectName(), vProps.Metadata, oProps.Metadata)

			// HTTP headers
			ValidatePropertyPtr(a, objMan.ObjectName(), "Cache control", vProps.HTTPHeaders.cacheControl, oProps.HTTPHeaders.cacheControl)
			ValidatePropertyPtr(a, objMan.ObjectName(), "Content disposition", vProps.HTTPHeaders.contentDisposition, oProps.HTTPHeaders.contentDisposition)
			ValidatePropertyPtr(a, objMan.ObjectName(), "Content encoding", vProps.HTTPHeaders.contentEncoding, oProps.HTTPHeaders.contentEncoding)
			ValidatePropertyPtr(a, objMan.ObjectName(), "Content language", vProps.HTTPHeaders.contentLanguage, oProps.HTTPHeaders.contentLanguage)
			ValidatePropertyPtr(a, objMan.ObjectName(), "Content type", vProps.HTTPHeaders.contentType, oProps.HTTPHeaders.contentType)

			switch manager.Location() {
			case common.ELocation.Blob():
				ValidatePropertyPtr(a, objMan.ObjectName(), "Blob type", vProps.BlobProperties.Type, oProps.BlobProperties.Type)
				ValidateTags(a, objMan.ObjectName(), vProps.BlobProperties.Tags, oProps.BlobProperties.Tags)
				ValidatePropertyPtr(a, objMan.ObjectName(), "Block blob access tier", vProps.BlobProperties.BlockBlobAccessTier, oProps.BlobProperties.BlockBlobAccessTier)
				ValidatePropertyPtr(a, objMan.ObjectName(), "Page blob access tier", vProps.BlobProperties.PageBlobAccessTier, oProps.BlobProperties.PageBlobAccessTier)
			case common.ELocation.File():
				ValidatePropertyPtr(a, objMan.ObjectName(), "Attributes", vProps.FileProperties.FileAttributes, oProps.FileProperties.FileAttributes)
				ValidatePropertyPtr(a, objMan.ObjectName(), "Creation time", vProps.FileProperties.FileCreationTime, oProps.FileProperties.FileCreationTime)
				ValidatePropertyPtr(a, objMan.ObjectName(), "Last write time", vProps.FileProperties.FileLastWriteTime, oProps.FileProperties.FileLastWriteTime)
				ValidatePropertyPtr(a, objMan.ObjectName(), "Permissions", vProps.FileProperties.FilePermissions, oProps.FileProperties.FilePermissions)
			case common.ELocation.BlobFS():
				ValidatePropertyPtr(a, objMan.ObjectName(), "Permissions", vProps.BlobFSProperties.Permissions, oProps.BlobFSProperties.Permissions)
				ValidatePropertyPtr(a, objMan.ObjectName(), "Owner", vProps.BlobFSProperties.Owner, oProps.BlobFSProperties.Owner)
				ValidatePropertyPtr(a, objMan.ObjectName(), "Group", vProps.BlobFSProperties.Group, oProps.BlobFSProperties.Group)
				ValidatePropertyPtr(a, objMan.ObjectName(), "ACL", vProps.BlobFSProperties.ACL, oProps.BlobFSProperties.ACL)
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

	listStdout, ok := stdout.(*AzCopyParsedListStdout)
	a.AssertNow("stdout must be AzCopyParsedListStdout", Equal{}, ok, true)

	a.AssertNow("stdout and expected objects must not be null", Not{IsNil{}}, a, stdout, expectedObjects)
	a.Assert("map of objects must be equivalent in size", Equal{}, len(expectedObjects), len(listStdout.Items))
	a.Assert("map of objects must match", MapContains[AzCopyOutputKey, cmd.AzCopyListObject]{TargetMap: expectedObjects}, listStdout.Items)
	a.Assert("summary must match", Equal{}, listStdout.Summary, DerefOrZero(expectedSummary))
}

func ValidateErrorOutput(a Asserter, stdout AzCopyStdout, errorMsg string) {
	if dryrunner, ok := a.(DryrunAsserter); ok && dryrunner.Dryrun() {
		return
	}
	for _, line := range stdout.RawStdout() {
		if strings.Contains(line, errorMsg) {
			return
		}
	}
	fmt.Println(stdout.String())
	a.Error("expected error message not found in azcopy output")
}
