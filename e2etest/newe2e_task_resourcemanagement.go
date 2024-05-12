package e2etest

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"io"
	"reflect"
	"sort"
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
				a.Assert(fmt.Sprintf("object %s must not exist", objMan.ObjectName()), Equal{}, objMan.Exists(), false)
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

func ValidateListTextOutput(a Asserter, stdout AzCopyStdout, expectedObjects map[AzCopyOutputKey]cmd.AzCopyListObject, expectedSummary *cmd.AzCopyListSummary) {
	if dryrunner, ok := a.(DryrunAsserter); ok && dryrunner.Dryrun() {
		return
	}

	expectedObj := expectedObjectsToString(expectedObjects)
	rawObj, rawSum := cleanupRaw(stdout.RawStdout(), expectedSummary != nil)

	// sort to avoid out of order arrays
	sort.Strings(rawObj)
	sort.Strings(expectedObj)

	if !reflect.DeepEqual(rawObj, expectedObj) {
		a.Error(fmt.Sprintf("Does not match - raw:%s. expectedObj:%s.", rawObj, expectedObj))
	}

	// if expectedObj summary is provided, the last two elements of raw are part of the summary
	if expectedSummary != nil {
		validateSummary(a, rawSum, expectedSummary)
	}
}

func cleanupRaw(raw []string, hasSummary bool) ([]string, []string) {
	var objectRaw []string
	var summaryRaw []string
	var tempRaw []string
	for i := 0; i < len(raw); i++ {
		if raw[i] != "" {
			tempRaw = append(tempRaw, raw[i])
		}
	}

	index := len(tempRaw) - 2 // last two values of raw output should be summary
	if hasSummary {
		objectRaw = tempRaw[:index]
		summaryRaw = tempRaw[index:]
	} else {
		objectRaw = tempRaw
	}
	return objectRaw, summaryRaw
}

func validateSummary(a Asserter, rawSummary []string, expectedSummary *cmd.AzCopyListSummary) {
	expectedSum := [2]string{fmt.Sprintf("File count: %s", expectedSummary.FileCount), fmt.Sprintf("Total file size: %s", expectedSummary.TotalFileSize)}

	if rawSummary[0] != expectedSum[0] {
		a.Error(fmt.Sprintf("File count does not match - raw:%s. expected:%s.", rawSummary[0], expectedSum[0]))
	}

	if rawSummary[1] != expectedSum[1] {
		a.Error(fmt.Sprintf("Total file size does not match - raw:%s. expected:%s.", rawSummary[1], expectedSum[1]))
	}
}

func expectedObjectsToString(expectedObjects map[AzCopyOutputKey]cmd.AzCopyListObject) []string {
	var stringArray []string

	for _, val := range expectedObjects {
		stringArray = append(stringArray, toString(val))
	}

	return stringArray
}

func toString(lo cmd.AzCopyListObject) string {
	builder := strings.Builder{}
	builder.WriteString(lo.Path + "; ")

	// set up azcopy list object string array
	if lo.LastModifiedTime != nil {
		builder.WriteString(fmt.Sprintf("%s: %s; ", cmd.LastModifiedTime, lo.LastModifiedTime.String()))
	}
	if lo.VersionId != "" {
		builder.WriteString(fmt.Sprintf("%s: %s; ", cmd.VersionId, lo.VersionId))
	}
	if lo.BlobType != "" {
		builder.WriteString(fmt.Sprintf("%s: %s; ", cmd.BlobType, string(lo.BlobType)))
	}
	if lo.BlobAccessTier != "" {
		builder.WriteString(fmt.Sprintf("%s: %s; ", cmd.BlobAccessTier, string(lo.BlobAccessTier)))
	}
	if lo.ContentType != "" {
		builder.WriteString(fmt.Sprintf("%s: %s; ", cmd.ContentType, lo.ContentType))
	}
	if lo.ContentEncoding != "" {
		builder.WriteString(fmt.Sprintf("%s: %s; ", cmd.ContentType, lo.ContentEncoding))
	}
	if lo.ContentMD5 != nil {
		builder.WriteString(fmt.Sprintf("%s: %s; ", cmd.ContentMD5, base64.StdEncoding.EncodeToString(lo.ContentMD5)))
	}
	if lo.LeaseState != "" {
		builder.WriteString(fmt.Sprintf("%s: %s; ", cmd.LeaseState, string(lo.LeaseState)))
	}
	if lo.LeaseStatus != "" {
		builder.WriteString(fmt.Sprintf("%s: %s; ", cmd.LeaseStatus, string(lo.LeaseStatus)))
	}
	if lo.LeaseDuration != "" {
		builder.WriteString(fmt.Sprintf("%s: %s; ", cmd.LeaseDuration, string(lo.LeaseDuration)))
	}
	if lo.ArchiveStatus != "" {
		builder.WriteString(fmt.Sprintf("%s: %s; ", cmd.ArchiveStatus, string(lo.ArchiveStatus)))
	}

	builder.WriteString("Content Length: " + lo.ContentLength)

	return builder.String()
}
