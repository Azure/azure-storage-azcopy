package e2etest

import (
	"context"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func init() {
	suiteManager.RegisterSuite(&FileTestSuite{})
}

type FileTestSuite struct{}

func (s *FileTestSuite) Scenario_SingleFileUploadDifferentSizes(svm *ScenarioVariationManager) {
	size := ResolveVariation(svm, []int64{0, 1, 4*common.MegaByte - 1, 4 * common.MegaByte, 4*common.MegaByte + 1})
	fileName := fmt.Sprintf("test_file_upload_%dB_fullname", size)
	body := NewRandomObjectContentContainer(svm, size)

	srcObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{}).
		GetObject(svm, fileName, common.EEntityType.File())
	srcObj.Create(svm, body, ObjectProperties{})

	dstObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.File()), ResourceDefinitionContainer{}).
		GetObject(svm, fileName, common.EEntityType.File())

	RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{srcObj, dstObj},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive:   pointerTo(true),
				BlockSizeMB: pointerTo(4.0),
			},
		},
	})

	ValidateResource[ObjectResourceManager](svm, dstObj, ResourceDefinitionObject{
		Body: body,
	}, true)
}

func (s *FileTestSuite) Scenario_CompleteSparseFileUpload(svm *ScenarioVariationManager) {
	body := NewZeroObjectContentContainer(4 * common.MegaByte)
	name := "sparse_file"
	srcObj := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionObject{
		ObjectName: pointerTo(name),
		Body:       body,
	})
	dstObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.File()), ResourceDefinitionContainer{}).
		GetObject(svm, name, common.EEntityType.File())

	RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbCopy,
			Targets: []ResourceManager{
				srcObj,
				TryApplySpecificAuthType(dstObj, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{}),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					BlockSizeMB: pointerTo(float64(4)),
				},
			},
		})

	ValidateResource[ObjectResourceManager](svm, dstObj, ResourceDefinitionObject{
		Body: body,
	}, true)

	if svm.Dryrun() {
		return
	}
	// Verify ranges
	manager := dstObj.(ObjectResourceManager).(*FileObjectResourceManager)
	resp, err := manager.getFileClient().GetRangeList(context.Background(), nil)
	svm.NoError("Get Range List call should not fail", err)
	svm.Assert("Ranges should be returned", Not{IsNil{}}, resp.Ranges)
	svm.Assert("Expected number of ranges does not match", Equal{}, len(resp.Ranges), 0)
}

func (s *FileTestSuite) Scenario_PartialSparseFileUpload(svm *ScenarioVariationManager) {
	body := NewPartialSparseObjectContentContainer(svm, 16*common.MegaByte)
	name := "test_partial_sparse_file"
	// Scale up from service to object
	srcObj := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionObject{
		ObjectName: pointerTo(name),
		Body:       body,
	})
	dstObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.File()), ResourceDefinitionContainer{}).
		GetObject(svm, name, common.EEntityType.File())

	RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbCopy,
			Targets: []ResourceManager{
				srcObj,
				TryApplySpecificAuthType(dstObj, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{}),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					BlockSizeMB: pointerTo(float64(4)),
				},
			},
		})

	ValidateResource[ObjectResourceManager](svm, dstObj, ResourceDefinitionObject{
		Body: body,
	}, true)

	if svm.Dryrun() {
		return
	}
	// Verify ranges
	manager := dstObj.(ObjectResourceManager).(*FileObjectResourceManager)
	resp, err := manager.getFileClient().GetRangeList(context.Background(), nil)
	svm.NoError("Get Range List call should not fail", err)
	svm.Assert("Ranges should be returned", Not{IsNil{}}, resp.Ranges)
	svm.Assert("Expected number of ranges does not match", Equal{}, len(resp.Ranges), 2)
}

func (s *FileTestSuite) Scenario_GuessMimeType(svm *ScenarioVariationManager) {
	size := int64(0)
	fileName := "test_guessmimetype.html"
	body := NewRandomObjectContentContainer(svm, size)

	srcObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{}).
		GetObject(svm, fileName, common.EEntityType.File())
	srcObj.Create(svm, body, ObjectProperties{})

	dstObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.File()), ResourceDefinitionContainer{}).
		GetObject(svm, fileName, common.EEntityType.File())

	RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{srcObj, dstObj},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive: pointerTo(true),
			},
		},
	})

	ValidateResource[ObjectResourceManager](svm, dstObj, ResourceDefinitionObject{
		Body: body,
		ObjectProperties: ObjectProperties{
			HTTPHeaders: contentHeaders{
				contentType: pointerTo("text/html"),
			},
		},
	}, false)
}

func (s *FileTestSuite) Scenario_UploadFileProperties(svm *ScenarioVariationManager) {
	size := int64(0)
	fileName := "test_properties"
	body := NewRandomObjectContentContainer(svm, size)

	srcObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{}).
		GetObject(svm, fileName, common.EEntityType.File()) // awkward capitalization to see if AzCopy catches it.
	srcObj.Create(svm, body, ObjectProperties{})

	dstObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.File()), ResourceDefinitionContainer{}).
		GetObject(svm, fileName, common.EEntityType.File())

	metadata := common.Metadata{"Author": pointerTo("gapra"), "Viewport": pointerTo("width"), "Description": pointerTo("test file")}
	contentType := pointerTo("testctype")
	contentEncoding := pointerTo("testenc")

	RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{srcObj, dstObj},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive: pointerTo(true),
			},
			Metadata:        metadata,
			ContentType:     contentType,
			ContentEncoding: contentEncoding,
			NoGuessMimeType: pointerTo(true),
		},
	})

	ValidateResource[ObjectResourceManager](svm, dstObj, ResourceDefinitionObject{
		Body: body,
		ObjectProperties: ObjectProperties{
			Metadata: metadata,
			HTTPHeaders: contentHeaders{
				contentType:     contentType,
				contentEncoding: contentEncoding,
			},
		},
	}, false)
}

func (s *FileTestSuite) Scenario_DownloadPreserveLMTFile(svm *ScenarioVariationManager) {
	body := NewZeroObjectContentContainer(0)
	name := "test_upload_preserve_last_mtime"
	srcObj := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, common.ELocation.File()), ResourceDefinitionObject{ObjectName: pointerTo(name), Body: body})
	dstObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{}).GetObject(svm, name, common.EEntityType.File())

	srcObjLMT := srcObj.GetProperties(svm).FileProperties.LastModifiedTime

	RunAzCopy(
		svm,
		AzCopyCommand{
			Verb:    AzCopyVerbCopy,
			Targets: []ResourceManager{srcObj, dstObj},
			Flags: CopyFlags{
				PreserveLMT: pointerTo(true),
			},
		})

	ValidateResource[ObjectResourceManager](svm, dstObj, ResourceDefinitionObject{
		Body: body,
		ObjectProperties: ObjectProperties{
			FileProperties: FileProperties{
				LastModifiedTime: srcObjLMT,
			},
		},
	}, false)
}

func (s *FileTestSuite) Scenario_Download63MBFile(svm *ScenarioVariationManager) {
	body := NewRandomObjectContentContainer(svm, 63*common.MegaByte)
	name := "test_63mb"
	srcObj := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, common.ELocation.File()), ResourceDefinitionObject{ObjectName: pointerTo(name), Body: body})
	dstObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{}).GetObject(svm, name, common.EEntityType.File())

	RunAzCopy(
		svm,
		AzCopyCommand{
			Verb:    AzCopyVerbCopy,
			Targets: []ResourceManager{srcObj, dstObj},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					BlockSizeMB: pointerTo(4.0),
				},
			},
		})

	ValidateResource[ObjectResourceManager](svm, dstObj, ResourceDefinitionObject{
		Body: body,
	}, false)
}
