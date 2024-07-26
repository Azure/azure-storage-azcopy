package e2etest

import (
	"context"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"strconv"
	"math"
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
	size := 16 * common.MegaByte
	ranges := make([]Range, 0)
	for i := 0; i < size; i += 8 * common.MegaByte {
		end := math.Min(float64(i+4*common.MegaByte), float64(size))
		ranges = append(ranges, Range{Start: int64(i), End: int64(end)})
	}
	body := NewPartialSparseObjectContentContainer(svm, int64(size), ranges)
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

	srcObjLMT := srcObj.GetProperties(svm).LastModifiedTime

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
			LastModifiedTime: srcObjLMT,
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

func (s *FileTestSuite) Scenario_UploadDirectory(svm *ScenarioVariationManager) {
	// Scale up from service to object
	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.File()), ResourceDefinitionContainer{})

	dirsToCreate := []string{"dir_file_copy_test", "dir_file_copy_test/sub_dir_copy_test"}

	// Create destination directories
	srcObjs := make(ObjectResourceMappingFlat)
	for _, dir := range dirsToCreate {
		obj := ResourceDefinitionObject{ObjectName: pointerTo(dir), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}}
		if dstContainer.Location() != common.ELocation.Blob() {
			srcObjs[dir] = obj
		}
		for i := range 3 {
			name := dir + "/test" + strconv.Itoa(i) + ".txt"
			obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: NewRandomObjectContentContainer(svm, SizeFromString("1K"))}
			srcObjs[name] = obj
		}
	}

	for _, obj := range srcObjs {
		if obj.EntityType != common.EEntityType.Folder() {
			CreateResource[ObjectResourceManager](svm, srcContainer, obj)
		}
	}

	srcDir := srcContainer.GetObject(svm, dirsToCreate[0], common.EEntityType.Folder())
	dstDir := dstContainer.GetObject(svm, dirsToCreate[0], common.EEntityType.Folder())

	sasOpts := GenericAccountSignatureValues{}

	RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbCopy,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcDir, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
				TryApplySpecificAuthType(dstDir, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
				},
				AsSubdir: pointerTo(false),
			},
		})

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, true)
}

func (s *FileTestSuite) Scenario_DownloadDirectory(svm *ScenarioVariationManager) {
	// Scale up from service to object
	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.File()), ResourceDefinitionContainer{})
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})

	dirsToCreate := []string{"dir_file_copy_test", "dir_file_copy_test/sub_dir_copy_test"}

	// Create destination directories
	srcObjs := make(ObjectResourceMappingFlat)
	for _, dir := range dirsToCreate {
		obj := ResourceDefinitionObject{ObjectName: pointerTo(dir), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}}
		if dstContainer.Location() != common.ELocation.Blob() {
			srcObjs[dir] = obj
		}
		for i := range 3 {
			name := dir + "/test" + strconv.Itoa(i) + ".txt"
			obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: NewRandomObjectContentContainer(svm, SizeFromString("1K"))}
			srcObjs[name] = obj
		}
	}

	for _, obj := range srcObjs {
		if obj.EntityType != common.EEntityType.Folder() {
			CreateResource[ObjectResourceManager](svm, srcContainer, obj)
		}
	}

	srcDir := srcContainer.GetObject(svm, dirsToCreate[0], common.EEntityType.Folder())
	dstDir := dstContainer.GetObject(svm, dirsToCreate[0], common.EEntityType.Folder())

	sasOpts := GenericAccountSignatureValues{}

	RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbCopy,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcDir, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
				TryApplySpecificAuthType(dstDir, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
				},
				AsSubdir: pointerTo(false),
			},
		})

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, true)
}

func (s *FileTestSuite) Scenario_SingleFileUploadWildcard(svm *ScenarioVariationManager) {
	size := common.MegaByte
	fileName := fmt.Sprintf("test_file_upload_%dB_fullname.txt", size)
	body := NewRandomObjectContentContainer(svm, int64(size))

	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})
	srcObj := srcContainer.GetObject(svm, fileName, common.EEntityType.File())
	srcObj.Create(svm, body, ObjectProperties{})

	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.File()), ResourceDefinitionContainer{})
	dstObj := dstContainer.GetObject(svm, fileName, common.EEntityType.File())

	RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{TryApplySpecificAuthType(srcContainer, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{Wildcard: "/*"}), dstContainer},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				BlockSizeMB: pointerTo(4.0),
			},
		},
	})

	ValidateResource[ObjectResourceManager](svm, dstObj, ResourceDefinitionObject{
		Body: body,
	}, true)
}

func (s *FileTestSuite) Scenario_AllFileUploadWildcard(svm *ScenarioVariationManager) {
	size := common.KiloByte
	fileName := fmt.Sprintf("test_file_upload_%dB_fullname", size)
	body := NewRandomObjectContentContainer(svm, int64(size))

	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})
	srcObj := srcContainer.GetObject(svm, fileName, common.EEntityType.File())
	srcObj.Create(svm, body, ObjectProperties{})

	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.File()), ResourceDefinitionContainer{})

	RunAzCopy(svm, AzCopyCommand{
		Verb: AzCopyVerbCopy,
		Targets: []ResourceManager{
			TryApplySpecificAuthType(srcContainer, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{Wildcard: "/*"}),
			dstContainer,
		},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				BlockSizeMB: pointerTo(4.0),
			},
		},
	})

	ValidateResource[ObjectResourceManager](svm, dstContainer.GetObject(svm, fileName, common.EEntityType.File()), ResourceDefinitionObject{
		Body: body,
	}, true)
}

func (s *FileTestSuite) Scenario_AllFileDownloadWildcard(svm *ScenarioVariationManager) {
	size := common.KiloByte
	fileName := fmt.Sprintf("test_file_upload_%dB_fullname", size)
	body := NewRandomObjectContentContainer(svm, int64(size))

	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.File()), ResourceDefinitionContainer{})
	srcObj := srcContainer.GetObject(svm, fileName, common.EEntityType.File())
	srcObj.Create(svm, body, ObjectProperties{})

	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})

	RunAzCopy(svm, AzCopyCommand{
		Verb: AzCopyVerbCopy,
		Targets: []ResourceManager{
			TryApplySpecificAuthType(srcContainer, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{Wildcard: "/*"}),
			dstContainer,
		},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				BlockSizeMB: pointerTo(4.0),
			},
		},
	})

	ValidateResource[ObjectResourceManager](svm, dstContainer.GetObject(svm, fileName, common.EEntityType.File()), ResourceDefinitionObject{
		Body: body,
	}, true)
}

func (s *FileTestSuite) Scenario_SeveralFileUploadWildcard(svm *ScenarioVariationManager) {
	size := common.KiloByte
	fileName := fmt.Sprintf("test_file_upload_%dB_fullname", size)
	body := NewRandomObjectContentContainer(svm, int64(size))

	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})
	srcObj := srcContainer.GetObject(svm, fileName, common.EEntityType.File())
	srcObj.Create(svm, body, ObjectProperties{})

	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.File()), ResourceDefinitionContainer{})

	RunAzCopy(svm, AzCopyCommand{
		Verb: AzCopyVerbCopy,
		Targets: []ResourceManager{
			TryApplySpecificAuthType(srcContainer, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{Wildcard: "/test_file*"}),
			dstContainer,
		},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				BlockSizeMB: pointerTo(4.0),
			},
		},
	})

	ValidateResource[ObjectResourceManager](svm, dstContainer.GetObject(svm, fileName, common.EEntityType.File()), ResourceDefinitionObject{
		Body: body,
	}, true)
}

func (s *FileTestSuite) Scenario_SeveralFileDownloadWildcard(svm *ScenarioVariationManager) {
	size := common.KiloByte
	fileName := fmt.Sprintf("test_file_upload_%dB_fullname", size)
	body := NewRandomObjectContentContainer(svm, int64(size))

	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.File()), ResourceDefinitionContainer{})
	srcObj := srcContainer.GetObject(svm, fileName, common.EEntityType.File())
	srcObj.Create(svm, body, ObjectProperties{})

	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})

	RunAzCopy(svm, AzCopyCommand{
		Verb: AzCopyVerbCopy,
		Targets: []ResourceManager{
			TryApplySpecificAuthType(srcContainer, EExplicitCredentialType.SASToken(), svm),
			dstContainer,
		},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				BlockSizeMB:    pointerTo(4.0),
				IncludePattern: pointerTo("test_file*"),
				Recursive:      pointerTo(true),
			},
		},
	})

	ValidateResource[ObjectResourceManager](svm, dstContainer.GetObject(svm, srcContainer.ContainerName()+"/"+fileName, common.EEntityType.File()), ResourceDefinitionObject{
		Body: body,
	}, true)
}
