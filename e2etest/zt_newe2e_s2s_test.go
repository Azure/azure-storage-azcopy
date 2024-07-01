package e2etest

import (
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"strconv"
)

func init() {
	suiteManager.RegisterSuite(&S2STestSuite{})
}

type S2STestSuite struct{}

func (s *S2STestSuite) Scenario_BlobDestinationSizes(svm *ScenarioVariationManager) {
	src := ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.File()})
	dst := common.ELocation.Blob()
	size := ResolveVariation(svm, []int64{0, common.KiloByte, 63 * common.MegaByte})
	fileName := "test_copy.txt"
	body := NewRandomObjectContentContainer(svm, size)

	// TODO : Add S3 to source
	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, src), ResourceDefinitionContainer{})
	srcObj := srcContainer.GetObject(svm, fileName, common.EEntityType.File())
	srcObj.Create(svm, body, ObjectProperties{})

	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, dst), ResourceDefinitionContainer{})
	dstObj := dstContainer.GetObject(svm, fileName, common.EEntityType.File())

	RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{srcObj, dstObj.(RemoteResourceManager).WithSpecificAuthType(EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{})},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive: pointerTo(true),
			},
		},
	})

	ValidateResource[ObjectResourceManager](svm, dstObj, ResourceDefinitionObject{
		Body: body,
	}, true)
}

func (s *S2STestSuite) Scenario_BlobFile1KB(svm *ScenarioVariationManager) {
	src := common.ELocation.Blob()
	dst := common.ELocation.File()
	size := common.KiloByte
	fileName := "test_copy.txt"
	body := NewRandomObjectContentContainer(svm, int64(size))

	// TODO : Add S3 to source
	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, src), ResourceDefinitionContainer{})
	srcObj := srcContainer.GetObject(svm, fileName, common.EEntityType.File())
	srcObj.Create(svm, body, ObjectProperties{})

	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, dst), ResourceDefinitionContainer{})
	dstObj := dstContainer.GetObject(svm, fileName, common.EEntityType.File())

	RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{srcObj, dstObj.(RemoteResourceManager).WithSpecificAuthType(EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{})},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive: pointerTo(true),
			},
		},
	})

	ValidateResource[ObjectResourceManager](svm, dstObj, ResourceDefinitionObject{
		Body: body,
	}, true)
}

func (s *S2STestSuite) Scenario_SingleFileCopyBlobTypeVariations(svm *ScenarioVariationManager) {
	srcBlobType := ResolveVariation(svm, []blob.BlobType{blob.BlobTypeBlockBlob, blob.BlobTypePageBlob, blob.BlobTypeAppendBlob})
	destBlobType := ResolveVariation(svm, []blob.BlobType{blob.BlobTypeBlockBlob, blob.BlobTypePageBlob, blob.BlobTypeAppendBlob})

	fileName := "test_512b_copy.txt"
	body := NewRandomObjectContentContainer(svm, 512)

	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})
	srcObj := srcContainer.GetObject(svm, fileName, common.EEntityType.File())
	srcObj.Create(svm, body, ObjectProperties{BlobProperties: BlobProperties{Type: pointerTo(srcBlobType)}})

	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})

	RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{srcObj, dstContainer},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive: pointerTo(true),
			},
			BlobType: pointerTo(common.FromBlobType(destBlobType)),
		},
	})

	ValidateResource[ObjectResourceManager](svm, dstContainer.GetObject(svm, fileName, common.EEntityType.File()), ResourceDefinitionObject{
		Body: body,
		ObjectProperties: ObjectProperties{
			BlobProperties: BlobProperties{
				Type: pointerTo(destBlobType),
			},
		},
	}, true)
}

func (s *S2STestSuite) Scenario_SingleFilePropertyMetadata(svm *ScenarioVariationManager) {
	fileName := "single_file_propertyandmetadata.txt"

	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.File()})), ResourceDefinitionContainer{})
	srcObj := srcContainer.GetObject(svm, fileName, common.EEntityType.File())
	srcBody := NewRandomObjectContentContainer(svm, 0)
	srcProps := ObjectProperties{
		Metadata: common.Metadata{"Author": pointerTo("gapra"), "Viewport": pointerTo("width"), "Description": pointerTo("test file")},
		HTTPHeaders: contentHeaders{
			contentType:        pointerTo("testctype"),
			contentEncoding:    pointerTo("testcenc"),
			contentDisposition: pointerTo("testcdis"),
			contentLanguage:    pointerTo("testclang"),
			cacheControl:       pointerTo("testcctrl"),
		},
	}
	srcObj.Create(svm, srcBody, srcProps)

	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.File()})), ResourceDefinitionContainer{})
	dstObj := dstContainer.GetObject(svm, fileName, common.EEntityType.File())

	RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{srcObj, dstObj.(RemoteResourceManager).WithSpecificAuthType(EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{})},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive: pointerTo(true),
			},
			// Preserve properties behavior is default
		},
	})

	ValidateResource[ObjectResourceManager](svm, dstObj, ResourceDefinitionObject{
		Body:             srcBody,
		ObjectProperties: srcProps,
	}, false)
}

func (s *S2STestSuite) Scenario_BlockBlobBlockBlob(svm *ScenarioVariationManager) {
	fileName := "test_copy.txt"
	size := ResolveVariation(svm, []int64{0, 1, 8*common.MegaByte - 1, 8 * common.MegaByte, 8*common.MegaByte + 1})
	body := NewRandomObjectContentContainer(svm, size)

	srcObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{}).
		GetObject(svm, fileName, common.EEntityType.File())

	srcObj.Create(svm, body, ObjectProperties{BlobProperties: BlobProperties{Type: pointerTo(blob.BlobTypeBlockBlob), BlockBlobAccessTier: pointerTo(blob.AccessTierCool)}})

	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})

	RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{srcObj, dstContainer},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive: pointerTo(true),
			},
		},
	})

	ValidateResource[ObjectResourceManager](svm, dstContainer.GetObject(svm, fileName, common.EEntityType.File()), ResourceDefinitionObject{
		Body: body,
		ObjectProperties: ObjectProperties{
			BlobProperties: BlobProperties{
				Type:                pointerTo(blob.BlobTypeBlockBlob),
				BlockBlobAccessTier: pointerTo(blob.AccessTierCool),
			},
		},
	}, true)
}

func (s *S2STestSuite) Scenario_BlockBlobBlockBlobNoPreserveTier(svm *ScenarioVariationManager) {
	fileName := "test_copy.txt"
	size := int64(4*common.MegaByte + 1)
	body := NewRandomObjectContentContainer(svm, size)

	srcObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{}).
		GetObject(svm, fileName, common.EEntityType.File())

	srcObj.Create(svm, body, ObjectProperties{BlobProperties: BlobProperties{Type: pointerTo(blob.BlobTypeBlockBlob), BlockBlobAccessTier: pointerTo(blob.AccessTierCool)}})

	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})

	RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{srcObj, dstContainer},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive:             pointerTo(true),
				S2SPreserveAccessTier: pointerTo(false),
			},
		},
	})

	ValidateResource[ObjectResourceManager](svm, dstContainer.GetObject(svm, fileName, common.EEntityType.File()), ResourceDefinitionObject{
		Body: body,
		ObjectProperties: ObjectProperties{
			BlobProperties: BlobProperties{
				Type:                pointerTo(blob.BlobTypeBlockBlob),
				BlockBlobAccessTier: pointerTo(blob.AccessTierHot),
			},
		},
	}, true)
}

func (s *S2STestSuite) Scenario_PageBlobToPageBlob(svm *ScenarioVariationManager) {
	fileName := "test_copy.txt"
	size := ResolveVariation(svm, []int64{0, 512, common.KiloByte, 4 * common.MegaByte})
	body := NewRandomObjectContentContainer(svm, size)

	srcObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{}).
		GetObject(svm, fileName, common.EEntityType.File())

	srcObj.Create(svm, body, ObjectProperties{BlobProperties: BlobProperties{Type: pointerTo(blob.BlobTypePageBlob)}})

	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})

	RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{srcObj, dstContainer},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive: pointerTo(true),
			},
		},
	})

	ValidateResource[ObjectResourceManager](svm, dstContainer.GetObject(svm, fileName, common.EEntityType.File()), ResourceDefinitionObject{
		Body: body,
		ObjectProperties: ObjectProperties{
			BlobProperties: BlobProperties{
				Type: pointerTo(blob.BlobTypePageBlob),
			},
		},
	}, true)
}

func (s *S2STestSuite) Scenario_AppendBlobToAppendBlob(svm *ScenarioVariationManager) {
	fileName := "test_copy.txt"
	size := ResolveVariation(svm, []int64{0, 1, 8*common.MegaByte - 1, 8 * common.MegaByte, 8*common.MegaByte + 1})
	body := NewRandomObjectContentContainer(svm, size)

	srcObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{}).
		GetObject(svm, fileName, common.EEntityType.File())

	srcObj.Create(svm, body, ObjectProperties{BlobProperties: BlobProperties{Type: pointerTo(blob.BlobTypeAppendBlob)}})

	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})

	RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{srcObj, dstContainer},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive: pointerTo(true),
			},
		},
	})

	ValidateResource[ObjectResourceManager](svm, dstContainer.GetObject(svm, fileName, common.EEntityType.File()), ResourceDefinitionObject{
		Body: body,
		ObjectProperties: ObjectProperties{
			BlobProperties: BlobProperties{
				Type: pointerTo(blob.BlobTypeAppendBlob),
			},
		},
	}, true)
}

func (s *S2STestSuite) Scenario_OverwriteSingleFile(svm *ScenarioVariationManager) {
	srcFileName := "test_1kb_copy.txt"
	dstFileName := "test_copy.txt"
	srcBody := NewRandomObjectContentContainer(svm, common.KiloByte)
	dstBody := NewRandomObjectContentContainer(svm, 2*common.KiloByte)

	// TODO : Add S3 to source
	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.File()})), ResourceDefinitionContainer{})
	srcObj := srcContainer.GetObject(svm, srcFileName, common.EEntityType.File())
	srcObj.Create(svm, srcBody, ObjectProperties{})

	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})
	dstObj := dstContainer.GetObject(svm, dstFileName, common.EEntityType.File())
	dstObj.Create(svm, dstBody, ObjectProperties{})

	RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{srcObj, dstObj.(RemoteResourceManager).WithSpecificAuthType(ResolveVariation(svm, []ExplicitCredentialTypes{EExplicitCredentialType.SASToken(), EExplicitCredentialType.OAuth()}), svm, CreateAzCopyTargetOptions{})},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive: pointerTo(true),
			},
			// Overwrite behavior is default
		},
	})

	ValidateResource[ObjectResourceManager](svm, dstContainer.GetObject(svm, dstFileName, common.EEntityType.File()), ResourceDefinitionObject{
		Body: srcBody,
	}, true)
}

func (s *S2STestSuite) Scenario_NonOverwriteSingleFile(svm *ScenarioVariationManager) {
	srcFileName := "test_1kb_copy.txt"
	dstFileName := "test_copy.txt"
	srcBody := NewRandomObjectContentContainer(svm, common.KiloByte)
	dstBody := NewRandomObjectContentContainer(svm, 2*common.KiloByte)

	// TODO : Add S3 to source
	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.File()})), ResourceDefinitionContainer{})
	srcObj := srcContainer.GetObject(svm, srcFileName, common.EEntityType.File())
	srcObj.Create(svm, srcBody, ObjectProperties{})

	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})
	dstObj := dstContainer.GetObject(svm, dstFileName, common.EEntityType.File())
	dstObj.Create(svm, dstBody, ObjectProperties{})

	RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{srcObj, dstObj.(RemoteResourceManager).WithSpecificAuthType(ResolveVariation(svm, []ExplicitCredentialTypes{EExplicitCredentialType.SASToken(), EExplicitCredentialType.OAuth()}), svm, CreateAzCopyTargetOptions{})},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive: pointerTo(true),
			},
			Overwrite: pointerTo(false),
		},
	})

	ValidateResource[ObjectResourceManager](svm, dstContainer.GetObject(svm, dstFileName, common.EEntityType.File()), ResourceDefinitionObject{
		Body: dstBody,
	}, true)
}

func (s *S2STestSuite) Scenario_BlobBlobOAuth(svm *ScenarioVariationManager) {
	fileName := "test_copy.txt"
	size := int64(17) * common.MegaByte
	body := NewRandomObjectContentContainer(svm, size)

	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})
	srcObj := srcContainer.GetObject(svm, fileName, common.EEntityType.File())
	srcObj.Create(svm, body, ObjectProperties{})

	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})
	dstObj := dstContainer.GetObject(svm, fileName, common.EEntityType.File())

	RunAzCopy(svm, AzCopyCommand{
		Verb: AzCopyVerbCopy,
		Targets: []ResourceManager{
			srcObj,
			dstObj.(RemoteResourceManager).WithSpecificAuthType(EExplicitCredentialType.OAuth(), svm, CreateAzCopyTargetOptions{})},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive: pointerTo(true),
			},
		},
	})

	ValidateResource[ObjectResourceManager](svm, dstContainer.GetObject(svm, fileName, common.EEntityType.File()), ResourceDefinitionObject{
		Body: body,
	}, true)
}

func (s *S2STestSuite) Scenario_S2SContainerSingleFilePropertyAndMetadata(svm *ScenarioVariationManager) {
	preserveProperty := ResolveVariation(svm, []bool{true, false})
	fileName := "s2scontainer_propertyandmetadata.txt"
	// Scale up from service to object
	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.File()})), ResourceDefinitionContainer{})
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})

	srcObj := srcContainer.GetObject(svm, fileName, common.EEntityType.File())
	srcBody := NewRandomObjectContentContainer(svm, 0)
	srcProps := ObjectProperties{
		Metadata: common.Metadata{"Author": pointerTo("gapra"), "Viewport": pointerTo("width"), "Description": pointerTo("test file")},
		HTTPHeaders: contentHeaders{
			contentType:        pointerTo("testctype"),
			contentEncoding:    pointerTo("testcenc"),
			contentDisposition: pointerTo("testcdis"),
			contentLanguage:    pointerTo("testclang"),
			cacheControl:       pointerTo("testcctrl"),
		},
	}
	srcObj.Create(svm, srcBody, srcProps)

	dstObj := dstContainer.GetObject(svm, fileName, common.EEntityType.File())

	sasOpts := GenericAccountSignatureValues{}

	RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbCopy,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcContainer, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
				TryApplySpecificAuthType(dstContainer, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
				},
				S2SPreserveProperties: pointerTo(preserveProperty),
			},
		})

	ValidateResource[ObjectResourceManager](svm, dstObj, ResourceDefinitionObject{
		Body:             srcBody,
		ObjectProperties: common.Iff(preserveProperty, srcProps, ObjectProperties{}),
	}, false)
}

func (s *S2STestSuite) Scenario_S2SContainerSingleFileStripTopDir(svm *ScenarioVariationManager) {
	recursive := ResolveVariation(svm, []bool{true, false})
	fileName := "copy_strip_top_dir_file.txt"
	// Scale up from service to object
	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.File()})), ResourceDefinitionContainer{})
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})

	srcObj := srcContainer.GetObject(svm, fileName, common.EEntityType.File())
	srcBody := NewRandomObjectContentContainer(svm, 0)
	srcProps := ObjectProperties{
		Metadata: common.Metadata{"Author": pointerTo("gapra"), "Viewport": pointerTo("width"), "Description": pointerTo("test file")},
		HTTPHeaders: contentHeaders{
			contentType:        pointerTo("testctype"),
			contentEncoding:    pointerTo("testcenc"),
			contentDisposition: pointerTo("testcdis"),
			contentLanguage:    pointerTo("testclang"),
			cacheControl:       pointerTo("testcctrl"),
		},
	}
	srcObj.Create(svm, srcBody, srcProps)

	dstObj := dstContainer.GetObject(svm, fileName, common.EEntityType.File())

	sasOpts := GenericAccountSignatureValues{}

	RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbCopy,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcContainer, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
					Wildcard:        true,
				}),
				TryApplySpecificAuthType(dstContainer, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(recursive),
				},
			},
		})

	ValidateResource[ObjectResourceManager](svm, dstObj, ResourceDefinitionObject{
		Body: srcBody,
	}, false)
}

func (s *S2STestSuite) Scenario_S2SAccount(svm *ScenarioVariationManager) {
	// Scale up from service to object
	srcAccount := GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.File()}))
	dstAccount := GetRootResource(svm, common.ELocation.Blob())
	srcContainer := CreateResource[ContainerResourceManager](svm, srcAccount, ResourceDefinitionContainer{})
	dstContainer := CreateResource[ContainerResourceManager](svm, dstAccount, ResourceDefinitionContainer{})

	dirsToCreate := []string{"dir_file_copy_test", "dir_file_copy_test/sub_dir_copy_test"}

	// Create destination directories
	srcObjs := make(ObjectResourceMappingFlat)
	for _, dir := range dirsToCreate {
		obj := ResourceDefinitionObject{ObjectName: pointerTo(dir), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}}
		if dstContainer.Location() != common.ELocation.Blob() {
			srcObjs[dir] = obj
		}
		for i := range 10 {
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

	sasOpts := GenericAccountSignatureValues{}

	RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbCopy,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcAccount, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
				TryApplySpecificAuthType(dstAccount, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
				},
			},
		})

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, true)
}
