package e2etest

import (
	"fmt"
	"strconv"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-storage-azcopy/v10/common"
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
	body := NewRandomObjectContentContainer(size)

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
	body := NewRandomObjectContentContainer(int64(size))

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
	body := NewRandomObjectContentContainer(512)

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
	srcBody := NewRandomObjectContentContainer(0)
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

	stdOut, _ := RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{srcObj, dstObj.(RemoteResourceManager).WithSpecificAuthType(EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{})},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive: pointerTo(true),
			},
			// Preserve properties behavior is default
		},
	})
	fmt.Println("StdOUT--------", stdOut)
	ValidateResource[ObjectResourceManager](svm, dstObj, ResourceDefinitionObject{
		Body:             srcBody,
		ObjectProperties: srcProps,
	}, false)
}

func (s *S2STestSuite) Scenario_BlockBlobBlockBlob(svm *ScenarioVariationManager) {
	fileName := "test_copy.txt"
	size := ResolveVariation(svm, []int64{0, 1, 8*common.MegaByte - 1, 8 * common.MegaByte, 8*common.MegaByte + 1})
	body := NewRandomObjectContentContainer(size)

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
	body := NewRandomObjectContentContainer(size)

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
	body := NewRandomObjectContentContainer(size)

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
	body := NewRandomObjectContentContainer(size)

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
	srcBody := NewRandomObjectContentContainer(common.KiloByte)
	dstBody := NewRandomObjectContentContainer(2 * common.KiloByte)

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
	srcBody := NewRandomObjectContentContainer(common.KiloByte)
	dstBody := NewRandomObjectContentContainer(2 * common.KiloByte)

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
	body := NewRandomObjectContentContainer(size)

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
	srcBody := NewRandomObjectContentContainer(0)
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
	srcBody := NewRandomObjectContentContainer(0)
	srcObj.Create(svm, srcBody, ObjectProperties{})

	dstObj := dstContainer.GetObject(svm, fileName, common.EEntityType.File())

	sasOpts := GenericAccountSignatureValues{}

	RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbCopy,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcContainer, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
					Wildcard:        "/*",
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

func (s *S2STestSuite) Scenario_S2SDirectoryMultipleFiles(svm *ScenarioVariationManager) {
	// Scale up from service to object
	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.File()})), ResourceDefinitionContainer{})
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})

	dirsToCreate := []string{"dir_file_copy_test", "dir_file_copy_test/sub_dir_copy_test"}

	// Create destination directories
	dstObjs := make(ObjectResourceMappingFlat)
	for _, dir := range dirsToCreate {
		if dstContainer.Location() != common.ELocation.Blob() {
			dstObj := ResourceDefinitionObject{ObjectName: pointerTo("dir_file_copy_test/" + dir), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}}
			dstObjs["dir_file_copy_test/"+dir] = dstObj
		}
		for i := range 10 {
			name := dir + "/test" + strconv.Itoa(i) + ".txt"
			body := NewRandomObjectContentContainer(SizeFromString("1K"))
			obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: body}
			CreateResource[ObjectResourceManager](svm, srcContainer, obj)
			dstObj := ResourceDefinitionObject{ObjectName: pointerTo("dir_file_copy_test/" + name), Body: body}
			dstObjs["dir_file_copy_test/"+name] = dstObj // src directory will be created in dest directory
		}
	}

	sasOpts := GenericAccountSignatureValues{}

	srcDirObj := srcContainer.GetObject(svm, "dir_file_copy_test", common.EEntityType.Folder())
	dstDirObj := dstContainer.GetObject(svm, "dir_file_copy_test", common.EEntityType.Folder())

	RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbCopy,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcDirObj, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
				TryApplySpecificAuthType(dstDirObj, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
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
		Objects: dstObjs,
	}, true)
}

func (s *S2STestSuite) Scenario_S2SDirectoryMultipleFilesStripTopDirRecursive(svm *ScenarioVariationManager) {
	// Scale up from service to object
	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.File()})), ResourceDefinitionContainer{})
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})

	dirsToCreate := []string{"dir_file_copy_test", "dir_file_copy_test/sub_dir_copy_test"}

	// Create destination directories
	dstObjs := make(ObjectResourceMappingFlat)
	for _, dir := range dirsToCreate {
		if dstContainer.Location() != common.ELocation.Blob() {
			dirName := dir
			dstObj := ResourceDefinitionObject{ObjectName: pointerTo(dirName), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}}
			dstObjs[dirName] = dstObj
		}
		for i := range 10 {
			name := dir + "/test" + strconv.Itoa(i) + ".txt"
			body := NewRandomObjectContentContainer(SizeFromString("1K"))
			obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: body}
			CreateResource[ObjectResourceManager](svm, srcContainer, obj)
			dstObj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: body}
			dstObjs[name] = dstObj
		}
	}

	sasOpts := GenericAccountSignatureValues{}

	srcDirObj := srcContainer.GetObject(svm, "dir_file_copy_test", common.EEntityType.Folder())
	dstDirObj := dstContainer.GetObject(svm, "dir_file_copy_test", common.EEntityType.Folder())

	RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbCopy,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcDirObj, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
					Wildcard:        "/*",
				}),
				TryApplySpecificAuthType(dstDirObj, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
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
		Objects: dstObjs,
	}, true)
}

func (s *S2STestSuite) Scenario_S2SDirectoryMultipleFilesStripTopDirNonRecursive(svm *ScenarioVariationManager) {
	// Scale up from service to object
	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.File()})), ResourceDefinitionContainer{})
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})

	dirsToCreate := []string{"dir_file_copy_test", "dir_file_copy_test/sub_dir_copy_test"}

	// Create destination directories
	dstObjs := make(ObjectResourceMappingFlat)
	for j, dir := range dirsToCreate {
		if dstContainer.Location() != common.ELocation.Blob() {
			dirName := dir
			if j == 0 {
				dstObj := ResourceDefinitionObject{ObjectName: pointerTo(dirName), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}}
				dstObjs[dirName] = dstObj
			}
		}
		for i := range 10 {
			name := dir + "/test" + strconv.Itoa(i) + ".txt"
			body := NewRandomObjectContentContainer(SizeFromString("1K"))
			obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: body}
			CreateResource[ObjectResourceManager](svm, srcContainer, obj)
			if j == 0 {
				dstObj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: body}
				dstObjs[name] = dstObj
			}
		}
	}

	sasOpts := GenericAccountSignatureValues{}

	srcDirObj := srcContainer.GetObject(svm, "dir_file_copy_test", common.EEntityType.Folder())
	dstDirObj := dstContainer.GetObject(svm, "dir_file_copy_test", common.EEntityType.Folder())

	RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbCopy,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcDirObj, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
					Wildcard:        "/*",
				}),
				TryApplySpecificAuthType(dstDirObj, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
			},
			Flags: CopyFlags{},
		})

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: dstObjs,
	}, true)
}

func (s *S2STestSuite) Scenario_SystemContainerCopy(svm *ScenarioVariationManager) {
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync})

	dstObj := GetAccount(svm, PrimaryStandardAcct).GetService(svm, common.ELocation.Blob()).GetContainer("$logs")
	srcObj := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Local(), common.ELocation.Blob()})), ResourceDefinitionObject{})

	sasOpts := GenericAccountSignatureValues{}

	stdOut, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: azCopyVerb,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcObj, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
				TryApplySpecificAuthType(dstObj, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
				},
			},
			ShouldFail: true,
		})
	ValidateMessageOutput(svm, stdOut, "cannot copy to system container", true)
}
