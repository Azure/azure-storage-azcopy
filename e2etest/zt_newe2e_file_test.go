package e2etest

import (
	"context"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"math"
	"os"
	"strconv"
)

func init() {
	suiteManager.RegisterSuite(&FileTestSuite{})
}

type FileTestSuite struct{}

func (s *FileTestSuite) Scenario_SingleFileUploadDifferentSizes(svm *ScenarioVariationManager) {
	size := ResolveVariation(svm, []int64{0, 1, 4*common.MegaByte - 1, 4 * common.MegaByte, 4*common.MegaByte + 1})
	fileName := fmt.Sprintf("test_file_upload_%dB_fullname", size)
	body := NewRandomObjectContentContainer(size)

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
	body := NewRandomObjectContentContainer(size)

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
	body := NewRandomObjectContentContainer(size)

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
	body := NewRandomObjectContentContainer(63 * common.MegaByte)
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
			obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: NewRandomObjectContentContainer(SizeFromString("1K"))}
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
			obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: NewRandomObjectContentContainer(SizeFromString("1K"))}
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
	body := NewRandomObjectContentContainer(int64(size))

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
	body := NewRandomObjectContentContainer(int64(size))

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
	body := NewRandomObjectContentContainer(int64(size))

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
	body := NewRandomObjectContentContainer(int64(size))

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
	body := NewRandomObjectContentContainer(int64(size))

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

// Test copy with AllowToUnsafeDestination option
func (s *FileTestSuite) Scenario_CopyTrailingDotUnsafeDestination(svm *ScenarioVariationManager) {
	body := NewRandomObjectContentContainer(0)

	name := "test."
	srcObj := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.File(), common.ELocation.Local()})),
		ResourceDefinitionObject{ObjectName: pointerTo(name), Body: body})
	dstObj := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Local(), common.ELocation.File()})),
		ResourceDefinitionObject{ObjectName: pointerTo("test"), Body: body})

	if srcObj.Location() == dstObj.Location() {
		svm.InvalidateScenario()
		return
	}

	RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbCopy,
			Targets: []ResourceManager{TryApplySpecificAuthType(srcObj, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{}),
				TryApplySpecificAuthType(dstObj, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{})},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					BlockSizeMB: pointerTo(4.0),
					TrailingDot: to.Ptr(common.ETrailingDotOption.AllowToUnsafeDestination()),
				},
				ListOfFiles: []string{"lof"},
			},
		})

	ValidateResource[ObjectResourceManager](svm, dstObj, ResourceDefinitionObject{
		Body: body,
	}, false)
}

// Test:
// - correct number of non-empty files are uploaded when file share quota is hit
func (s *FileTestSuite) Scenario_UploadFilesWithQuota(svm *ScenarioVariationManager) {
	if svm.Dryrun() {
		return
	}

	quotaGB := int32(1) // 1 GB quota
	shareResource := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.File()), ResourceDefinitionContainer{
		Properties: ContainerProperties{
			FileContainerProperties: FileContainerProperties{
				Quota: &quotaGB},
		},
	})
	svm.Assert("Quota is 1GB", Equal{Deep: true},
		DerefOrZero(shareResource.GetProperties(svm).FileContainerProperties.Quota), int32(1))

	// Fill the share up
	if !svm.Dryrun() {
		shareClient := shareResource.(*FileShareResourceManager).internalClient
		fileClient := shareClient.NewRootDirectoryClient().NewFileClient("big.txt")
		_, err := fileClient.Create(ctx, 990*common.MegaByte, nil)
		svm.NoError("Create large file", err)
	}

	srcOverflowObject := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, common.ELocation.Local()),
		ResourceDefinitionObject{
			Body: NewRandomObjectContentContainer(common.GigaByte),
		})
	env := &AzCopyEnvironment{
		InheritEnvironment: map[string]bool{},
	}
	stdOut, _ := RunAzCopy(svm, AzCopyCommand{
		Verb: AzCopyVerbCopy,
		Targets: []ResourceManager{
			TryApplySpecificAuthType(srcOverflowObject, EExplicitCredentialType.OAuth(), svm, CreateAzCopyTargetOptions{}),
			TryApplySpecificAuthType(shareResource, EExplicitCredentialType.OAuth(), svm, CreateAzCopyTargetOptions{}),
		},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive: pointerTo(true),
			},
		},
		ShouldFail:  true,
		Environment: env,
	})

	// Error catchers for full file share
	ValidateContainsError(svm, stdOut, []string{"Increase the file share quota and call Resume command."})

	fileMap := shareResource.ListObjects(svm, "", true)
	svm.Assert("One file should be uploaded within the quota", Equal{}, len(fileMap), 1)

	// Increase quota to fit all files
	newQuota := int32(2)
	if resourceManager, ok := shareResource.(*FileShareResourceManager); ok {
		resourceManager.SetProperties(svm, &ContainerProperties{
			FileContainerProperties: FileContainerProperties{
				Quota: &newQuota}})
	}

	// Validate correctly SetProperties updates quota. Prevent nil deref in dry runs
	svm.Assert("Quota should be updated", Equal{},
		DerefOrZero(shareResource.GetProperties(svm).FileContainerProperties.Quota),
		newQuota)

	var jobId string
	if parsedOut, ok := stdOut.(*AzCopyParsedCopySyncRemoveStdout); ok {
		if parsedOut.InitMsg.JobID != "" {
			jobId = parsedOut.InitMsg.JobID
		}
	} else {
		// Will enter during dry runs
		fmt.Println("failed to cast to AzCopyParsedCopySyncRemoveStdout")
	}
	resStdOut, _ := RunAzCopy(svm, AzCopyCommand{
		Verb:           AzCopyVerbJobs,
		PositionalArgs: []string{"resume", jobId},
		Environment:    env, // Resume job with same log and job plan folders
		ShouldFail:     true,
	})

	if env.LogLocation != nil {
		if _, err := os.Stat(*env.LogLocation); os.IsNotExist(err) {
			svm.Log("Log directory does not exist: %s", *env.LogLocation)
		}
		svm.Log("Log location: %v", DerefOrZero(env.LogLocation))
	}

	if !svm.Dryrun() {
		svm.Log("%v", resStdOut)
	}

	// Validate all files can be uploaded after resume and quota increase
	fileMapResume := shareResource.ListObjects(svm, "", true)
	svm.Assert("All files should be successfully uploaded after quota increase",
		Equal{}, len(fileMapResume), 2)
}

// Test that POSIX errors are not returned in a RemoteLocal transfer
func (s *FileTestSuite) Scenario_SingleFileDownloadNoError(svm *ScenarioVariationManager) {
	body := NewRandomObjectContentContainer(0)
	srcObj := CreateResource[ObjectResourceManager](svm,
		GetRootResource(svm, common.ELocation.File()),
		ResourceDefinitionObject{Body: body})
	destObj := CreateResource[ObjectResourceManager](svm,
		GetRootResource(svm, common.ELocation.Local()),
		ResourceDefinitionObject{Body: body})

	stdOut, _ := RunAzCopy(svm, AzCopyCommand{
		Verb:       AzCopyVerbCopy,
		Targets:    []ResourceManager{srcObj, destObj},
		ShouldFail: false,
	})

	//ValidateResource[ObjectResourceManager](svm, destObj, ResourceDefinitionObject{
	//	Body: body,
	//}, true)

	ValidateDoesNotContainError(svm, stdOut, []string{"interrupted system call"})
}
