package e2etest

import (
	"strconv"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	blobsas "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func init() {
	suiteManager.RegisterSuite(&RemoveSuite{})
}

type RemoveSuite struct{}

func (s *RemoveSuite) SetupSuite(a Asserter) {
	//a.Log("Setup logging!")
}

func (s *RemoveSuite) TeardownSuite(a Asserter) {
	//a.Log("Teardown logging!")
	//a.Error("Oops!")
}

func (s *RemoveSuite) Scenario_SingleFileRemoveBlobFSEncodedPath(svm *ScenarioVariationManager) {
	acct := GetAccount(svm, PrimaryHNSAcct)
	srcService := acct.GetService(svm, ResolveVariation(svm, []common.Location{common.ELocation.BlobFS()}))

	svm.InsertVariationSeparator(":")
	body := NewRandomObjectContentContainer(SizeFromString("0K"))
	// Scale up from service to object
	srcObj := CreateResource[ObjectResourceManager](svm, srcService, ResourceDefinitionObject{
		ObjectName: pointerTo("%23%25%3F"),
		Body:       body,
	})

	RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: ResolveVariation(svm, []AzCopyVerb{AzCopyVerbRemove}),
			Targets: []ResourceManager{
				srcObj.(RemoteResourceManager).WithSpecificAuthType(EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: GenericServiceSignatureValues{
						ContainerName: srcObj.ContainerName(),
						Permissions:   (&blobsas.BlobPermissions{Read: true, List: true, Delete: true}).String(),
					},
				}),
			},
			Flags: RemoveFlags{},
		})
	ValidateResource[ObjectResourceManager](svm, srcObj, ResourceDefinitionObject{
		ObjectShouldExist: to.Ptr(false),
	}, ValidateResourceOptions{
		validateObjectContent: false,
	})
}

func (s *RemoveSuite) Scenario_EmptySASErrorCodes(svm *ScenarioVariationManager) {
	// Scale up from service to object
	// File - ShareNotFound error
	// BlobFS - errors out in log file, not stdout
	srcObj := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob()})), ResourceDefinitionObject{})

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbRemove,
			Targets: []ResourceManager{
				AzCopyTarget{srcObj, EExplicitCredentialType.PublicAuth(), CreateAzCopyTargetOptions{}},
			},
			ShouldFail: true,
		})

	// Validate that the stdout contains these error URLs
	ValidateMessageOutput(svm, stdout, "https://aka.ms/AzCopyError/NoAuthenticationInformation", true)
}

func (s *RemoveSuite) Scenario_RemoveVirtualDirectory(svm *ScenarioVariationManager) {
	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})

	srcObject := srcContainer.GetObject(svm, "dir_5_files", common.EEntityType.Folder())

	srcObjs := make(ObjectResourceMappingFlat)
	for i := range 5 {
		name := "dir_5_files/test" + strconv.Itoa(i) + ".txt"
		obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: NewRandomObjectContentContainer(SizeFromString("1K"))}
		CreateResource[ObjectResourceManager](svm, srcContainer, obj)
		obj.Body = nil
		obj.ObjectShouldExist = to.Ptr(false)
		srcObjs[name] = obj
	}

	RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbRemove,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcObject, ResolveVariation(svm, []ExplicitCredentialTypes{EExplicitCredentialType.OAuth(), EExplicitCredentialType.SASToken()}), svm, CreateAzCopyTargetOptions{}),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
				},
			},
		})

	ValidateResource[ContainerResourceManager](svm, srcContainer, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, ValidateResourceOptions{
		validateObjectContent: true,
	})
}

// Scenario_RemoveFileWithOnlyDotsTrailingDotDisabled tests removing a file with only dots. i.e "...."
// remove with trailing dot flag disabled does not delete any files until trailing dot is enabled
func (s *RemoveSuite) Scenario_RemoveFileWithOnlyDotsTrailingDotDisabled(svm *ScenarioVariationManager) {
	// File Share
	fileShare := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.File()),
		ResourceDefinitionContainer{})

	// File to remove with multiple dots
	srcObject := fileShare.GetObject(svm, "...", common.EEntityType.File())
	srcObject.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{})

	// Fill the file share with other files
	for i := range 3 {
		name := "test" + strconv.Itoa(i) + ".txt"
		fileObject := fileShare.GetObject(svm, name, common.EEntityType.File())
		fileObject.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{})
	}
	stdOut, _ := RunAzCopy(svm,
		AzCopyCommand{
			Verb: AzCopyVerbRemove,
			Targets: []ResourceManager{
				srcObject,
			},
			Flags: RemoveFlags{
				TrailingDot: pointerTo(common.ETrailingDotOption.Disable()),
				Recursive:   pointerTo(true),
				FromTo:      pointerTo(common.EFromTo.FileTrash()),
				GlobalFlags: GlobalFlags{
					OutputType: pointerTo(common.EOutputFormat.Text()),
				},
			},
			ShouldFail: true, // AzCopy should not continue operation
		})
	ValidateMessageOutput(svm, stdOut, "Retry remove command with default --trailing-dot=Enable", true)

	// Validate that no files are deleted in File share
	fileMap := fileShare.ListObjects(svm, "", true)
	svm.Assert("No files should be removed", Equal{}, len(fileMap), 4)

}

// Scenario_RemoveFileWithOnlyDots tests removing a file with only dots. i.e "...."
// with trailing dot flag enabled correctly deletes only that file
func (s *RemoveSuite) Scenario_RemoveFileWithOnlyDotsEnabled(svm *ScenarioVariationManager) {
	// File Share
	fileShare := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.File()),
		ResourceDefinitionContainer{})

	// Create parent directory to replicate scenario
	dirName := "dir"
	srcObjs := make(ObjectResourceMappingFlat)
	srcObjs[dirName] = ResourceDefinitionObject{
		ObjectName:       pointerTo(dirName),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}}

	// File to remove with multiple dots
	srcObject := CreateResource[ObjectResourceManager](svm, fileShare, ResourceDefinitionObject{
		ObjectName: pointerTo("..."),
		Body:       NewZeroObjectContentContainer(0),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.File(),
		},
	})

	// Fill the file share with other files
	for i := range 3 {
		name := dirName + "/test" + strconv.Itoa(i) + ".txt"
		fileObject := fileShare.GetObject(svm, name, common.EEntityType.File())
		fileObject.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{
			EntityType: common.EEntityType.File(),
		})
	}

	RunAzCopy(svm,
		AzCopyCommand{
			Verb: AzCopyVerbRemove,
			Targets: []ResourceManager{
				srcObject,
			},
			Flags: RemoveFlags{
				TrailingDot: pointerTo(common.ETrailingDotOption.Enable()),
				Recursive:   pointerTo(true),
				FromTo:      pointerTo(common.EFromTo.FileTrash()),
			},
		})

	// Validate that relevant file is deleted in File share - does not exist
	ValidateResource[ObjectResourceManager](svm, srcObject, ResourceDefinitionObject{
		ObjectShouldExist: pointerTo(false),
	}, ValidateResourceOptions{
		validateObjectContent: false,
	})

	fileMap := make(map[string]ObjectProperties)
	fileMap = fileShare.ListObjects(svm, "", true)
	// Folders are objects, fileMap contains test1, test2, test3 and dir
	svm.Assert("One file should be removed", Equal{}, len(fileMap), 4)

}

func (s *RemoveSuite) Scenario_RemoveFilesWithExcludePath(svm *ScenarioVariationManager) {
	src := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.File()),
		ResourceDefinitionContainer{})

	fullList := []string{
		"file1.txt",
		"file2.pdf",
		"myfoldertoexclude/file3.json",
		"myfoldertoexclude/subfolder/file4.txt",
		"anotherfolder/file5.txt",
		"anotherfolder/subfolder/file6.txt",
		"rootfile.txt",
		"sub/myfoldertoexclude/file1.txt",
	}
	// create objs in different paths to test exclude-path functionality
	for i := range len(fullList) {
		obj := src.GetObject(svm, fullList[i], common.EEntityType.File())
		obj.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{})
	}

	// the files remaining
	expectedList := []string{
		"myfoldertoexclude/file3.json",
		"myfoldertoexclude/subfolder/file4.txt",
		"sub/myfoldertoexclude/file1.txt",
	}

	stdOut, _ := RunAzCopy(svm,
		AzCopyCommand{
			Verb:    AzCopyVerbRemove,
			Targets: []ResourceManager{src},
			Flags: RemoveFlags{
				Recursive:   pointerTo(true),
				ExcludePath: pointerTo("myfoldertoexclude"),
			},
		})

	fileMap := make(map[string]ObjectProperties)
	fileMap = src.ListObjects(svm, "", true)
	svm.Assert("Only necessary files should be removed",
		Equal{}, len(fileMap)-1, len(expectedList)) // Minus 1 to remove the directory
	ValidateResource[ContainerResourceManager](svm, src, ResourceDefinitionContainer{}, ValidateResourceOptions{
		validateObjectContent: true,
	})
	ValidateDoesNotContainError(svm, stdOut, []string{"unknown flag: --exclude-path"})
}

// test files are not removed when exclude path is specified
func (s *RemoveSuite) Scenario_RemoveBlobsWithExcludePath(svm *ScenarioVariationManager) {
	src := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()),
		ResourceDefinitionContainer{})

	fullList := []string{
		"myfolder/subfolder1/blob1.json",
		"myfolder/subfolder2/blob2.json",
		"myfoldertoexclude/blob4.jpg",
		"myfoldertoexclude/blob5.jpg",
	}
	// create objs in different paths to test exclude-path functionality
	for i := range len(fullList) {
		obj := src.GetObject(svm, fullList[i], common.EEntityType.File())
		obj.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{})
	}

	// the files remaining
	expectedList := []string{
		"myfoldertoexclude/blob4.jpg",
		"myfoldertoexclude/blob5.jpg",
	}

	stdOut, _ := RunAzCopy(svm,
		AzCopyCommand{
			Verb:    AzCopyVerbRemove,
			Targets: []ResourceManager{src},
			Flags: RemoveFlags{
				Recursive:   pointerTo(true),
				ExcludePath: pointerTo("myfoldertoexclude"),
			},
		})

	fileMap := make(map[string]ObjectProperties)
	fileMap = src.ListObjects(svm, "", true)
	svm.Assert("Only necessary files should be removed", Equal{}, len(fileMap), len(expectedList))
	ValidateResource[ContainerResourceManager](svm, src, ResourceDefinitionContainer{}, ValidateResourceOptions{
		validateObjectContent: true,
	})
	ValidateDoesNotContainError(svm, stdOut, []string{"unknown flag: --exclude-path"})
}

// Scenario_RemoveFilesWithSpecialChars validates that we correctly remove special chars and do not decode them prematurely
func (s *RemoveSuite) Scenario_RemoveFilesWithSpecialChars(svm *ScenarioVariationManager) {
	if !svm.Dryrun() {
		src := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(),
			common.ELocation.File()})),
			ResourceDefinitionContainer{})

		fullList := []string{
			"sample/sample%5C.json",
			"test/test%5C.json",
			"ex%5C",
			"regular_file.txt",

			// other encoded chars
			"file%2Ffile.png",
			"file/file.png",
			"test/test%aC.json",
			"test/test%25aC.json",
		}
		// create objs
		for i := range len(fullList) {
			obj := src.GetObject(svm, fullList[i], common.EEntityType.File())
			obj.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{})
		}

		stdOut, _ := RunAzCopy(svm,
			AzCopyCommand{
				Verb:    AzCopyVerbRemove,
				Targets: []ResourceManager{src},
				Flags: RemoveFlags{
					Recursive: pointerTo(true),
				},
			})

		fileMap := make(map[string]ObjectProperties)
		fileMap = src.ListObjects(svm, "", true)
		svm.Assert("All files should be removed", Equal{}, len(fileMap), 0)
		ValidateResource[ContainerResourceManager](svm, src, ResourceDefinitionContainer{}, ValidateResourceOptions{validateObjectContent: false})
		ValidateDoesNotContainError(svm, stdOut, []string{"inconsistent path separators. Some are forward, some are back. This is not supported"})
	}
}
