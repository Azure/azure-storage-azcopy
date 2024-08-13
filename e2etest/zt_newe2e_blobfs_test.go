package e2etest

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"strconv"
)

func init() {
	suiteManager.RegisterSuite(&BlobFSTestSuite{})
}

type BlobFSTestSuite struct{}

func (s *BlobFSTestSuite) Scenario_UploadFile(svm *ScenarioVariationManager) {
	fileName := "test.txt"
	size := ResolveVariation(svm, []int64{common.KiloByte, 64 * common.MegaByte})
	body := NewRandomObjectContentContainer(svm, size)

	srcObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{}).
		GetObject(svm, fileName, common.EEntityType.File())
	srcObj.Create(svm, body, ObjectProperties{})

	acct := GetAccount(svm, PrimaryHNSAcct)
	dstService := acct.GetService(svm, common.ELocation.BlobFS())
	dstContainer := CreateResource[ContainerResourceManager](svm, dstService, ResourceDefinitionContainer{})

	RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{srcObj, dstContainer.(RemoteResourceManager).WithSpecificAuthType(ResolveVariation(svm, []ExplicitCredentialTypes{EExplicitCredentialType.SASToken(), EExplicitCredentialType.OAuth()}), svm, CreateAzCopyTargetOptions{})},
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

func (s *BlobFSTestSuite) Scenario_UploadFileMultiflushOAuth(svm *ScenarioVariationManager) {
	fileName := "test_multiflush_64MB_file.txt"
	body := NewRandomObjectContentContainer(svm, 64*common.MegaByte)

	srcObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{}).
		GetObject(svm, fileName, common.EEntityType.File())
	srcObj.Create(svm, body, ObjectProperties{})

	acct := GetAccount(svm, PrimaryHNSAcct)
	dstService := acct.GetService(svm, common.ELocation.BlobFS())
	dstContainer := CreateResource[ContainerResourceManager](svm, dstService, ResourceDefinitionContainer{})

	flushThreshold := ResolveVariation(svm, []uint32{15, 16}) // uneven, even

	// Upload the file using AzCopy @ 1MB blocks, 15 block flushes (5 flushes, 4 15 blocks, 1 4 blocks)
	RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{srcObj, dstContainer.(RemoteResourceManager).WithSpecificAuthType(EExplicitCredentialType.OAuth(), svm, CreateAzCopyTargetOptions{})},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive:   pointerTo(true),
				BlockSizeMB: pointerTo(1.0),
			},
			ADLSFlushThreshold: pointerTo(flushThreshold),
		},
	})

	ValidateResource[ObjectResourceManager](svm, dstContainer.GetObject(svm, fileName, common.EEntityType.File()), ResourceDefinitionObject{
		Body: body,
	}, true)
}

func (s *BlobFSTestSuite) Scenario_Upload100Files(svm *ScenarioVariationManager) {
	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})
	acct := GetAccount(svm, PrimaryHNSAcct)
	dstService := acct.GetService(svm, common.ELocation.BlobFS())
	dstContainer := CreateResource[ContainerResourceManager](svm, dstService, ResourceDefinitionContainer{})

	srcObject := srcContainer.GetObject(svm, "dir_100_files", common.EEntityType.Folder())

	srcObjs := make(ObjectResourceMappingFlat)
	for i := range 100 {
		name := "dir_100_files/test" + strconv.Itoa(i) + ".txt"
		obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: NewRandomObjectContentContainer(svm, SizeFromString("1K"))}
		CreateResource[ObjectResourceManager](svm, srcContainer, obj)
		srcObjs[name] = obj
	}

	RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{srcObject, dstContainer.(RemoteResourceManager).WithSpecificAuthType(ResolveVariation(svm, []ExplicitCredentialTypes{EExplicitCredentialType.SASToken(), EExplicitCredentialType.OAuth()}), svm, CreateAzCopyTargetOptions{})},
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

func (s *BlobFSTestSuite) Scenario_DownloadFile(svm *ScenarioVariationManager) {
	fileName := "test.txt"
	size := ResolveVariation(svm, []int64{common.KiloByte, 64 * common.MegaByte})
	body := NewRandomObjectContentContainer(svm, size)

	dstObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{}).
		GetObject(svm, fileName, common.EEntityType.File())

	acct := GetAccount(svm, PrimaryHNSAcct)
	srcService := acct.GetService(svm, common.ELocation.BlobFS())
	srcContainer := CreateResource[ContainerResourceManager](svm, srcService, ResourceDefinitionContainer{})
	srcObj := srcContainer.GetObject(svm, fileName, common.EEntityType.File())
	srcObj.Create(svm, body, ObjectProperties{})

	RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{srcObj.(RemoteResourceManager).WithSpecificAuthType(ResolveVariation(svm, []ExplicitCredentialTypes{EExplicitCredentialType.SASToken(), EExplicitCredentialType.OAuth()}), svm, CreateAzCopyTargetOptions{}), dstObj},
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

func (s *BlobFSTestSuite) Scenario_Download100Files(svm *ScenarioVariationManager) {
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})
	acct := GetAccount(svm, PrimaryHNSAcct)
	srcService := acct.GetService(svm, common.ELocation.BlobFS())
	srcContainer := CreateResource[ContainerResourceManager](svm, srcService, ResourceDefinitionContainer{})

	srcObject := srcContainer.GetObject(svm, "dir_100_files", common.EEntityType.Folder())

	srcObjs := make(ObjectResourceMappingFlat)
	for i := range 100 {
		name := "dir_100_files/test" + strconv.Itoa(i) + ".txt"
		obj := ResourceDefinitionObject{ObjectName: pointerTo(name), Body: NewRandomObjectContentContainer(svm, SizeFromString("1K"))}
		CreateResource[ObjectResourceManager](svm, srcContainer, obj)
		srcObjs[name] = obj
	}

	RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{srcObject.(RemoteResourceManager).WithSpecificAuthType(ResolveVariation(svm, []ExplicitCredentialTypes{EExplicitCredentialType.SASToken(), EExplicitCredentialType.OAuth()}), svm, CreateAzCopyTargetOptions{}), dstContainer},
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
