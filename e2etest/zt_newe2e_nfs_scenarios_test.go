package e2etest

import (
	"os/user"
	"runtime"
	"strconv"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/google/uuid"
)

func init() {
	suiteManager.RegisterSuite(&FilesNFSTestSuite{})
}

type FilesNFSTestSuite struct{}

func GetCurrentUIDAndGID(a Asserter) (uid, gid string) {
	// Get the current user information
	currentUser, err := user.Current()
	a.NoError("Error retrieving current user:", err)

	uid = currentUser.Uid
	gid = currentUser.Gid
	return
}

func getPropertiesAndPermissions(svm *ScenarioVariationManager, preserveProperties, preservePermissions bool) (*FileNFSProperties, *FileNFSProperties, *FileNFSPermissions) {
	uid, gid := GetCurrentUIDAndGID(svm)
	var folderProperties, fileProperties *FileNFSProperties
	if preserveProperties {
		folderProperties = &FileNFSProperties{
			FileCreationTime: pointerTo(time.Now()),
		}
		fileProperties = &FileNFSProperties{
			FileCreationTime:  pointerTo(time.Now()),
			FileLastWriteTime: pointerTo(time.Now()),
		}
	}
	var fileOrFolderPermissions *FileNFSPermissions
	if preservePermissions {
		fileOrFolderPermissions = &FileNFSPermissions{
			Owner:    pointerTo(uid),
			Group:    pointerTo(gid),
			FileMode: pointerTo("0755"),
		}
	}
	return folderProperties, fileProperties, fileOrFolderPermissions
}

func (s *FilesNFSTestSuite) Scenario_LocalLinuxToAzureNFS(svm *ScenarioVariationManager) {

	// 	Test Scenario:
	// 	1. Create a NFS enabled file share container in Azure
	// 	2. Create a folder with some files in it. Create a regular,special file,symlink and hardlink files in the folder.
	// 	3. Run azcopy copy/sync command to copy the folder from Azure NFS enabled file share to local.
	// 	4. Hardlinked files should be downloaded as regular files. Hardlinks will not be preserved.
	// 	5. Number of hardlinks converted count will be displayed in job's summary
	//  6. Symlinked and special files should be skipped and number of skipped files will be displayed in job's summary

	if runtime.GOOS == "windows" || runtime.GOOS == "darwin" {
		svm.InvalidateScenario()
		return
	}
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync}) // Calculate verb early to create the destination object early
	preserveProperties := ResolveVariation(svm, []bool{true, false})
	preservePermissions := ResolveVariation(svm, []bool{true, false})

	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.FileNFS()}), GetResourceOptions{
		PreferredAccount: pointerTo(PremiumFileShareAcct),
	}), ResourceDefinitionContainer{
		Properties: ContainerProperties{
			FileContainerProperties: FileContainerProperties{
				EnabledProtocols: pointerTo("NFS"),
			},
		},
	})
	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})

	rootDir := "dir_file_copy_test_" + uuid.NewString()

	var dst ResourceManager
	if azCopyVerb == AzCopyVerbSync {
		dstObj := dstContainer.GetObject(svm, rootDir+"/test1.txt", common.EEntityType.File())
		dstObj.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{})
		if !svm.Dryrun() {
			// Make sure the LMT is in the past
			time.Sleep(time.Second * 5)
		}
		dst = dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	} else {
		dst = dstContainer
	}

	folderProperties, fileProperties, fileOrFolderPermissions := getPropertiesAndPermissions(svm, preserveProperties, preservePermissions)

	srcObjs := make(ObjectResourceMappingFlat)
	srcObjRes := make(map[string]ObjectResourceManager)

	obj := ResourceDefinitionObject{
		ObjectName: pointerTo(rootDir),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Folder(),
			FileNFSProperties:  folderProperties,
			FileNFSPermissions: fileOrFolderPermissions,
		},
	}
	srcObjRes[rootDir] = CreateResource[ObjectResourceManager](svm, srcContainer, obj)
	srcObjs[rootDir] = obj

	for i := range 2 {
		name := rootDir + "/test" + strconv.Itoa(i) + ".txt"
		obj := ResourceDefinitionObject{
			ObjectName: pointerTo(name),
			Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
			ObjectProperties: ObjectProperties{
				EntityType:         common.EEntityType.File(),
				FileNFSProperties:  fileProperties,
				FileNFSPermissions: fileOrFolderPermissions,
			}}
		srcObjRes[name] = CreateResource[ObjectResourceManager](svm, srcContainer, obj)
		srcObjs[name] = obj
	}

	// create original file for linking symlink
	sOriginalFileName := rootDir + "/soriginal.txt"
	obj = ResourceDefinitionObject{
		ObjectName: pointerTo(sOriginalFileName),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.File(),
			FileNFSProperties:  fileProperties,
			FileNFSPermissions: fileOrFolderPermissions,
		}}
	srcObjRes[sOriginalFileName] = CreateResource[ObjectResourceManager](svm, srcContainer, obj)
	srcObjs[sOriginalFileName] = obj

	// create symlink file
	symLinkedFileName := rootDir + "/symlinked.txt"
	obj = ResourceDefinitionObject{
		ObjectName: pointerTo(symLinkedFileName),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Symlink(),
			FileNFSProperties:  fileProperties,
			FileNFSPermissions: fileOrFolderPermissions,
			SymlinkedFileName:  sOriginalFileName,
		}}
	// Symlink file should not be copied
	srcObjRes[symLinkedFileName] = CreateResource[ObjectResourceManager](svm, srcContainer, obj)

	// create original file for creating hardlinked file
	hOriginalFileName := rootDir + "/horiginal.txt"
	obj = ResourceDefinitionObject{
		ObjectName: pointerTo(hOriginalFileName),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.File(),
			FileNFSProperties:  fileProperties,
			FileNFSPermissions: fileOrFolderPermissions,
		}}
	srcObjRes[hOriginalFileName] = CreateResource[ObjectResourceManager](svm, srcContainer, obj)
	srcObjs[hOriginalFileName] = obj

	// create hardlinked file
	hardLinkedFileName := rootDir + "/hardlinked.txt"
	obj = ResourceDefinitionObject{
		ObjectName: pointerTo(hardLinkedFileName),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			FileNFSProperties:  fileProperties,
			FileNFSPermissions: fileOrFolderPermissions,
			HardLinkedFileName: hOriginalFileName,
		}}
	srcObjRes[hardLinkedFileName] = CreateResource[ObjectResourceManager](svm, srcContainer, obj)
	srcObjs[hardLinkedFileName] = obj

	// create special file
	specialFileFileName := rootDir + "/mypipe"
	obj = ResourceDefinitionObject{
		ObjectName: pointerTo(specialFileFileName),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Other(),
			FileNFSProperties:  fileProperties,
			FileNFSPermissions: fileOrFolderPermissions,
		}}
	srcObjRes[specialFileFileName] = CreateResource[ObjectResourceManager](svm, srcContainer, obj)

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	stdOut, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb:    azCopyVerb,
			Targets: []ResourceManager{srcDirObj, dst.(RemoteResourceManager).WithSpecificAuthType(ResolveVariation(svm, []ExplicitCredentialTypes{EExplicitCredentialType.SASToken(), EExplicitCredentialType.OAuth()}), svm, CreateAzCopyTargetOptions{})},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
					FromTo:    pointerTo(common.EFromTo.LocalFileNFS()),
					// --preserve-info flag will be true by default in case of linux
					PreserveInfo:        pointerTo(preserveProperties),
					PreservePermissions: pointerTo(preservePermissions),
				},
			},
		})

	// As we cannot set creationTime in linux we will fetch the properties from local and set it to src object properties
	for objName := range srcObjs {
		obj := srcObjs[objName]
		objProp := srcObjRes[objName].GetProperties(svm)
		if obj.ObjectProperties.FileNFSProperties != nil {
			obj.ObjectProperties.FileNFSProperties.FileCreationTime = objProp.FileProperties.FileCreationTime
		}
	}
	// Dont validate the root directory in case of sync
	if azCopyVerb == AzCopyVerbSync {
		delete(srcObjs, rootDir)
	}

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, true)
	ValidateSkippedSymLinkedCount(svm, stdOut, 1)
	ValidateHardlinkedSkippedCount(svm, stdOut, 2)
	ValidateSkippedSpecialFileCount(svm, stdOut, 1)
}

func (s *FilesNFSTestSuite) Scenario_AzureNFSToLocal(svm *ScenarioVariationManager) {

	//
	// 	Test Scenario:
	// 	1. Create a NFS enabled file share container in Azure
	// 	2. Create a folder with some files in it. Create a regular and hardlink files in the folder.
	// 	3. We cannot create symlink files in NFS enabled file share as of now.
	// 	4. Creating special file via NFS REST API is not allowed.
	// 	5. Run azcopy copy/sync command to copy the folder from Azure NFS enabled file share to local.
	// 	6. Hardlinked files should be downloaded as regular files. Hardlinks will not be preserved.
	// 	7. Number of hardlinks converted count will be displayed in job's summary
	//

	if runtime.GOOS == "windows" || runtime.GOOS == "darwin" {
		svm.InvalidateScenario()
		return
	}
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync}) // Calculate verb early to create the destination object early
	preserveProperties := ResolveVariation(svm, []bool{true, false})
	//TODO: Not checking for this flag as false as azcopy needs to run by root user
	// in order to set the owner and group to 0(root)
	preservePermissions := ResolveVariation(svm, []bool{true})

	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})

	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.FileNFS()}), GetResourceOptions{
		PreferredAccount: pointerTo(PremiumFileShareAcct),
	}), ResourceDefinitionContainer{
		Properties: ContainerProperties{
			FileContainerProperties: FileContainerProperties{
				EnabledProtocols: pointerTo("NFS"),
			},
		},
	})

	folderProperties, fileProperties, fileOrFolderPermissions := getPropertiesAndPermissions(svm, preserveProperties, preservePermissions)
	rootDir := "dir_file_copy_test_" + uuid.NewString()

	var dst ResourceManager
	if azCopyVerb == AzCopyVerbSync {
		dstObj := dstContainer.GetObject(svm, rootDir+"/test1.txt", common.EEntityType.File())
		dstObj.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{})
		if !svm.Dryrun() {
			// Make sure the LMT is in the past
			time.Sleep(time.Second * 5)
		}
		dst = dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	} else {
		dst = dstContainer
	}
	// Create source dataset
	srcObjs := make(ObjectResourceMappingFlat)

	obj := ResourceDefinitionObject{
		ObjectName: pointerTo(rootDir),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Folder(),
			FileNFSProperties:  folderProperties,
			FileNFSPermissions: fileOrFolderPermissions,
		},
	}
	CreateResource[ObjectResourceManager](svm, srcContainer, obj)
	srcObjs[rootDir] = obj

	for i := range 2 {
		name := rootDir + "/test" + strconv.Itoa(i) + ".txt"
		obj := ResourceDefinitionObject{
			ObjectName: pointerTo(name),
			Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
			ObjectProperties: ObjectProperties{
				EntityType:         common.EEntityType.File(),
				FileNFSProperties:  fileProperties,
				FileNFSPermissions: fileOrFolderPermissions,
			}}
		CreateResource[ObjectResourceManager](svm, srcContainer, obj)
		srcObjs[name] = obj
	}

	// create original file for linking symlink
	// Symlink creation in NFS is currently not supported in go-sdk.
	// TODO: Add this once the support is added
	// sOriginalFileName := rootDir + "/soriginal.txt"
	// obj = ResourceDefinitionObject{
	// 	ObjectName: pointerTo(sOriginalFileName),
	// 	ObjectProperties: ObjectProperties{
	// 		EntityType:         common.EEntityType.File(),
	// 		FileNFSProperties:  fileProperties,
	// 		FileNFSPermissions: fileOrFolderPermissions,
	// 	}}
	// CreateResource[ObjectResourceManager](svm, srcContainer, obj)
	// srcObjs[sOriginalFileName] = obj

	// // create symlink file
	// symLinkedFileName := rootDir + "/symlinked.txt"
	// obj = ResourceDefinitionObject{
	// 	ObjectName: pointerTo(symLinkedFileName),
	// 	ObjectProperties: ObjectProperties{
	// 		EntityType:         common.EEntityType.Symlink(),
	// 		FileNFSProperties:  fileProperties,
	// 		FileNFSPermissions: fileOrFolderPermissions,
	// 		SymlinkedFileName:  sOriginalFileName,
	// 	}}
	// // Symlink file should not be copied
	// CreateResource[ObjectResourceManager](svm, srcContainer, obj)

	// create original file for creating hardlinked file
	hOriginalFileName := rootDir + "/horiginal.txt"
	obj = ResourceDefinitionObject{
		ObjectName: pointerTo(hOriginalFileName),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.File(),
			FileNFSProperties:  fileProperties,
			FileNFSPermissions: fileOrFolderPermissions,
		}}
	CreateResource[ObjectResourceManager](svm, srcContainer, obj)
	srcObjs[hOriginalFileName] = obj

	// create hardlinked file
	hardLinkedFileName := rootDir + "/hardlinked.txt"
	obj = ResourceDefinitionObject{
		ObjectName: pointerTo(hardLinkedFileName),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			FileNFSProperties:  fileProperties,
			FileNFSPermissions: fileOrFolderPermissions,
			HardLinkedFileName: hOriginalFileName,
		}}
	CreateResource[ObjectResourceManager](svm, srcContainer, obj)
	srcObjs[hardLinkedFileName] = obj

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	stdOut, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb:    azCopyVerb,
			Targets: []ResourceManager{srcDirObj.(RemoteResourceManager).WithSpecificAuthType(ResolveVariation(svm, []ExplicitCredentialTypes{EExplicitCredentialType.SASToken(), EExplicitCredentialType.OAuth()}), svm, CreateAzCopyTargetOptions{}), dst},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive:           pointerTo(true),
					FromTo:              pointerTo(common.EFromTo.FileNFSLocal()),
					PreservePermissions: pointerTo(preservePermissions),
					PreserveInfo:        pointerTo(preserveProperties),
				},
			},
		})

	// Dont validate the root directory in case of sync
	if azCopyVerb == AzCopyVerbSync {
		delete(srcObjs, rootDir)
	}

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, true)
	ValidateHardlinkedSkippedCount(svm, stdOut, 2)
	// TODO: add this validation later when symlink is supported for NFS in go-sdk
	//ValidateSkippedSymLinkedCount(svm, stdOut, 1)

}

func (s *FilesNFSTestSuite) Scenario_AzureNFSToAzureNFS(svm *ScenarioVariationManager) {

	//
	// 	Test Scenario:
	// 	1. Create a NFS enabled file share container in Azure
	// 	2. Create a folder with some files in it. Create a regular and hardlink files in the folder.
	// 	3. We cannot create symlink files in NFS enabled file share as of now.
	// 	4. Creating special file via NFS REST API is not allowed.
	// 	5. Run azcopy copy/sync command to copy the folder from Azure NFS enabled file share to local.
	// 	6. Hardlinked files should be downloaded as regular files. Hardlinks will not be preserved.
	// 	7. Number of hardlinks converted count will be displayed in job's summary
	//

	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync}) // Calculate verb early to create the destination object early
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.FileNFS()}), GetResourceOptions{
		PreferredAccount: pointerTo(PremiumFileShareAcct),
	}), ResourceDefinitionContainer{
		Properties: ContainerProperties{
			FileContainerProperties: FileContainerProperties{
				EnabledProtocols: pointerTo("NFS"),
			},
		},
	})
	//defer deleteShare(svm, dstContainer)

	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.FileNFS()}), GetResourceOptions{
		PreferredAccount: pointerTo(PremiumFileShareAcct),
	}), ResourceDefinitionContainer{
		Properties: ContainerProperties{
			FileContainerProperties: FileContainerProperties{
				EnabledProtocols: pointerTo("NFS"),
			},
		},
	})

	preserveProperties := ResolveVariation(svm, []bool{true, false})
	preservePermissions := ResolveVariation(svm, []bool{true, false})

	var folderProperties, fileProperties *FileNFSProperties
	if preserveProperties {
		folderProperties = &FileNFSProperties{
			FileCreationTime: pointerTo(time.Now()),
		}
		fileProperties = &FileNFSProperties{
			FileCreationTime:  pointerTo(time.Now()),
			FileLastWriteTime: pointerTo(time.Now()),
		}
	}
	var fileOrFolderPermissions *FileNFSPermissions
	if preservePermissions {
		fileOrFolderPermissions = &FileNFSPermissions{
			Owner:    pointerTo("1000"),
			Group:    pointerTo("1000"),
			FileMode: pointerTo("0755"),
		}
	}

	rootDir := "dir_file_copy_test_" + uuid.NewString()

	var dst, src ResourceManager
	if azCopyVerb == AzCopyVerbSync {
		dstObj := dstContainer.GetObject(svm, rootDir+"/test1.txt", common.EEntityType.File())
		dstObj.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{
			FileNFSPermissions: fileOrFolderPermissions,
			FileNFSProperties:  fileProperties,
		})
		dstObj = dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
		dst = dstObj
		if !svm.Dryrun() {
			// Make sure the LMT is in the past
			time.Sleep(time.Second * 5)
		}
	} else {
		dst = dstContainer
	}
	src = srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	// Create destination directories
	srcObjs := make(ObjectResourceMappingFlat)

	obj := ResourceDefinitionObject{
		ObjectName: pointerTo(rootDir),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Folder(),
			FileNFSProperties:  folderProperties,
			FileNFSPermissions: fileOrFolderPermissions,
		},
	}
	CreateResource[ObjectResourceManager](svm, srcContainer, obj)
	srcObjs[rootDir] = obj

	for i := range 2 {
		name := rootDir + "/test" + strconv.Itoa(i) + ".txt"
		obj := ResourceDefinitionObject{
			ObjectName: pointerTo(name),
			Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
			ObjectProperties: ObjectProperties{
				EntityType:         common.EEntityType.File(),
				FileNFSProperties:  fileProperties,
				FileNFSPermissions: fileOrFolderPermissions,
			}}
		CreateResource[ObjectResourceManager](svm, srcContainer, obj)
		srcObjs[name] = obj
	}

	// create original file for creating hardlinked file
	hOriginalFileName := rootDir + "/horiginal.txt"
	obj = ResourceDefinitionObject{
		ObjectName: pointerTo(hOriginalFileName),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.File(),
			FileNFSProperties:  fileProperties,
			FileNFSPermissions: fileOrFolderPermissions,
		}}
	CreateResource[ObjectResourceManager](svm, srcContainer, obj)
	srcObjs[hOriginalFileName] = obj

	// create hardlinked file
	hardLinkedFileName := rootDir + "/hardlinked.txt"
	obj = ResourceDefinitionObject{
		ObjectName: pointerTo(hardLinkedFileName),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			FileNFSProperties:  fileProperties,
			FileNFSPermissions: fileOrFolderPermissions,
			HardLinkedFileName: hOriginalFileName,
		}}
	CreateResource[ObjectResourceManager](svm, srcContainer, obj)
	srcObjs[hardLinkedFileName] = obj

	stdOut, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: azCopyVerb,
			Targets: []ResourceManager{
				src.(RemoteResourceManager).WithSpecificAuthType(ResolveVariation(svm, []ExplicitCredentialTypes{EExplicitCredentialType.SASToken(), EExplicitCredentialType.OAuth()}), svm, CreateAzCopyTargetOptions{}),
				dst.(RemoteResourceManager).WithSpecificAuthType(ResolveVariation(svm, []ExplicitCredentialTypes{EExplicitCredentialType.SASToken(), EExplicitCredentialType.OAuth()}), svm, CreateAzCopyTargetOptions{}),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive:           pointerTo(true),
					FromTo:              pointerTo(common.EFromTo.FileNFSFileNFS()),
					PreservePermissions: pointerTo(preservePermissions),
					PreserveInfo:        pointerTo(preserveProperties),
				},
			},
		})

	// Dont validate the root directory in case of sync
	if azCopyVerb == AzCopyVerbSync {
		delete(srcObjs, rootDir)
	}
	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, false)

	ValidateHardlinkedSkippedCount(svm, stdOut, 2)
}

func (s *FilesNFSTestSuite) Scenario_TestInvalidScenariosForSMB(svm *ScenarioVariationManager) {

	// Test Scenarios
	// 1. If nfs flag is provided and if the source or destination is SMB its an unsupported scenario

	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync}) // Calculate verb early to create the destination object early
	preserveProperties := ResolveVariation(svm, []bool{true, false})
	preservePermissions := ResolveVariation(svm, []bool{true, false})

	desNFSShare := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.FileNFS()}), GetResourceOptions{
		PreferredAccount: ResolveVariation(svm, []*string{pointerTo(PremiumFileShareAcct)}),
	}), ResourceDefinitionContainer{
		Properties: ContainerProperties{
			FileContainerProperties: FileContainerProperties{
				EnabledProtocols: pointerTo("NFS"),
			},
		},
	}).GetObject(svm, "test", common.EEntityType.File())

	desSMBShare := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.FileSMB()}), GetResourceOptions{
		PreferredAccount: ResolveVariation(svm, []*string{pointerTo(PremiumFileShareAcct)}),
	}), ResourceDefinitionContainer{
		Properties: ContainerProperties{
			FileContainerProperties: FileContainerProperties{
				EnabledProtocols: pointerTo("SMB"),
			},
		},
	}).GetObject(svm, "test", common.EEntityType.File())

	dstObj := ResolveVariation(svm, []ObjectResourceManager{desNFSShare, desSMBShare})

	srcNFSShare := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.FileNFS()}), GetResourceOptions{
		PreferredAccount: ResolveVariation(svm, []*string{pointerTo(PremiumFileShareAcct)}),
	}), ResourceDefinitionContainer{
		Properties: ContainerProperties{
			FileContainerProperties: FileContainerProperties{
				EnabledProtocols: pointerTo("NFS"),
			},
		},
	}).GetObject(svm, "test", common.EEntityType.File())

	srcSMBShare := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.FileSMB()}), GetResourceOptions{
		PreferredAccount: ResolveVariation(svm, []*string{pointerTo(PremiumFileShareAcct)}),
	}), ResourceDefinitionContainer{
		Properties: ContainerProperties{
			FileContainerProperties: FileContainerProperties{
				EnabledProtocols: pointerTo("SMB"),
			},
		},
	}).GetObject(svm, "test", common.EEntityType.File())

	srcObj := ResolveVariation(svm, []ObjectResourceManager{srcNFSShare, srcSMBShare})

	// The object must exist already if we're syncing.
	if azCopyVerb == AzCopyVerbSync {
		dstObj.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{})

		if !svm.Dryrun() {
			// Make sure the LMT is in the past
			time.Sleep(time.Second * 10)
		}
	}

	var fromTo common.FromTo
	if srcObj.Location() == common.ELocation.FileNFS() && dstObj.Location() == common.ELocation.FileNFS() {
		fromTo = common.EFromTo.FileNFSFileNFS()
	} else if srcObj.Location() == common.ELocation.FileNFS() && dstObj.Location() == common.ELocation.FileSMB() {
		fromTo = common.EFromTo.FileNFSFileSMB()
	} else if srcObj.Location() == common.ELocation.FileSMB() && dstObj.Location() == common.ELocation.FileNFS() {
		fromTo = common.EFromTo.FileSMBFileNFS()
	}
	stdOut, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: azCopyVerb,
			Targets: []ResourceManager{
				srcObj.(RemoteResourceManager).WithSpecificAuthType(ResolveVariation(svm, []ExplicitCredentialTypes{EExplicitCredentialType.SASToken(), EExplicitCredentialType.OAuth()}), svm, CreateAzCopyTargetOptions{}),
				dstObj.(RemoteResourceManager).WithSpecificAuthType(ResolveVariation(svm, []ExplicitCredentialTypes{EExplicitCredentialType.SASToken(), EExplicitCredentialType.OAuth()}), svm, CreateAzCopyTargetOptions{}),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					FromTo:              pointerTo(fromTo),
					PreserveInfo:        pointerTo(preserveProperties),
					PreservePermissions: pointerTo(preservePermissions),
				},
			},
			ShouldFail: true,
		})
	if srcObj.Location() == common.ELocation.Local() && runtime.GOOS != "linux" {
		ValidateContainsError(svm, stdOut, []string{
			"This functionality is only available on Linux.",
		})
	} else if srcObj.Location() == common.ELocation.FileSMB() && dstObj.Location() == common.ELocation.FileNFS() {
		ValidateContainsError(svm, stdOut, []string{
			"Copy operations between SMB and NFS file shares are not supported yet.",
		})
	} else if dstObj.Location() == common.ELocation.FileSMB() && srcObj.Location() == common.ELocation.FileNFS() {
		if azCopyVerb == AzCopyVerbCopy {
			ValidateContainsError(svm, stdOut, []string{
				"Copy operations between SMB and NFS file shares are not supported yet",
			})
		} else {
			ValidateContainsError(svm, stdOut, []string{
				"Copy operations between SMB and NFS file shares are not supported yet",
			})
		}
	}
}

func (s *FilesNFSTestSuite) Scenario_TestInvalidScenariosForNFS(svm *ScenarioVariationManager) {

	//
	//Test Scenarios
	//1. If nfs flag is not provided and if the source or destination is NFS its an unsupported scenario
	//

	if runtime.GOOS == "darwin" {
		svm.InvalidateScenario()
		return
	}
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync}) // Calculate verb early to create the destination object early
	preserveProperties := ResolveVariation(svm, []bool{true, false})
	preservePermissions := ResolveVariation(svm, []bool{true, false})

	dstObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.FileNFS()}), GetResourceOptions{
		PreferredAccount: ResolveVariation(svm, []*string{pointerTo(PremiumFileShareAcct)}),
	}), ResourceDefinitionContainer{
		Properties: ContainerProperties{
			FileContainerProperties: FileContainerProperties{
				EnabledProtocols: pointerTo("NFS"),
			},
		},
	}).GetObject(svm, "test", common.EEntityType.File())

	srcObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Local(), common.ELocation.FileSMB()}),
		GetResourceOptions{
			PreferredAccount: ResolveVariation(svm, []*string{pointerTo(PremiumFileShareAcct)}),
		}), ResourceDefinitionContainer{
		Properties: ContainerProperties{
			FileContainerProperties: FileContainerProperties{
				EnabledProtocols: pointerTo("NFS"),
			},
		},
	}).GetObject(svm, "test", common.EEntityType.File())

	// The object must exist already if we're syncing.
	if azCopyVerb == AzCopyVerbSync {
		dstObj.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{})

		if !svm.Dryrun() {
			// Make sure the LMT is in the past
			time.Sleep(time.Second * 10)
		}
	}

	stdOut, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: azCopyVerb,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcObj, ResolveVariation(svm, []ExplicitCredentialTypes{EExplicitCredentialType.SASToken(), EExplicitCredentialType.OAuth()}), svm, CreateAzCopyTargetOptions{}),
				dstObj.(RemoteResourceManager).WithSpecificAuthType(ResolveVariation(svm, []ExplicitCredentialTypes{EExplicitCredentialType.SASToken(), EExplicitCredentialType.OAuth()}), svm, CreateAzCopyTargetOptions{}),
			},

			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					PreserveInfo:        pointerTo(preserveProperties),
					PreservePermissions: pointerTo(preservePermissions),
				},
			},
			ShouldFail: true,
		})
	if runtime.GOOS == "darwin" {
		if preservePermissions {
			ValidateContainsError(svm, stdOut, []string{
				"up/downloads is supported only in Windows and Linux",
			})
		}
	}
	if srcObj.Location() == common.ELocation.FileSMB() {
		ValidateContainsError(svm, stdOut, []string{
			"The source share has NFS protocol enabled. To copy from a NFS share, use the appropriate --from-to flag value",
		})
	} else if dstObj.Location() == common.ELocation.FileSMB() {
		ValidateContainsError(svm, stdOut, []string{
			"The destination share has NFS protocol enabled. To copy to a NFS share, use the appropriate --from-to flag value",
		})
	}
}

func (s *FilesNFSTestSuite) Scenario_DstShareDoesNotExists(svm *ScenarioVariationManager) {

	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync}) // Calculate verb early to create the destination object early
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.FileSMB()}), GetResourceOptions{}), ResourceDefinitionContainer{})

	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.FileSMB()}), GetResourceOptions{}), ResourceDefinitionContainer{})

	rootDir := "dir_file_copy_test_" + uuid.NewString()

	var dst, src ResourceManager
	if azCopyVerb == AzCopyVerbSync {
		dstObj := dstContainer.GetObject(svm, rootDir+"/test1.txt", common.EEntityType.File())
		dstObj.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{})
		dstObj = dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
		dst = dstObj
		if !svm.Dryrun() {
			// Make sure the LMT is in the past
			time.Sleep(time.Second * 5)
		}
	} else {
		dst = dstContainer
	}
	src = srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	// Create destination directories
	srcObjs := make(ObjectResourceMappingFlat)

	obj := ResourceDefinitionObject{
		ObjectName: pointerTo(rootDir),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.Folder(),
		},
	}
	CreateResource[ObjectResourceManager](svm, srcContainer, obj)
	srcObjs[rootDir] = obj

	for i := range 2 {
		name := rootDir + "/test" + strconv.Itoa(i) + ".txt"
		obj := ResourceDefinitionObject{
			ObjectName: pointerTo(name),
			Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
			ObjectProperties: ObjectProperties{
				EntityType: common.EEntityType.File(),
			}}
		CreateResource[ObjectResourceManager](svm, srcContainer, obj)
		srcObjs[name] = obj
	}

	RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: azCopyVerb,
			Targets: []ResourceManager{
				src.(RemoteResourceManager).WithSpecificAuthType(ResolveVariation(svm, []ExplicitCredentialTypes{EExplicitCredentialType.SASToken(), EExplicitCredentialType.OAuth()}), svm, CreateAzCopyTargetOptions{}),
				dst.(RemoteResourceManager).WithSpecificAuthType(ResolveVariation(svm, []ExplicitCredentialTypes{EExplicitCredentialType.SASToken(), EExplicitCredentialType.OAuth()}), svm, CreateAzCopyTargetOptions{}),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
				},
			},
		})

	// Dont validate the root directory in case of sync
	if azCopyVerb == AzCopyVerbSync {
		delete(srcObjs, rootDir)
	}
}
