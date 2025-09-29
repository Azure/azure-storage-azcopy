package e2etest

import (
	"os/user"
	"runtime"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/Azure/azure-storage-azcopy/v10/common"
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
	preserveSymlinks := ResolveVariation(svm, []bool{true, false})
	followSymlinks := ResolveVariation(svm, []bool{true, false})

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
		dstObj1 := dstContainer.GetObject(svm, rootDir+"/test1.txt", common.EEntityType.File())
		dstObj1.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{})

		dstObj2 := dstContainer.GetObject(svm, rootDir+"/symlinked2.txt", common.EEntityType.File())
		dstObj2.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{})

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
	srcObjRes[symLinkedFileName] = CreateResource[ObjectResourceManager](svm, srcContainer, obj)
	if preserveSymlinks {
		srcObjs[symLinkedFileName] = obj
	}

	// create symlink file
	symLinkedFileName2 := rootDir + "/symlinked2.txt"
	obj = ResourceDefinitionObject{
		ObjectName: pointerTo(symLinkedFileName2),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Symlink(),
			FileNFSProperties:  fileProperties,
			FileNFSPermissions: fileOrFolderPermissions,
			SymlinkedFileName:  sOriginalFileName,
		}}
	srcObjRes[symLinkedFileName2] = CreateResource[ObjectResourceManager](svm, srcContainer, obj)
	if preserveSymlinks {
		srcObjs[symLinkedFileName2] = obj
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

	shouldFail := false
	if followSymlinks && preserveSymlinks {
		shouldFail = true
	}

	stdOut, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: azCopyVerb,
			Targets: []ResourceManager{srcDirObj, dst.(RemoteResourceManager).WithSpecificAuthType(
				ResolveVariation(svm, []ExplicitCredentialTypes{
					EExplicitCredentialType.SASToken(),
					EExplicitCredentialType.OAuth(),
				}), svm, CreateAzCopyTargetOptions{}),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
					FromTo:    pointerTo(common.EFromTo.LocalFileNFS()),
					// --preserve-info flag will be true by default in case of linux
					PreserveInfo:        pointerTo(preserveProperties),
					PreservePermissions: pointerTo(preservePermissions),
					PreserveSymlinks:    pointerTo(preserveSymlinks),
					FollowSymlinks:      pointerTo(followSymlinks),
				},
			},
			ShouldFail: shouldFail,
		})

	if followSymlinks && preserveSymlinks {
		ValidateMessageOutput(svm, stdOut, "cannot both follow and preserve symlinks", true)
		return
	}

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

	if !preserveSymlinks && !followSymlinks {
		ValidateSkippedSymlinksCount(svm, stdOut, 2)
	}
	ValidateHardlinksConvertedCount(svm, stdOut, 2)
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
			Verb: azCopyVerb,
			Targets: []ResourceManager{srcDirObj.(RemoteResourceManager).WithSpecificAuthType(
				ResolveVariation(svm, []ExplicitCredentialTypes{EExplicitCredentialType.SASToken(),
					EExplicitCredentialType.OAuth()}), svm, CreateAzCopyTargetOptions{}),
				dst},
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
	ValidateHardlinksConvertedCount(svm, stdOut, 2)
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

	ValidateHardlinksConvertedCount(svm, stdOut, 2)
}

func (s *FilesNFSTestSuite) Scenario_AzureNFSToAzureSMB(svm *ScenarioVariationManager) {

	//
	// 	Test Scenario:
	// 	1. Create a NFS enabled file share container in Azure
	// 	2. Create a folder with some files in it.
	// 	5. Run azcopy copy/sync command to copy the folder from Azure NFS enabled file share to local.
	//

	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync})
	// NFS and SMB Share
	dstShare := CreateResource[ContainerResourceManager](svm, GetRootResource(svm,
		ResolveVariation(svm, []common.Location{common.ELocation.File()}),
		GetResourceOptions{
			PreferredAccount: pointerTo(PremiumFileShareAcct),
		}), ResourceDefinitionContainer{
		Properties: ContainerProperties{
			FileContainerProperties: FileContainerProperties{
				EnabledProtocols: pointerTo("SMB"),
			},
		},
	})

	srcShare := CreateResource[ContainerResourceManager](svm, GetRootResource(svm,
		ResolveVariation(svm, []common.Location{common.ELocation.FileNFS()}),
		GetResourceOptions{
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

	rootDir := "dir_file_copy_test_" + uuid.NewString()

	var dst, src ResourceManager
	if azCopyVerb == AzCopyVerbSync {
		dstObj := dstShare.GetObject(svm, rootDir+"/test1.txt", common.EEntityType.File())
		dstObj.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{})
		dstObj = dstShare.GetObject(svm, rootDir, common.EEntityType.Folder())
		dst = dstObj
		if !svm.Dryrun() {
			// Make sure the LMT is in the past
			time.Sleep(time.Second * 5)
		}
	} else {
		dst = dstShare
	}
	src = srcShare.GetObject(svm, rootDir, common.EEntityType.Folder())

	// Create destination directories
	srcObjs := make(ObjectResourceMappingFlat)

	obj := ResourceDefinitionObject{
		ObjectName: pointerTo(rootDir),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.Folder(),
		},
	}

	// based on SMB or NFS set the properties
	if preserveProperties {
		if srcShare.Location() == common.ELocation.FileNFS() {
			//obj.ObjectProperties.FileNFSPermissions = fileOrFolderPermissions
			obj.ObjectProperties.FileNFSProperties = folderProperties
		} else {
			obj.FileProperties.FileCreationTime = folderProperties.FileCreationTime
			obj.FileProperties.LastModifiedTime = folderProperties.FileLastWriteTime
		}
	}
	CreateResource[ObjectResourceManager](svm, srcShare, obj)
	srcObjs[rootDir] = obj

	for i := range 2 {
		name := rootDir + "/test" + strconv.Itoa(i) + ".txt"
		obj := ResourceDefinitionObject{
			ObjectName: pointerTo(name),
			Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
			ObjectProperties: ObjectProperties{
				EntityType: common.EEntityType.File(),
			}}
		if preserveProperties {
			if srcShare.Location() == common.ELocation.FileNFS() {
				obj.ObjectProperties.FileNFSProperties = fileProperties
			} else {
				obj.FileProperties.FileCreationTime = fileProperties.FileCreationTime
				obj.FileProperties.LastModifiedTime = fileProperties.FileLastWriteTime
			}
		}

		CreateResource[ObjectResourceManager](svm, srcShare, obj)
		srcObjs[name] = obj
	}

	var fromTo common.FromTo
	if srcShare.Location() == common.ELocation.FileNFS() && dstShare.Location() == common.ELocation.File() {
		fromTo = common.EFromTo.FileNFSFileSMB()
	} else if srcShare.Location() == common.ELocation.File() && dstShare.Location() == common.ELocation.FileNFS() {
		fromTo = common.EFromTo.FileSMBFileNFS()
	}

	// we are already testing this scenarios in other tests so skipping the same location scenario here
	if srcShare.Location() == dstShare.Location() {
		svm.InvalidateScenario()
		return
	}

	hardlinkHandling := ResolveVariation(svm, []common.HardlinkHandlingType{common.EHardlinkHandlingType.Skip(), common.EHardlinkHandlingType.Follow()})
	shouldFail := true
	if hardlinkHandling != common.EHardlinkHandlingType.Follow() && !preservePermissions {
		shouldFail = false
	}

	stdOut, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: azCopyVerb,
			Targets: []ResourceManager{
				src.(RemoteResourceManager).WithSpecificAuthType(ResolveVariation(svm, []ExplicitCredentialTypes{EExplicitCredentialType.SASToken()}), svm, CreateAzCopyTargetOptions{}),
				dst.(RemoteResourceManager).WithSpecificAuthType(ResolveVariation(svm, []ExplicitCredentialTypes{EExplicitCredentialType.SASToken()}), svm, CreateAzCopyTargetOptions{}),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive:           pointerTo(true),
					FromTo:              pointerTo(fromTo),
					PreservePermissions: pointerTo(preservePermissions),
					PreserveInfo:        pointerTo(preserveProperties),
					HardlinkType:        pointerTo(hardlinkHandling),
				},
			},
			ShouldFail: shouldFail,
		})

	if preservePermissions {
		ValidateContainsError(svm, stdOut, []string{
			"--preserve-permissions flag is not supported for cross-protocol transfers",
		})
		return
	}

	if hardlinkHandling == common.EHardlinkHandlingType.Follow() {
		ValidateContainsError(svm, stdOut, []string{
			"Hardlinked files are not supported between NFS and SMB and will always be skipped. Please re-run with '--hardlinks=skip'.",
		})
		return
	}

	// Dont validate the root directory in case of sync
	if azCopyVerb == AzCopyVerbSync {
		delete(srcObjs, rootDir)
	}
}

func (s *FilesNFSTestSuite) Scenario_AzureSMBToAzureNFS(svm *ScenarioVariationManager) {

	//
	// 	Test Scenario:
	// 	1. Create a NFS enabled file share container in Azure
	// 	2. Create a folder with some files in it.
	// 	5. Run azcopy copy/sync command to copy the folder from Azure NFS enabled file share to local.
	//

	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync})
	// NFS and SMB Share
	dstShare := CreateResource[ContainerResourceManager](svm, GetRootResource(svm,
		ResolveVariation(svm, []common.Location{common.ELocation.FileNFS()}),
		GetResourceOptions{
			PreferredAccount: pointerTo(PremiumFileShareAcct),
		}), ResourceDefinitionContainer{
		Properties: ContainerProperties{
			FileContainerProperties: FileContainerProperties{
				EnabledProtocols: pointerTo("NFS"),
			},
		},
	})

	srcShare := CreateResource[ContainerResourceManager](svm, GetRootResource(svm,
		ResolveVariation(svm, []common.Location{common.ELocation.File()}),
		GetResourceOptions{
			PreferredAccount: pointerTo(PremiumFileShareAcct),
		}), ResourceDefinitionContainer{
		Properties: ContainerProperties{
			FileContainerProperties: FileContainerProperties{
				EnabledProtocols: pointerTo("SMB"),
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

	rootDir := "dir_file_copy_test_" + uuid.NewString()

	var dst, src ResourceManager
	if azCopyVerb == AzCopyVerbSync {
		dstObj := dstShare.GetObject(svm, rootDir+"/test1.txt", common.EEntityType.File())
		dstObj.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{})
		dstObj = dstShare.GetObject(svm, rootDir, common.EEntityType.Folder())
		dst = dstObj
		if !svm.Dryrun() {
			// Make sure the LMT is in the past
			time.Sleep(time.Second * 5)
		}
	} else {
		dst = dstShare
	}
	src = srcShare.GetObject(svm, rootDir, common.EEntityType.Folder())

	// Create destination directories
	srcObjs := make(ObjectResourceMappingFlat)

	obj := ResourceDefinitionObject{
		ObjectName: pointerTo(rootDir),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.Folder(),
		},
	}

	// based on SMB or NFS set the properties
	if preserveProperties {
		if srcShare.Location() == common.ELocation.FileNFS() {
			//obj.ObjectProperties.FileNFSPermissions = fileOrFolderPermissions
			obj.ObjectProperties.FileNFSProperties = folderProperties
		} else {
			obj.FileProperties.FileCreationTime = folderProperties.FileCreationTime
			obj.FileProperties.LastModifiedTime = folderProperties.FileLastWriteTime
		}
	}
	CreateResource[ObjectResourceManager](svm, srcShare, obj)
	srcObjs[rootDir] = obj

	for i := range 2 {
		name := rootDir + "/test" + strconv.Itoa(i) + ".txt"
		obj := ResourceDefinitionObject{
			ObjectName: pointerTo(name),
			Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
			ObjectProperties: ObjectProperties{
				EntityType: common.EEntityType.File(),
			}}
		if preserveProperties {
			if srcShare.Location() == common.ELocation.FileNFS() {
				obj.ObjectProperties.FileNFSProperties = fileProperties
			} else {
				obj.FileProperties.FileCreationTime = fileProperties.FileCreationTime
				obj.FileProperties.LastModifiedTime = fileProperties.FileLastWriteTime
			}
		}

		CreateResource[ObjectResourceManager](svm, srcShare, obj)
		srcObjs[name] = obj
	}

	var fromTo common.FromTo
	if srcShare.Location() == common.ELocation.FileNFS() && dstShare.Location() == common.ELocation.File() {
		fromTo = common.EFromTo.FileNFSFileSMB()
	} else if srcShare.Location() == common.ELocation.File() && dstShare.Location() == common.ELocation.FileNFS() {
		fromTo = common.EFromTo.FileSMBFileNFS()
	}

	// we are already testing this scenarios in other tests so skipping the same location scenario here
	if srcShare.Location() == dstShare.Location() {
		svm.InvalidateScenario()
		return
	}

	hardlinkHandling := ResolveVariation(svm, []common.HardlinkHandlingType{common.EHardlinkHandlingType.Skip(), common.EHardlinkHandlingType.Follow()})
	shouldFail := true
	if hardlinkHandling != common.EHardlinkHandlingType.Follow() && !preservePermissions {
		shouldFail = false
	}

	stdOut, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: azCopyVerb,
			Targets: []ResourceManager{
				src.(RemoteResourceManager).WithSpecificAuthType(ResolveVariation(svm, []ExplicitCredentialTypes{EExplicitCredentialType.SASToken()}), svm, CreateAzCopyTargetOptions{}),
				dst.(RemoteResourceManager).WithSpecificAuthType(ResolveVariation(svm, []ExplicitCredentialTypes{EExplicitCredentialType.SASToken()}), svm, CreateAzCopyTargetOptions{}),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive:           pointerTo(true),
					FromTo:              pointerTo(fromTo),
					PreservePermissions: pointerTo(preservePermissions),
					PreserveInfo:        pointerTo(preserveProperties),
					HardlinkType:        pointerTo(hardlinkHandling),
				},
			},
			ShouldFail: shouldFail,
		})

	if preservePermissions {
		ValidateContainsError(svm, stdOut, []string{
			"--preserve-permissions flag is not supported for cross-protocol transfers",
		})
		return
	}

	if hardlinkHandling == common.EHardlinkHandlingType.Follow() {
		ValidateContainsError(svm, stdOut, []string{
			"Hardlinked files are not supported between NFS and SMB and will always be skipped. Please re-run with '--hardlinks=skip'.",
		})
		return
	}

	// Dont validate the root directory in case of sync
	if azCopyVerb == AzCopyVerbSync {
		delete(srcObjs, rootDir)
	}
}

func (s *FilesNFSTestSuite) Scenario_TestInvalidScenariosForNFS(svm *ScenarioVariationManager) {

	//
	//Test Scenarios
	//1. If --from-to flag is not provided and if the source or destination is NFS its an unsupported scenario
	//

	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync}) // Calculate verb early to create the destination object early

	dstObj1 := CreateResource[ContainerResourceManager](svm, GetRootResource(svm,
		ResolveVariation(svm, []common.Location{common.ELocation.FileNFS()}), GetResourceOptions{
			PreferredAccount: ResolveVariation(svm, []*string{pointerTo(PremiumFileShareAcct)}),
		}), ResourceDefinitionContainer{
		Properties: ContainerProperties{
			FileContainerProperties: FileContainerProperties{
				EnabledProtocols: pointerTo("NFS"),
			},
		},
	})

	dstObj2 := CreateResource[ContainerResourceManager](svm, GetRootResource(svm,
		ResolveVariation(svm, []common.Location{common.ELocation.File()}), GetResourceOptions{
			PreferredAccount: ResolveVariation(svm, []*string{pointerTo(PremiumFileShareAcct)}),
		}), ResourceDefinitionContainer{})

	dstShare := ResolveVariation(svm, []ContainerResourceManager{dstObj1, dstObj2})

	srcObj1 := CreateResource[ContainerResourceManager](svm, GetRootResource(svm,
		ResolveVariation(svm, []common.Location{common.ELocation.File()}),
		GetResourceOptions{
			PreferredAccount: ResolveVariation(svm, []*string{pointerTo(PremiumFileShareAcct)}),
		}), ResourceDefinitionContainer{})

	srcObj2 := CreateResource[ContainerResourceManager](svm, GetRootResource(svm,
		ResolveVariation(svm, []common.Location{common.ELocation.FileNFS()}),
		GetResourceOptions{
			PreferredAccount: ResolveVariation(svm, []*string{pointerTo(PremiumFileShareAcct)}),
		}), ResourceDefinitionContainer{
		Properties: ContainerProperties{
			FileContainerProperties: FileContainerProperties{
				EnabledProtocols: pointerTo("NFS"),
			},
		},
	})

	srcShare := ResolveVariation(svm, []ContainerResourceManager{srcObj1, srcObj2})

	rootDir := "dir_file_copy_test_" + uuid.NewString()
	var dst, src ResourceManager

	// The object must exist already if we're syncing.
	if azCopyVerb == AzCopyVerbSync {
		dstObj := dstShare.GetObject(svm, rootDir+"/test1.txt", common.EEntityType.File())
		dstObj.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{})
		dstObj = dstShare.GetObject(svm, rootDir, common.EEntityType.Folder())
		dst = dstObj
		if !svm.Dryrun() {
			// Make sure the LMT is in the past
			time.Sleep(time.Second * 5)
		}
	} else {
		dst = dstShare
	}
	src = srcShare.GetObject(svm, rootDir, common.EEntityType.Folder())

	// Create destination directories
	srcObjs := make(ObjectResourceMappingFlat)

	obj := ResourceDefinitionObject{
		ObjectName: pointerTo(rootDir),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.Folder(),
		},
	}
	CreateResource[ObjectResourceManager](svm, srcShare, obj)
	srcObjs[rootDir] = obj

	for i := range 2 {
		name := rootDir + "/test" + strconv.Itoa(i) + ".txt"
		obj := ResourceDefinitionObject{
			ObjectName: pointerTo(name),
			Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
			ObjectProperties: ObjectProperties{
				EntityType: common.EEntityType.File(),
			}}
		CreateResource[ObjectResourceManager](svm, srcShare, obj)
		srcObjs[name] = obj
	}

	if srcShare.Location() == dstShare.Location() {
		svm.InvalidateScenario()
		return
	}

	_, _ = RunAzCopy(
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
			ShouldFail: true,
		})
}

func (s *FilesNFSTestSuite) Scenario_DstShareDoesNotExists(svm *ScenarioVariationManager) {

	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync}) // Calculate verb early to create the destination object early
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.File()}), GetResourceOptions{}), ResourceDefinitionContainer{})

	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.File()}), GetResourceOptions{}), ResourceDefinitionContainer{})

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
