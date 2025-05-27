package e2etest

import (
	"fmt"
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

	//TODO: Remove it. For testing purpose
	//dstContainer := GetAccount(svm, PremiumFileShareAcct).GetService(svm, common.ELocation.File()).GetContainer("aznfs3")
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.File()}), GetResourceOptions{
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

	sasOpts := GenericAccountSignatureValues{}

	stdOut, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: azCopyVerb,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcDirObj, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
				TryApplySpecificAuthType(dst, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
					NFS:       pointerTo(true),
					// --preserve-info flag will be true by default in case of linux
					PreserveInfo:        pointerTo(preserveProperties),
					PreservePermissions: pointerTo(preservePermissions),
				},
			},
		})
	//As we cannot set creationTime in linux we will fetch the properties from local and set it to src object properties
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
	fmt.Println("StdOut: ", stdOut)
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
	//srcContainer := GetAccount(svm, PremiumFileShareAcct).GetService(svm, common.ELocation.File()).GetContainer("aznfs3")

	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.File()}), GetResourceOptions{
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

	sasOpts := GenericAccountSignatureValues{}
	stdOut, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: azCopyVerb,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcDirObj, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
				dst,
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive:           pointerTo(true),
					NFS:                 pointerTo(true),
					PreservePermissions: pointerTo(preservePermissions),
					PreserveInfo:        pointerTo(preserveProperties),
				},
			},
		})

	// Dont validate the root directory in case of sync
	if azCopyVerb == AzCopyVerbSync {
		delete(srcObjs, rootDir)
	}
	fmt.Println("StdOut: ", stdOut)
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

	// if runtime.GOOS == "windows" || runtime.GOOS == "darwin" {
	// 	svm.InvalidateScenario()
	// 	return
	// }

	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync}) // Calculate verb early to create the destination object early
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.File()}), GetResourceOptions{
		PreferredAccount: pointerTo(PremiumFileShareAcct),
	}), ResourceDefinitionContainer{
		Properties: ContainerProperties{
			FileContainerProperties: FileContainerProperties{
				EnabledProtocols: pointerTo("NFS"),
			},
		},
	})

	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.File()}), GetResourceOptions{
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

	folderProperties, fileProperties, fileOrFolderPermissions := getPropertiesAndPermissions(svm, preserveProperties, preservePermissions)
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

	sasOpts := GenericAccountSignatureValues{}

	stdOut, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: azCopyVerb,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(src, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
				TryApplySpecificAuthType(dst, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive:           pointerTo(true),
					NFS:                 pointerTo(true),
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

	fmt.Println("StdOut: ", stdOut)
	ValidateHardlinkedSkippedCount(svm, stdOut, 2)
}
