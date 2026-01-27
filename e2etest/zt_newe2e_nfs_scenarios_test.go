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

	if runtime.GOOS == "linux" {
		// Get the current user information
		currentUser, err := user.Current()
		a.NoError("Error retrieving current user:", err)
		uid = currentUser.Uid
		gid = currentUser.Gid
	} else { // for windows and mac
		uid = "1000"
		gid = "1000"
	}

	return
}

func getPropertiesAndPermissions(svm *ScenarioVariationManager, preserveProperties, preservePermissions bool) (*FileNFSProperties, *FileNFSProperties, *FileNFSPermissions) {
	uid, gid := GetCurrentUIDAndGID(svm)

	var folderProperties, fileProperties *FileNFSProperties
	if preserveProperties {
		folderProperties = &FileNFSProperties{
			FileCreationTime: pointerTo(time.Now().Add(-1 * time.Minute)),
		}
		fileProperties = &FileNFSProperties{
			FileCreationTime:  pointerTo(time.Now().Add(-1 * time.Minute)),
			FileLastWriteTime: pointerTo(time.Now().Add(-1 * time.Minute)),
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

// These tests are using the same source and desination shares for testing to avoid
// creating too many share accounts which may lead to throttling by Azure.
// So in order to avoid conflicts between tests, we cleanup the test directories created during the test run.
func CleanupNFSDirectory(
	svm *ScenarioVariationManager,
	container ContainerResourceManager,
	rootDir string,
) {
	if svm.Dryrun() {
		return
	}
	// 1. List all objects under rootDir
	objs := container.ListObjects(svm, rootDir+"/", true)

	// 2. Delete files, symlinks, hardlinks, special files first
	for objName, objProp := range objs {
		if objProp.EntityType != common.EEntityType.Folder() {
			container.
				GetObject(svm, objName, objProp.EntityType).
				Delete(svm)
		}
	}

	// 4. Finally delete root directory
	container.GetObject(svm, rootDir, common.EEntityType.Folder()).Delete(svm)
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

	preserveSymlinks := NamedResolveVariation(svm, map[string]bool{
		"|preserveSymlinks=true":  true,
		"|preserveSymlinks=false": false,
	})

	followSymlinks := NamedResolveVariation(svm, map[string]bool{
		"|followSymlinks=true":  true,
		"|followSymlinks=false": false,
	})

	preserveProperties := NamedResolveVariation(svm, map[string]bool{
		"|preserveInfo=true":  true,
		"|preserveInfo=false": false,
	})

	preservePermissions := NamedResolveVariation(svm, map[string]bool{
		"|preservePermissions=true":  true,
		"|preservePermissions=false": false,
	})

	hardlinkType := NamedResolveVariation(svm, map[string]common.HardlinkHandlingType{
		"|hardlinks=follow": common.DefaultHardlinkHandlingType,
		"|hardlinks=skip":   common.SkipHardlinkHandlingType,
	})

	dstContainer := GetRootResource(svm, common.ELocation.FileNFS(), GetResourceOptions{
		PreferredAccount: pointerTo(PremiumFileShareAcct),
	}).(ServiceResourceManager).GetContainer("destnfs")

	if !dstContainer.Exists() {
		dstContainer.Create(svm, ContainerProperties{
			FileContainerProperties: FileContainerProperties{
				EnabledProtocols: pointerTo("NFS"),
			},
		})
	}

	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(
		svm, common.ELocation.Local()), ResourceDefinitionContainer{})

	rootDir := "dir_file_copy_test_" + uuid.NewString()

	var dst ResourceManager
	if azCopyVerb == AzCopyVerbSync {
		dstObj1 := dstContainer.GetObject(svm, rootDir+"/test1.txt", common.EEntityType.File())
		dstObj1.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{})
		var props ObjectProperties
		if preserveProperties {
			props = ObjectProperties{
				FileNFSProperties: &FileNFSProperties{
					FileCreationTime:  pointerTo(time.Now().Add(-10 * time.Minute)),
					FileLastWriteTime: pointerTo(time.Now().Add(-10 * time.Minute)),
				},
			}
		}
		dstObj1.SetObjectProperties(svm, props)

		dstObj2 := dstContainer.GetObject(svm, rootDir+"/symlinked2.txt", common.EEntityType.File())
		dstObj2.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{})
		dstObj2.SetObjectProperties(svm, props)

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
					Recursive:           pointerTo(true),
					FromTo:              pointerTo(common.EFromTo.LocalFileNFS()),
					PreserveInfo:        pointerTo(preserveProperties),
					PreservePermissions: pointerTo(preservePermissions),
					PreserveSymlinks:    pointerTo(preserveSymlinks),
					FollowSymlinks:      pointerTo(followSymlinks),
					HardlinkType:        pointerTo(hardlinkType),
				},
			},
			ShouldFail: shouldFail,
		})

	if followSymlinks && preserveSymlinks {
		ValidateMessageOutput(svm, stdOut, "cannot both follow and preserve symlinks", true)
		return
	}

	// As we cannot set creationTime in linux we will fetch the properties from local and set it to src object properties
	var hardlinkFileDeleteList []string
	for objName := range srcObjs {
		obj := srcObjs[objName]
		objProp := srcObjRes[objName].GetProperties(svm)
		if obj.ObjectProperties.FileNFSProperties != nil {
			obj.ObjectProperties.FileNFSProperties.FileCreationTime = objProp.FileProperties.FileCreationTime
		}
		if obj.EntityType == common.EEntityType.Hardlink() {
			if hardlinkType == common.SkipHardlinkHandlingType {
				hardlinkFileDeleteList = append(hardlinkFileDeleteList, objName)
				hardlinkFileDeleteList = append(hardlinkFileDeleteList, obj.HardLinkedFileName)
			}
		}
	}

	if hardlinkType == common.SkipHardlinkHandlingType {
		for _, objName := range hardlinkFileDeleteList {
			delete(srcObjs, objName)
		}
	}

	// Dont validate the root directory in case of sync
	if azCopyVerb == AzCopyVerbSync {
		delete(srcObjs, rootDir)
	}

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, ValidateResourceOptions{
		validateObjectContent: true,
		fromTo:                common.EFromTo.LocalFileNFS(),
	})
	defer CleanupNFSDirectory(svm, dstContainer, rootDir)

	if !preserveSymlinks && !followSymlinks {
		ValidateSkippedSymlinksCount(svm, stdOut, 2)
	}
	if hardlinkType == common.SkipHardlinkHandlingType {
		ValidateHardlinksSkippedCount(svm, stdOut, 2)
	} else {
		ValidateHardlinksConvertedCount(svm, stdOut, 2)
	}
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
	preserveSymlinks := NamedResolveVariation(svm, map[string]bool{
		"|preserveSymlinks=true":  true,
		"|preserveSymlinks=false": false,
	})

	followSymlinks := NamedResolveVariation(svm, map[string]bool{
		"|followSymlinks=true":  true,
		"|followSymlinks=false": false,
	})

	preserveProperties := NamedResolveVariation(svm, map[string]bool{
		"|preserveInfo=true":  true,
		"|preserveInfo=false": false,
	})

	hardlinkType := NamedResolveVariation(svm, map[string]common.HardlinkHandlingType{
		"|hardlinks=follow": common.DefaultHardlinkHandlingType,
		"|hardlinks=skip":   common.SkipHardlinkHandlingType,
	})

	//TODO: Not checking for this flag as false as azcopy needs to run by root user
	// in order to set the owner and group to 0(root)
	preservePermissions := NamedResolveVariation(svm, map[string]bool{
		"|preservePermissions=true": true,
	})

	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})

	srcContainer := GetRootResource(svm, common.ELocation.FileNFS(), GetResourceOptions{
		PreferredAccount: pointerTo(PremiumFileShareAcct),
	}).(ServiceResourceManager).GetContainer("srcnfs")

	if !srcContainer.Exists() {
		srcContainer.Create(svm, ContainerProperties{
			FileContainerProperties: FileContainerProperties{
				EnabledProtocols: pointerTo("NFS"),
			},
		})
	}

	folderProperties, fileProperties, fileOrFolderPermissions := getPropertiesAndPermissions(svm, preserveProperties, preservePermissions)
	rootDir := "dir_file_copy_test_" + uuid.NewString()
	defer CleanupNFSDirectory(svm, srcContainer, rootDir)

	var dst ResourceManager
	if azCopyVerb == AzCopyVerbSync {
		dstObj := dstContainer.GetObject(svm, rootDir+"/test1.txt", common.EEntityType.File())
		dstObj.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{
			FileNFSPermissions: fileOrFolderPermissions,
			FileNFSProperties: &FileNFSProperties{
				FileCreationTime:  pointerTo(time.Now().Add(-10 * time.Minute)),
				FileLastWriteTime: pointerTo(time.Now().Add(-10 * time.Minute)),
			},
		})
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
	sOriginalFileName := rootDir + "/soriginal.txt"
	obj = ResourceDefinitionObject{
		ObjectName: pointerTo(sOriginalFileName),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.File(),
			FileNFSProperties:  fileProperties,
			FileNFSPermissions: fileOrFolderPermissions,
		}}
	CreateResource[ObjectResourceManager](svm, srcContainer, obj)
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
	CreateResource[ObjectResourceManager](svm, srcContainer, obj)
	if preserveSymlinks {
		srcObjs[symLinkedFileName] = obj
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
	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	shouldFail := false
	if (followSymlinks && preserveSymlinks) || followSymlinks {
		shouldFail = true
	}

	stdOut, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: azCopyVerb,
			Targets: []ResourceManager{srcDirObj.(RemoteResourceManager).WithSpecificAuthType(
				ResolveVariation(svm, []ExplicitCredentialTypes{
					EExplicitCredentialType.SASToken(),
					EExplicitCredentialType.OAuth(),
				}), svm, CreateAzCopyTargetOptions{}), dst},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive:           pointerTo(true),
					FromTo:              pointerTo(common.EFromTo.FileNFSLocal()),
					PreservePermissions: pointerTo(preservePermissions),
					PreserveInfo:        pointerTo(preserveProperties),
					PreserveSymlinks:    pointerTo(preserveSymlinks),
					FollowSymlinks:      pointerTo(followSymlinks),
					HardlinkType:        pointerTo(hardlinkType),
				},
			},
			ShouldFail: shouldFail,
		})

	if followSymlinks && preserveSymlinks {
		ValidateContainsError(svm, stdOut, []string{
			"--preserve-symlinks and --follow-symlinks contradict",
		})
		return
	}

	if followSymlinks {
		ValidateContainsError(svm, stdOut, []string{
			"The '--follow-symlink' flag is only applicable when uploading from local filesystem.",
		})
		return
	}

	// Dont validate the root directory in case of sync
	if azCopyVerb == AzCopyVerbSync {
		delete(srcObjs, rootDir)
	}

	var hardlinkFileDeleteList []string
	for objName := range srcObjs {
		obj := srcObjs[objName]
		if obj.EntityType == common.EEntityType.Hardlink() {
			if hardlinkType == common.SkipHardlinkHandlingType {
				hardlinkFileDeleteList = append(hardlinkFileDeleteList, objName)
				hardlinkFileDeleteList = append(hardlinkFileDeleteList, obj.HardLinkedFileName)
			}
		}
	}

	if hardlinkType == common.SkipHardlinkHandlingType {
		for _, objName := range hardlinkFileDeleteList {
			delete(srcObjs, objName)
		}
	}

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, ValidateResourceOptions{
		validateObjectContent: true,
		fromTo:                common.EFromTo.FileNFSLocal(),
	})
	if hardlinkType == common.SkipHardlinkHandlingType {
		ValidateHardlinksSkippedCount(svm, stdOut, 2)
	} else {
		ValidateHardlinksConvertedCount(svm, stdOut, 2)
	}

	if !followSymlinks && !preserveSymlinks {
		ValidateSkippedSymlinksCount(svm, stdOut, 1)
	}
}

func (s *FilesNFSTestSuite) Scenario_AzureNFSToAzureNFS(svm *ScenarioVariationManager) {

	//
	// 	Test Scenario:
	// 	1. Create a NFS enabled file share container in Azure
	// 	2. Create a folder with some files in it. Create a regular and hardlink files in the folder.
	// 	4. Creating special file via NFS REST API is not allowed.
	// 	5. Run azcopy copy/sync command to copy the folder from Azure NFS enabled file share to local.
	// 	6. Hardlinked files should be transferred as regular files. Hardlinks will not be preserved.
	// 	7. Number of hardlinks converted count will be displayed in job's summary
	// 	8. Symlinked files should be copied as symlink files if --preserve-symlinks flag is set.
	//  9. If --follow-symlinks flag is set, then copy should fail as this flag is not supported in NFS<->NFS copy.

	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync}) // Calculate verb early to create the destination object early
	preserveSymlinks := NamedResolveVariation(svm, map[string]bool{
		"|preserveSymlinks=true":  true,
		"|preserveSymlinks=false": false,
	})

	followSymlinks := NamedResolveVariation(svm, map[string]bool{
		"|followSymlinks=true":  true,
		"|followSymlinks=false": false,
	})

	preserveProperties := NamedResolveVariation(svm, map[string]bool{
		"|preserveInfo=true":  true,
		"|preserveInfo=false": false,
	})

	preservePermissions := NamedResolveVariation(svm, map[string]bool{
		"|preservePermissions=true":  true,
		"|preservePermissions=false": false,
	})

	hardlinkType := NamedResolveVariation(svm, map[string]common.HardlinkHandlingType{
		"|hardlinks=follow": common.DefaultHardlinkHandlingType,
		"|hardlinks=skip":   common.SkipHardlinkHandlingType,
	})

	dstContainer := GetRootResource(svm, common.ELocation.FileNFS(), GetResourceOptions{
		PreferredAccount: pointerTo(PremiumFileShareAcct),
	}).(ServiceResourceManager).GetContainer("dstnfs")
	if !dstContainer.Exists() {
		dstContainer.Create(svm, ContainerProperties{
			FileContainerProperties: FileContainerProperties{
				EnabledProtocols: pointerTo("NFS"),
			},
		})
	}

	srcContainer := GetRootResource(svm, common.ELocation.FileNFS(), GetResourceOptions{
		PreferredAccount: pointerTo(PremiumFileShareAcct),
	}).(ServiceResourceManager).GetContainer("srcnfs")
	if !srcContainer.Exists() {
		srcContainer.Create(svm, ContainerProperties{
			FileContainerProperties: FileContainerProperties{
				EnabledProtocols: pointerTo("NFS"),
			},
		})
	}

	folderProperties, fileProperties, fileOrFolderPermissions := getPropertiesAndPermissions(svm, preserveProperties, preservePermissions)

	rootDir := "dir_file_copy_test_" + uuid.NewString()
	defer CleanupNFSDirectory(svm, srcContainer, rootDir)

	var dst, src ResourceManager
	if azCopyVerb == AzCopyVerbSync {
		dstObj := dstContainer.GetObject(svm, rootDir+"/test1.txt", common.EEntityType.File())
		dstObj.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{
			FileNFSPermissions: fileOrFolderPermissions,
			FileNFSProperties: &FileNFSProperties{
				FileCreationTime:  pointerTo(time.Now().Add(-10 * time.Minute)),
				FileLastWriteTime: pointerTo(time.Now().Add(-10 * time.Minute)),
			},
		})

		dstObj = dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
		dst = dstObj
	} else {
		dst = dstContainer
	}
	src = srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	// Create source directories
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
	sOriginalFileName := rootDir + "/soriginal.txt"
	obj = ResourceDefinitionObject{
		ObjectName: pointerTo(sOriginalFileName),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.File(),
			FileNFSProperties:  fileProperties,
			FileNFSPermissions: fileOrFolderPermissions,
		}}
	CreateResource[ObjectResourceManager](svm, srcContainer, obj)
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
	CreateResource[ObjectResourceManager](svm, srcContainer, obj)
	if preserveSymlinks {
		srcObjs[symLinkedFileName] = obj
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

	shouldFail := false
	if (followSymlinks && preserveSymlinks) || followSymlinks {
		shouldFail = true
	}

	stdOut, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: azCopyVerb,
			Targets: []ResourceManager{
				src.(RemoteResourceManager).WithSpecificAuthType(ResolveVariation(svm,
					[]ExplicitCredentialTypes{EExplicitCredentialType.SASToken(),
						EExplicitCredentialType.OAuth(),
					}),
					svm, CreateAzCopyTargetOptions{}),
				dst.(RemoteResourceManager).WithSpecificAuthType(ResolveVariation(svm,
					[]ExplicitCredentialTypes{EExplicitCredentialType.SASToken(),
						EExplicitCredentialType.OAuth(),
					}),
					svm, CreateAzCopyTargetOptions{}),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive:           pointerTo(true),
					FromTo:              pointerTo(common.EFromTo.FileNFSFileNFS()),
					PreservePermissions: pointerTo(preservePermissions),
					PreserveInfo:        pointerTo(preserveProperties),
					PreserveSymlinks:    pointerTo(preserveSymlinks),
					FollowSymlinks:      pointerTo(followSymlinks),
					HardlinkType:        pointerTo(hardlinkType),
				},
			},
			ShouldFail: shouldFail,
		})

	if followSymlinks && preserveSymlinks {
		ValidateContainsError(svm, stdOut, []string{
			"--preserve-symlinks and --follow-symlinks contradict",
		})
		return
	}

	if followSymlinks {
		ValidateContainsError(svm, stdOut, []string{
			"The '--follow-symlink' flag is only applicable when uploading from local filesystem.",
		})
		return
	}

	// Dont validate the root directory in case of sync
	if azCopyVerb == AzCopyVerbSync {
		delete(srcObjs, rootDir)
	}

	var hardlinkFileDeleteList []string
	for objName := range srcObjs {
		obj := srcObjs[objName]
		if obj.EntityType == common.EEntityType.Hardlink() {
			if hardlinkType == common.SkipHardlinkHandlingType {
				hardlinkFileDeleteList = append(hardlinkFileDeleteList, objName)
				hardlinkFileDeleteList = append(hardlinkFileDeleteList, obj.HardLinkedFileName)
			}
		}
	}

	if hardlinkType == common.SkipHardlinkHandlingType {
		for _, objName := range hardlinkFileDeleteList {
			delete(srcObjs, objName)
		}
	}

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, ValidateResourceOptions{
		validateObjectContent: false,
		fromTo:                common.EFromTo.FileNFSFileNFS(),
		preservePermissions:   preservePermissions,
		preserveInfo:          preserveProperties,
	})
	defer CleanupNFSDirectory(svm, dstContainer, rootDir)

	if hardlinkType == common.SkipHardlinkHandlingType {
		ValidateHardlinksSkippedCount(svm, stdOut, 2)
	} else {
		ValidateHardlinksConvertedCount(svm, stdOut, 2)
	}
	if !preserveSymlinks && !followSymlinks {
		ValidateSkippedSymlinksCount(svm, stdOut, 1)
	}
}

func (s *FilesNFSTestSuite) Scenario_AzureNFSToAzureSMB(svm *ScenarioVariationManager) {

	//
	// 	Test Scenario:
	// 	1. Create a NFS enabled file share in Azure
	// 	2. Create a folder with some files in it.
	// 	5. Run azcopy copy/sync command to copy the folder from Azure NFS enabled file share to local.
	//

	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync})
	preserveSymlinks := NamedResolveVariation(svm, map[string]bool{
		"|preserveSymlinks=true":  true,
		"|preserveSymlinks=false": false,
	})

	followSymlinks := NamedResolveVariation(svm, map[string]bool{
		"|followSymlinks=true":  true,
		"|followSymlinks=false": false,
	})

	preserveProperties := NamedResolveVariation(svm, map[string]bool{
		"|preserveInfo=true":  true,
		"|preserveInfo=false": false,
	})

	preservePermissions := NamedResolveVariation(svm, map[string]bool{
		"|preservePermissions=true":  true,
		"|preservePermissions=false": false,
	})

	hardlinkType := NamedResolveVariation(svm, map[string]common.HardlinkHandlingType{
		"|hardlinks=follow": common.DefaultHardlinkHandlingType,
		"|hardlinks=skip":   common.SkipHardlinkHandlingType,
	})

	dstShare := GetRootResource(svm, common.ELocation.File(), GetResourceOptions{
		PreferredAccount: pointerTo(PremiumFileShareAcct),
	}).(ServiceResourceManager).GetContainer("dstsmb")
	if !dstShare.Exists() {
		dstShare.Create(svm, ContainerProperties{
			FileContainerProperties: FileContainerProperties{
				EnabledProtocols: pointerTo("SMB"),
			},
		})
	}

	srcShare := GetRootResource(svm, common.ELocation.FileNFS(), GetResourceOptions{
		PreferredAccount: pointerTo(PremiumFileShareAcct),
	}).(ServiceResourceManager).GetContainer("srcnfs")
	if !srcShare.Exists() {
		srcShare.Create(svm, ContainerProperties{
			FileContainerProperties: FileContainerProperties{
				EnabledProtocols: pointerTo("NFS"),
			},
		})
	}
	folderProperties, fileProperties, _ := getPropertiesAndPermissions(svm, preserveProperties, preservePermissions)

	rootDir := "dir_file_copy_test_" + uuid.NewString()
	defer CleanupNFSDirectory(svm, srcShare, rootDir)

	var dst, src ResourceManager
	if azCopyVerb == AzCopyVerbSync {
		dstObj := dstShare.GetObject(svm, rootDir+"/test1.txt", common.EEntityType.File())
		dstObj.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{
			FileProperties: FileProperties{
				FileCreationTime:  pointerTo(time.Now().Add(-10 * time.Minute)),
				FileLastWriteTime: pointerTo(time.Now().Add(-10 * time.Minute)),
			},
		})
		dstObj = dstShare.GetObject(svm, rootDir, common.EEntityType.Folder())
		dst = dstObj
	} else {
		dst = dstShare
	}
	src = srcShare.GetObject(svm, rootDir, common.EEntityType.Folder())

	// Create destination directories
	srcObjs := make(ObjectResourceMappingFlat)

	obj := ResourceDefinitionObject{
		ObjectName: pointerTo(rootDir),
		ObjectProperties: ObjectProperties{
			EntityType:        common.EEntityType.Folder(),
			FileNFSProperties: folderProperties,
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
				EntityType:        common.EEntityType.File(),
				FileNFSProperties: fileProperties,
			}}
		CreateResource[ObjectResourceManager](svm, srcShare, obj)
		srcObjs[name] = obj
	}

	// create original file for linking symlink
	sOriginalFileName := rootDir + "/soriginal.txt"
	obj = ResourceDefinitionObject{
		ObjectName: pointerTo(sOriginalFileName),
		ObjectProperties: ObjectProperties{
			EntityType:        common.EEntityType.File(),
			FileNFSProperties: fileProperties,
		}}
	CreateResource[ObjectResourceManager](svm, srcShare, obj)
	srcObjs[sOriginalFileName] = obj

	// create symlink file
	symLinkedFileName := rootDir + "/symlinked.txt"
	obj = ResourceDefinitionObject{
		ObjectName: pointerTo(symLinkedFileName),
		ObjectProperties: ObjectProperties{
			EntityType:        common.EEntityType.Symlink(),
			FileNFSProperties: fileProperties,
			SymlinkedFileName: sOriginalFileName,
		}}
	CreateResource[ObjectResourceManager](svm, srcShare, obj)

	// create original file for creating hardlinked file
	hOriginalFileName := rootDir + "/horiginal.txt"
	obj = ResourceDefinitionObject{
		ObjectName: pointerTo(hOriginalFileName),
		ObjectProperties: ObjectProperties{
			EntityType:        common.EEntityType.File(),
			FileNFSProperties: fileProperties,
		}}
	CreateResource[ObjectResourceManager](svm, srcShare, obj)
	srcObjs[hOriginalFileName] = obj

	// create hardlinked file
	hardLinkedFileName := rootDir + "/hardlinked.txt"
	obj = ResourceDefinitionObject{
		ObjectName: pointerTo(hardLinkedFileName),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			FileNFSProperties:  fileProperties,
			HardLinkedFileName: hOriginalFileName,
		}}
	CreateResource[ObjectResourceManager](svm, srcShare, obj)
	srcObjs[hardLinkedFileName] = obj

	shouldFail := false
	if (followSymlinks && preserveSymlinks) || // both flags cannot be set to true
		followSymlinks || // follow symlinks is not supported in NFS
		preservePermissions || // preserve permissions is not supported in cross-protocol copy
		preserveSymlinks ||
		hardlinkType != common.EHardlinkHandlingType.Skip() {
		shouldFail = true
	}

	stdOut, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: azCopyVerb,
			Targets: []ResourceManager{
				src.(RemoteResourceManager).WithSpecificAuthType(
					ResolveVariation(svm, []ExplicitCredentialTypes{
						EExplicitCredentialType.SASToken(),
						EExplicitCredentialType.OAuth(),
					}), svm, CreateAzCopyTargetOptions{}),
				dst.(RemoteResourceManager).WithSpecificAuthType(
					ResolveVariation(svm, []ExplicitCredentialTypes{
						EExplicitCredentialType.SASToken(),
						EExplicitCredentialType.OAuth(),
					}), svm, CreateAzCopyTargetOptions{}),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive:           pointerTo(true),
					FromTo:              pointerTo(common.EFromTo.FileNFSFileSMB()),
					PreservePermissions: pointerTo(preservePermissions),
					PreserveInfo:        pointerTo(preserveProperties),
					HardlinkType:        pointerTo(hardlinkType),
					PreserveSymlinks:    pointerTo(preserveSymlinks),
					FollowSymlinks:      pointerTo(followSymlinks),
				},
			},
			ShouldFail: shouldFail,
		})

	if preserveSymlinks && followSymlinks {
		ValidateContainsError(svm, stdOut, []string{
			"--preserve-symlinks and --follow-symlinks contradict",
		})
		return
	}

	if preserveSymlinks {
		ValidateContainsError(svm, stdOut, []string{
			"flag --preserve-symlinks can only be used on",
		})
		return
	}

	if preservePermissions {
		ValidateContainsError(svm, stdOut, []string{
			"--preserve-permissions flag is not supported for cross-protocol transfers",
		})
		return
	}

	if followSymlinks {
		ValidateContainsError(svm, stdOut, []string{
			"The '--follow-symlink' flag is only applicable when uploading from local filesystem.",
		})
		return
	}

	if hardlinkType != common.EHardlinkHandlingType.Skip() {
		ValidateContainsError(svm, stdOut, []string{
			"Hardlinked files are not supported between NFS and SMB",
		})
		return
	}

	// Dont validate the root directory in case of sync
	if azCopyVerb == AzCopyVerbSync {
		delete(srcObjs, rootDir)
	}

	var hardlinkFileDeleteList []string
	for objName := range srcObjs {
		obj := srcObjs[objName]
		if obj.EntityType == common.EEntityType.Hardlink() {
			if hardlinkType == common.SkipHardlinkHandlingType {
				hardlinkFileDeleteList = append(hardlinkFileDeleteList, objName)
				hardlinkFileDeleteList = append(hardlinkFileDeleteList, obj.HardLinkedFileName)
			}
		}
	}

	if hardlinkType == common.SkipHardlinkHandlingType {
		for _, objName := range hardlinkFileDeleteList {
			delete(srcObjs, objName)
		}
	}

	ValidateResource[ContainerResourceManager](svm, dstShare, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, ValidateResourceOptions{
		validateObjectContent: false,
		fromTo:                common.EFromTo.FileNFSFileSMB(),
		preservePermissions:   preservePermissions,
		preserveInfo:          preserveProperties,
	})
	defer CleanupNFSDirectory(svm, dstShare, rootDir)

	if hardlinkType == common.SkipHardlinkHandlingType {
		ValidateHardlinksSkippedCount(svm, stdOut, 2)
	} else {
		ValidateHardlinksConvertedCount(svm, stdOut, 2)
	}
	if !preserveSymlinks && !followSymlinks {
		ValidateSkippedSymlinksCount(svm, stdOut, 1)
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
	preserveSymlinks := NamedResolveVariation(svm, map[string]bool{
		"|preserveSymlinks=true":  true,
		"|preserveSymlinks=false": false,
	})

	followSymlinks := NamedResolveVariation(svm, map[string]bool{
		"|followSymlinks=true":  true,
		"|followSymlinks=false": false,
	})

	preserveProperties := NamedResolveVariation(svm, map[string]bool{
		"|preserveInfo=true":  true,
		"|preserveInfo=false": false,
	})

	preservePermissions := NamedResolveVariation(svm, map[string]bool{
		"|preservePermissions=true":  true,
		"|preservePermissions=false": false,
	})

	hardlinkType := NamedResolveVariation(svm, map[string]common.HardlinkHandlingType{
		"|hardlinks=follow": common.DefaultHardlinkHandlingType,
		"|hardlinks=skip":   common.SkipHardlinkHandlingType,
	})

	// NFS Share
	dstShare := GetRootResource(svm, common.ELocation.FileNFS(), GetResourceOptions{
		PreferredAccount: pointerTo(PremiumFileShareAcct),
	}).(ServiceResourceManager).GetContainer("dstnfs")
	if !dstShare.Exists() {
		dstShare.Create(svm, ContainerProperties{
			FileContainerProperties: FileContainerProperties{
				EnabledProtocols: pointerTo("NFS"),
			},
		})
	}

	// SMB Share
	srcShare := GetRootResource(svm, common.ELocation.File(), GetResourceOptions{
		PreferredAccount: pointerTo(PremiumFileShareAcct),
	}).(ServiceResourceManager).GetContainer("srcsmb")
	if !srcShare.Exists() {
		srcShare.Create(svm, ContainerProperties{
			FileContainerProperties: FileContainerProperties{
				EnabledProtocols: pointerTo("SMB"),
			},
		})
	}

	var folderProperties, fileProperties FileProperties
	if preserveProperties {
		folderProperties = FileProperties{
			FileCreationTime: pointerTo(time.Now().Add(-2 * time.Minute)),
		}
		fileProperties = FileProperties{
			FileCreationTime:  pointerTo(time.Now().Add(-2 * time.Minute)),
			FileLastWriteTime: pointerTo(time.Now().Add(-2 * time.Minute)),
		}
	}

	rootDir := "dir_file_copy_test_" + uuid.NewString()
	defer CleanupNFSDirectory(svm, srcShare, rootDir)

	var dst, src ResourceManager
	if azCopyVerb == AzCopyVerbSync {
		dstObj := dstShare.GetObject(svm, rootDir+"/test0.txt", common.EEntityType.File())
		dstObj.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{
			FileNFSProperties: &FileNFSProperties{
				FileCreationTime:  pointerTo(time.Now().Add(-10 * time.Minute)),
				FileLastWriteTime: pointerTo(time.Now().Add(-10 * time.Minute)),
			},
		})

		dstObj = dstShare.GetObject(svm, rootDir, common.EEntityType.Folder())
		dst = dstObj
	} else {
		dst = dstShare
	}
	src = srcShare.GetObject(svm, rootDir, common.EEntityType.Folder())

	// Create destination directories
	srcObjs := make(ObjectResourceMappingFlat)

	obj := ResourceDefinitionObject{
		ObjectName: pointerTo(rootDir),
		ObjectProperties: ObjectProperties{
			EntityType:     common.EEntityType.Folder(),
			FileProperties: folderProperties,
		},
	}

	CreateResource[ObjectResourceManager](svm, srcShare, obj)
	srcObjs[rootDir] = obj

	for i := range 1 {
		name := rootDir + "/test" + strconv.Itoa(i) + ".txt"
		obj := ResourceDefinitionObject{
			ObjectName: pointerTo(name),
			Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
			ObjectProperties: ObjectProperties{
				EntityType:     common.EEntityType.File(),
				FileProperties: fileProperties,
			},
		}
		CreateResource[ObjectResourceManager](svm, srcShare, obj)
		srcObjs[name] = obj
	}

	shouldFail := false
	if (followSymlinks && preserveSymlinks) || // both flags cannot be set to true
		followSymlinks || // follow symlinks is not supported in NFS
		preservePermissions || // preserve permissions is not supported in cross-protocol copy
		preserveSymlinks ||
		hardlinkType != common.EHardlinkHandlingType.Skip() {
		shouldFail = true
	}

	stdOut, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: azCopyVerb,
			Targets: []ResourceManager{
				src.(RemoteResourceManager).WithSpecificAuthType(
					ResolveVariation(svm, []ExplicitCredentialTypes{
						EExplicitCredentialType.SASToken(),
						EExplicitCredentialType.OAuth(),
					}),
					svm, CreateAzCopyTargetOptions{}),
				dst.(RemoteResourceManager).WithSpecificAuthType(
					ResolveVariation(svm, []ExplicitCredentialTypes{
						EExplicitCredentialType.SASToken(),
						EExplicitCredentialType.OAuth(),
					}), svm, CreateAzCopyTargetOptions{}),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive:           pointerTo(true),
					FromTo:              pointerTo(common.EFromTo.FileSMBFileNFS()),
					PreservePermissions: pointerTo(preservePermissions),
					PreserveInfo:        pointerTo(true),
					PreserveSymlinks:    pointerTo(preserveSymlinks),
					FollowSymlinks:      pointerTo(followSymlinks),
					HardlinkType:        pointerTo(hardlinkType),
				},
			},
			ShouldFail: shouldFail,
		})

	if preserveSymlinks && followSymlinks {
		ValidateContainsError(svm, stdOut, []string{
			"--preserve-symlinks and --follow-symlinks contradict",
		})
		return
	}

	if preserveSymlinks {
		ValidateContainsError(svm, stdOut, []string{
			"flag --preserve-symlinks can only be used on",
		})
		return
	}

	if preservePermissions {
		ValidateContainsError(svm, stdOut, []string{
			"--preserve-permissions flag is not supported for cross-protocol transfers",
		})
		return
	}

	if followSymlinks {
		ValidateContainsError(svm, stdOut, []string{
			"The '--follow-symlink' flag is only applicable when uploading from local filesystem.",
		})
		return
	}

	if hardlinkType != common.EHardlinkHandlingType.Skip() {
		ValidateContainsError(svm, stdOut, []string{
			"'--hardlinks' must be set to 'skip'",
		})
		return
	}

	// Dont validate the root directory in case of sync
	if azCopyVerb == AzCopyVerbSync {
		delete(srcObjs, rootDir)
	}

	var hardlinkFileDeleteList []string
	for objName := range srcObjs {
		obj := srcObjs[objName]
		if obj.EntityType == common.EEntityType.Hardlink() {
			if hardlinkType == common.SkipHardlinkHandlingType {
				hardlinkFileDeleteList = append(hardlinkFileDeleteList, objName)
				hardlinkFileDeleteList = append(hardlinkFileDeleteList, obj.HardLinkedFileName)
			}
		}
	}

	if hardlinkType == common.SkipHardlinkHandlingType {
		for _, objName := range hardlinkFileDeleteList {
			delete(srcObjs, objName)
		}
	}

	ValidateResource[ContainerResourceManager](svm, dstShare, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, ValidateResourceOptions{
		validateObjectContent: true,
		fromTo:                common.EFromTo.FileSMBFileNFS(),
		preserveInfo:          true,
		preservePermissions:   preservePermissions,
	})
	defer CleanupNFSDirectory(svm, dstShare, rootDir)
}

func (s *FilesNFSTestSuite) Scenario_TestInvalidScenariosForNFS(svm *ScenarioVariationManager) {

	//
	//Test Scenarios
	//1. If --from-to flag is not provided and if the source or destination is NFS
	// its an unsupported scenario
	//

	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync}) // Calculate verb early to create the destination object early

	dstObj1 := GetRootResource(svm, common.ELocation.FileNFS(), GetResourceOptions{
		PreferredAccount: pointerTo(PremiumFileShareAcct),
	}).(ServiceResourceManager).GetContainer("dstnfs")
	if !dstObj1.Exists() {
		dstObj1.Create(svm, ContainerProperties{
			FileContainerProperties: FileContainerProperties{
				EnabledProtocols: pointerTo("NFS"),
			},
		})
	}

	dstObj2 := GetRootResource(svm, common.ELocation.File(), GetResourceOptions{
		PreferredAccount: pointerTo(PremiumFileShareAcct),
	}).(ServiceResourceManager).GetContainer("dstsmb")
	if !dstObj2.Exists() {
		dstObj2.Create(svm, ContainerProperties{
			FileContainerProperties: FileContainerProperties{
				EnabledProtocols: pointerTo("SMB"),
			},
		})
	}
	dstShare := ResolveVariation(svm, []ContainerResourceManager{dstObj1, dstObj2})

	srcObj1 := GetRootResource(svm, common.ELocation.File(), GetResourceOptions{
		PreferredAccount: pointerTo(PremiumFileShareAcct),
	}).(ServiceResourceManager).GetContainer("srcsmb")
	if !srcObj1.Exists() {
		srcObj1.Create(svm, ContainerProperties{
			FileContainerProperties: FileContainerProperties{
				EnabledProtocols: pointerTo("SMB"),
			},
		})
	}

	srcObj2 := GetRootResource(svm, common.ELocation.FileNFS(), GetResourceOptions{
		PreferredAccount: pointerTo(PremiumFileShareAcct),
	}).(ServiceResourceManager).GetContainer("srcnfs")
	if !srcObj2.Exists() {
		srcObj2.Create(svm, ContainerProperties{
			FileContainerProperties: FileContainerProperties{
				EnabledProtocols: pointerTo("NFS"),
			},
		})
	}

	srcShare := ResolveVariation(svm, []ContainerResourceManager{srcObj1, srcObj2})

	rootDir := "dir_file_copy_test_" + uuid.NewString()
	var dst, src ResourceManager

	// The object must exist already if we're syncing.
	if azCopyVerb == AzCopyVerbSync {
		dstObj := dstShare.GetObject(svm, rootDir+"/test1.txt", common.EEntityType.File())
		dstObj.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{})
		dstObj = dstShare.GetObject(svm, rootDir, common.EEntityType.Folder())
		if !svm.Dryrun() {
			// Make sure the LMT is in the past
			time.Sleep(time.Second * 5)
		}
		dst = dstObj
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
}
