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

func (s *FilesNFSTestSuite) Scenario_LocalLinuxToAzureNFS(svm *ScenarioVariationManager) {

	if runtime.GOOS == "windows" || runtime.GOOS == "darwin" {
		svm.InvalidateScenario()
		return
	}
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync}) // Calculate verb early to create the destination object early
	preserveProperties := ResolveVariation(svm, []bool{true, false})
	preservePermissions := ResolveVariation(svm, []bool{true, false})
	uid, gid := GetCurrentUIDAndGID(svm)

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

	// Create destination directories
	srcObjs := make(ObjectResourceMappingFlat)
	srcObjRes := make(map[string]ObjectResourceManager)

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

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	sasOpts := GenericAccountSignatureValues{}

	_, _ = RunAzCopy(
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

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, true)
}

func (s *FilesNFSTestSuite) Scenario_AzureNFSToLocal(svm *ScenarioVariationManager) {

	if runtime.GOOS == "windows" || runtime.GOOS == "darwin" {
		svm.InvalidateScenario()
		return
	}
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync}) // Calculate verb early to create the destination object early
	preserveProperties := ResolveVariation(svm, []bool{true, false})
	preservePermissions := ResolveVariation(svm, []bool{true, false})
	uid, gid := GetCurrentUIDAndGID(svm)

	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})

	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.File()}), GetResourceOptions{
		PreferredAccount: pointerTo(PremiumFileShareAcct),
	}), ResourceDefinitionContainer{
		Properties: ContainerProperties{
			FileContainerProperties: FileContainerProperties{
				EnabledProtocols: pointerTo("NFS"),
			},
		},
	})
	rootDir := "dir_file_copy_test_" + uuid.NewString()

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

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	sasOpts := GenericAccountSignatureValues{}
	_, _ = RunAzCopy(
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

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, true)
}

func (s *FilesNFSTestSuite) Scenario_AzureNFSToAzureNFS(svm *ScenarioVariationManager) {

	if runtime.GOOS == "windows" || runtime.GOOS == "macos" {
		svm.InvalidateScenario()
		return
	}
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync}) // Calculate verb early to create the destination object early
	dstContainer := GetAccount(svm, PremiumFileShareAcct).GetService(svm, common.ELocation.File()).GetContainer("aznfs2")
	srcContainer := GetAccount(svm, PremiumFileShareAcct).GetService(svm, common.ELocation.File()).GetContainer("aznfs3")

	preserveProperties := ResolveVariation(svm, []bool{true, false})
	preservePermissions := ResolveVariation(svm, []bool{true, false})

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

	rootDir := "dir_file_copy_test_" + uuid.NewString()

	var dst, src ResourceManager
	if azCopyVerb == AzCopyVerbSync {
		dstObj := dstContainer.GetObject(svm, rootDir+"/test1.txt", common.EEntityType.File())
		dstObj.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{
			FileNFSPermissions: fileOrFolderPermissions,
			FileNFSProperties:  fileProperties,
		})
		dstObj = dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
		dstObj.SetObjectProperties(svm, ObjectProperties{
			FileNFSPermissions: fileOrFolderPermissions,
			FileNFSProperties:  folderProperties,
		})
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

	sasOpts := GenericAccountSignatureValues{}

	_, _ = RunAzCopy(
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

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, false)
}
