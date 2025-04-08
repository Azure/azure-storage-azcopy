package e2etest

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"os/user"
	"runtime"
	"strconv"
	"time"
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

//func (s *FilesNFSTestSuite) Scenario_LocalLinuxToAzureNFS_PreservePropertiesAndPerms(svm *ScenarioVariationManager) {
//
//	if runtime.GOOS == "windows" || runtime.GOOS == "macos" {
//		return
//	}
//
//	dstContainer := GetAccount(svm, PremiumFileShareAcct).GetService(svm, common.ELocation.File()).GetContainer("aznfs3")
//	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})
//
//	srcObject := srcContainer.GetObject(svm, "dir_2_files", common.EEntityType.Folder())
//	srcObjs := make(ObjectResourceMappingFlat)
//	srcObjRes := make(map[string]ObjectResourceManager)
//	uid, gid := GetCurrentUIDAndGID(svm)
//	for i := range 2 {
//		name := "dir_2_files/test" + strconv.Itoa(i) + ".txt"
//		obj := ResourceDefinitionObject{
//			ObjectName: pointerTo(name),
//			Body:       NewRandomObjectContentContainer(SizeFromString("10K")),
//			ObjectProperties: ObjectProperties{
//				FileNFSProperties: &FileNFSProperties{
//					FileCreationTime:  pointerTo(time.Now()),
//					FileLastWriteTime: pointerTo(time.Now()),
//				},
//				FileNFSPermissions: &FileNFSPermissions{
//					Owner:    pointerTo(uid),
//					Group:    pointerTo(gid),
//					FileMode: pointerTo("0755"),
//				},
//			},
//		}
//		srcObjRes[name] = CreateResource[ObjectResourceManager](svm, srcContainer, obj)
//		srcObjs[name] = obj
//	}
//
//	sasOpts := GenericAccountSignatureValues{}
//
//	_, _ = RunAzCopy(
//		svm,
//		AzCopyCommand{
//			Verb: AzCopyVerbCopy,
//			Targets: []ResourceManager{
//				TryApplySpecificAuthType(srcObject, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
//					SASTokenOptions: sasOpts,
//				}),
//				TryApplySpecificAuthType(dstContainer, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
//					SASTokenOptions: sasOpts,
//				}),
//			},
//			Flags: CopyFlags{
//				CopySyncCommonFlags: CopySyncCommonFlags{
//					Recursive:           pointerTo(true),
//					NFS:                 pointerTo(true),
//					PreserveInfo:        pointerTo(true),
//					PreservePermissions: pointerTo(true),
//				},
//			},
//		})
//
//	for objName := range srcObjs {
//		obj := srcObjs[objName]
//		obj.ObjectProperties = srcObjRes[objName].GetProperties(svm)
//	}
//
//	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
//		Objects: srcObjs,
//	}, true)
//}
//
//func (s *FilesNFSTestSuite) Scenario_LocalLinuxToAzureNFS_PreservePropertiesOnly(svm *ScenarioVariationManager) {
//
//	if runtime.GOOS == "windows" || runtime.GOOS == "macos" {
//		return
//	}
//
//	dstContainer := GetAccount(svm, PremiumFileShareAcct).GetService(svm, common.ELocation.File()).GetContainer("aznfs3")
//	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})
//
//	srcObject := srcContainer.GetObject(svm, "dir_2_files", common.EEntityType.Folder())
//	srcObjs := make(ObjectResourceMappingFlat)
//
//	for i := range 2 {
//		name := "dir_2_files/test" + strconv.Itoa(i) + ".txt"
//		obj := ResourceDefinitionObject{
//			ObjectName: pointerTo(name),
//			Body:       NewRandomObjectContentContainer(SizeFromString("10K")),
//			ObjectProperties: ObjectProperties{
//				FileNFSProperties: &FileNFSProperties{
//					FileCreationTime:  pointerTo(time.Now()),
//					FileLastWriteTime: pointerTo(time.Now()),
//				},
//			},
//		}
//		srcObjRes[name] = CreateResource[ObjectResourceManager](svm, srcContainer, obj)
//	}
//
//	sasOpts := GenericAccountSignatureValues{}
//
//	_, _ = RunAzCopy(
//		svm,
//		AzCopyCommand{
//			Verb: AzCopyVerbCopy,
//			Targets: []ResourceManager{
//				TryApplySpecificAuthType(srcObject, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
//					SASTokenOptions: sasOpts,
//				}),
//				TryApplySpecificAuthType(dstContainer, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
//					SASTokenOptions: sasOpts,
//				}),
//			},
//			Flags: CopyFlags{
//				CopySyncCommonFlags: CopySyncCommonFlags{
//					Recursive:    pointerTo(true),
//					NFS:          pointerTo(true),
//					PreserveInfo: pointerTo(true),
//				},
//			},
//		})
//
//	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
//		Objects: srcObjs,
//	}, true)
//}

//func (s *FilesNFSTestSuite) Scenario_LocalToAzureNFS_PreservePermissionsOnly(svm *ScenarioVariationManager) {
//
//	if runtime.GOOS == "windows" || runtime.GOOS == "macos" {
//		return
//	}
//
//	dstContainer := GetAccount(svm, PremiumFileShareAcct).GetService(svm, common.ELocation.File()).GetContainer("aznfs3")
//	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})
//
//	srcObject := srcContainer.GetObject(svm, "dir_2_files", common.EEntityType.Folder())
//	srcObjs := make(ObjectResourceMappingFlat)
//	uid, gid := GetCurrentUIDAndGID(svm)
//	for i := range 2 {
//		name := "dir_2_files/test" + strconv.Itoa(i) + ".txt"
//		obj := ResourceDefinitionObject{
//			ObjectName: pointerTo(name),
//			Body:       NewRandomObjectContentContainer(SizeFromString("10K")),
//			ObjectProperties: ObjectProperties{
//				FileNFSPermissions: &FileNFSPermissions{
//					Owner:    pointerTo(uid),
//					Group:    pointerTo(gid),
//					FileMode: pointerTo("0755"),
//				},
//			},
//		}
//		CreateResource[ObjectResourceManager](svm, srcContainer, obj)
//	}
//
//	sasOpts := GenericAccountSignatureValues{}
//
//	_, _ = RunAzCopy(
//		svm,
//		AzCopyCommand{
//			Verb: AzCopyVerbCopy,
//			Targets: []ResourceManager{
//				TryApplySpecificAuthType(srcObject, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
//					SASTokenOptions: sasOpts,
//				}),
//				TryApplySpecificAuthType(dstContainer, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
//					SASTokenOptions: sasOpts,
//				}),
//			},
//			Flags: CopyFlags{
//				CopySyncCommonFlags: CopySyncCommonFlags{
//					Recursive:           pointerTo(true),
//					NFS:                 pointerTo(true),
//					PreserveInfo:        pointerTo(true),
//					PreservePermissions: pointerTo(true),
//				},
//			},
//		})
//
//	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
//		Objects: srcObjs,
//	}, true)
//}

//TODO: Not working
//func (s *FilesNFSTestSuite) Scenario_AzureNFSToLocal_PreservePropertiesAndPerms(svm *ScenarioVariationManager) {
//
//	if runtime.GOOS == "windows" || runtime.GOOS == "macos" {
//		return
//	}
//
//	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})
//	srcContainer := GetAccount(svm, PremiumFileShareAcct).GetService(svm, common.ELocation.File()).GetContainer("aznfs3")
//
//	uid, gid := GetCurrentUIDAndGID(svm)
//	srcObject := srcContainer.GetObject(svm, "dir_2_files", common.EEntityType.Folder())
//	srcObjs := make(ObjectResourceMappingFlat)
//	for i := range 2 {
//		name := "dir_2_files/test" + strconv.Itoa(i) + ".txt"
//		obj := ResourceDefinitionObject{
//			ObjectName: pointerTo(name),
//			Body:       NewRandomObjectContentContainer(SizeFromString("10K")),
//			ObjectProperties: ObjectProperties{
//				FileNFSProperties: &FileNFSProperties{
//					FileCreationTime:  pointerTo(time.Now()),
//					FileLastWriteTime: pointerTo(time.Now()),
//				},
//				FileNFSPermissions: &FileNFSPermissions{
//					Owner:    pointerTo(uid),
//					Group:    pointerTo(gid),
//					FileMode: pointerTo("0755"),
//				},
//			},
//		}
//		CreateResource[ObjectResourceManager](svm, srcContainer, obj)
//		srcObjs[name] = obj
//	}
//	sasOpts := GenericAccountSignatureValues{}
//
//	stdOut, _ := RunAzCopy(
//		svm,
//		AzCopyCommand{
//			Verb: AzCopyVerbCopy,
//			Targets: []ResourceManager{
//				TryApplySpecificAuthType(srcObject, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
//					SASTokenOptions: sasOpts,
//				}),
//				dstContainer,
//			},
//			Flags: CopyFlags{
//				CopySyncCommonFlags: CopySyncCommonFlags{
//					Recursive:           pointerTo(true),
//					NFS:                 pointerTo(true),
//					PreserveInfo:        pointerTo(true),
//					PreservePermissions: pointerTo(true),
//				},
//			},
//		})
//	fmt.Println("STDOUT: ", stdOut)
//	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
//		Objects: srcObjs,
//	}, true)
//}

func (s *FilesNFSTestSuite) Scenario_AzureNFSToAzureNFS_PreservePropertiesAndPerms(svm *ScenarioVariationManager) {

	if runtime.GOOS == "windows" || runtime.GOOS == "macos" {
		return
	}

	dstContainer := GetAccount(svm, PremiumFileShareAcct).GetService(svm, common.ELocation.File()).GetContainer("aznfs2")
	srcContainer := GetAccount(svm, PremiumFileShareAcct).GetService(svm, common.ELocation.File()).GetContainer("aznfs3")

	uid, gid := GetCurrentUIDAndGID(svm)

	dirsToCreate := []string{"dir_file_copy_test", "dir_file_copy_test/sub_dir_copy_test"}

	// Create destination directories
	srcObjs := make(ObjectResourceMappingFlat)
	for _, dir := range dirsToCreate {
		obj := ResourceDefinitionObject{
			ObjectName: pointerTo(dir),
			ObjectProperties: ObjectProperties{
				EntityType: common.EEntityType.Folder(),

				FileNFSProperties: &FileNFSProperties{
					FileCreationTime: pointerTo(time.Now()),
					//FileLastWriteTime: pointerTo(time.Now()),
				},
				FileNFSPermissions: &FileNFSPermissions{
					Owner:    pointerTo(uid),
					Group:    pointerTo(gid),
					FileMode: pointerTo("0755"),
				},
			},
		}
		CreateResource[ObjectResourceManager](svm, srcContainer, obj)
		srcObjs[dir] = obj

		for i := range 2 {
			name := dir + "/test" + strconv.Itoa(i) + ".txt"
			obj := ResourceDefinitionObject{
				ObjectName: pointerTo(name),
				Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
				ObjectProperties: ObjectProperties{
					FileNFSProperties: &FileNFSProperties{
						FileCreationTime:  pointerTo(time.Now()),
						FileLastWriteTime: pointerTo(time.Now()),
					},
					FileNFSPermissions: &FileNFSPermissions{
						Owner:    pointerTo(uid),
						Group:    pointerTo(gid),
						FileMode: pointerTo("0755"),
					},
				}}
			CreateResource[ObjectResourceManager](svm, srcContainer, obj)
			srcObjs[name] = obj
		}
	}

	sasOpts := GenericAccountSignatureValues{}

	stdOut, _ := RunAzCopy(
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
					Recursive:           pointerTo(true),
					NFS:                 pointerTo(true),
					PreserveInfo:        pointerTo(true),
					PreservePermissions: pointerTo(true),
				},
			},
		})
	fmt.Println("STDOUT: ", stdOut)
	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, true)
}
