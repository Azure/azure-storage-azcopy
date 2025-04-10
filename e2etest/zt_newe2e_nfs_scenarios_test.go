package e2etest

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"math/rand"
	"os/user"
	"runtime"
	"strconv"
	"time"
)

func init() {
	suiteManager.RegisterSuite(&FilesNFSTestSuite{})
}

type FilesNFSTestSuite struct{}

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randomString(length int) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func GetCurrentUIDAndGID(a Asserter) (uid, gid string) {
	// Get the current user information
	currentUser, err := user.Current()
	a.NoError("Error retrieving current user:", err)

	uid = currentUser.Uid
	gid = currentUser.Gid
	return
}

func (s *FilesNFSTestSuite) Scenario_LocalLinuxToAzureNFS_PreservePropertiesAndPerms(svm *ScenarioVariationManager) {

	if runtime.GOOS == "windows" || runtime.GOOS == "macos" {
		return
	}
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{ /*AzCopyVerbCopy, */ AzCopyVerbSync}) // Calculate verb early to create the destination object early
	uid, gid := GetCurrentUIDAndGID(svm)

	dstContainer := GetAccount(svm, PremiumFileShareAcct).GetService(svm, common.ELocation.File()).GetContainer("aznfs3")
	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})

	rootDir := "dir_file_copy_test_" + randomString(6)
	dirsToCreate := []string{rootDir, rootDir + "/sub_dir_copy_test"}

	// Create destination directories
	srcObjs := make(ObjectResourceMappingFlat)
	srcObjRes := make(map[string]ObjectResourceManager)
	for _, dir := range dirsToCreate {
		obj := ResourceDefinitionObject{
			ObjectName: pointerTo(dir),
			ObjectProperties: ObjectProperties{
				EntityType: common.EEntityType.Folder(),

				FileNFSProperties: &FileNFSProperties{
					FileCreationTime: pointerTo(time.Now()),
				},
				FileNFSPermissions: &FileNFSPermissions{
					Owner:    pointerTo(uid),
					Group:    pointerTo(gid),
					FileMode: pointerTo("0755"),
				},
			},
		}
		srcObjRes[dir] = CreateResource[ObjectResourceManager](svm, srcContainer, obj)
		srcObjs[dir] = obj

		for i := range 2 {
			name := dir + "/test" + strconv.Itoa(i) + ".txt"
			obj := ResourceDefinitionObject{
				ObjectName: pointerTo(name),
				Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
				ObjectProperties: ObjectProperties{
					EntityType: common.EEntityType.File(),
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
			srcObjRes[name] = CreateResource[ObjectResourceManager](svm, srcContainer, obj)
			srcObjs[name] = obj
		}
	}
	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	var dst ResourceManager
	if azCopyVerb == AzCopyVerbSync {
		dstObj := dstContainer.GetObject(svm, rootDir+"/sub_dir_copy_test/test1.txt", common.EEntityType.File())
		dstObj.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{})
		if !svm.Dryrun() {
			// Make sure the LMT is in the past
			time.Sleep(time.Second * 10)
		}
		dst = dstObj
	} else {
		dst = dstContainer
	}

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
					PreservePermissions: pointerTo(true),
				},
			},
		})

	// As we cannot set creationTime in linux we will fetch the properties from local and set it to src object properties
	for objName := range srcObjs {
		obj := srcObjs[objName]
		objProp := srcObjRes[objName].GetProperties(svm)
		obj.ObjectProperties.FileNFSProperties.FileCreationTime = objProp.FileProperties.FileCreationTime

	}

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, true)
}

/*
func (s *FilesNFSTestSuite) Scenario_LocalLinuxToAzureNFS_PreservePropertiesOnly(svm *ScenarioVariationManager) {

	if runtime.GOOS == "windows" || runtime.GOOS == "macos" {
		return
	}
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync}) // Calculate verb early to create the destination object early

	dstContainer := GetAccount(svm, PremiumFileShareAcct).GetService(svm, common.ELocation.File()).GetContainer("aznfs3")
	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})

	rootDir := "dir_file_copy_test_" + randomString(6)
	dirsToCreate := []string{rootDir, rootDir + "/sub_dir_copy_test"}

	// Create destination directories
	srcObjs := make(ObjectResourceMappingFlat)
	srcObjRes := make(map[string]ObjectResourceManager)
	for _, dir := range dirsToCreate {
		obj := ResourceDefinitionObject{
			ObjectName: pointerTo(dir),
			ObjectProperties: ObjectProperties{
				EntityType: common.EEntityType.Folder(),

				FileNFSProperties: &FileNFSProperties{
					FileCreationTime: pointerTo(time.Now()),
				},
			},
		}
		srcObjRes[dir] = CreateResource[ObjectResourceManager](svm, srcContainer, obj)
		srcObjs[dir] = obj

		for i := range 2 {
			name := dir + "/test" + strconv.Itoa(i) + ".txt"
			obj := ResourceDefinitionObject{
				ObjectName: pointerTo(name),
				Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
				ObjectProperties: ObjectProperties{
					EntityType: common.EEntityType.File(),
					FileNFSProperties: &FileNFSProperties{
						FileCreationTime:  pointerTo(time.Now()),
						FileLastWriteTime: pointerTo(time.Now()),
					},
				}}
			srcObjRes[name] = CreateResource[ObjectResourceManager](svm, srcContainer, obj)
			srcObjs[name] = obj
		}
	}
	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	var dst ResourceManager
	if azCopyVerb == AzCopyVerbSync {
		dstObj := dstContainer.GetObject(svm, rootDir+"/sub_dir_copy_test/test1.txt", common.EEntityType.File())
		dstObj.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{})
		if !svm.Dryrun() {
			// Make sure the LMT is in the past
			time.Sleep(time.Second * 10)
		}
		dst = dstObj
	} else {
		dst = dstContainer
	}

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
					PreservePermissions: pointerTo(false),
				},
			},
		})

	// As we cannot set creationTime in linux we will fetch the properties from local and set it to src object properties
	for objName := range srcObjs {
		obj := srcObjs[objName]
		objProp := srcObjRes[objName].GetProperties(svm)
		obj.ObjectProperties.FileNFSProperties.FileCreationTime = objProp.FileProperties.FileCreationTime

	}

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, true)
}

func (s *FilesNFSTestSuite) Scenario_LocalToAzureNFS_PreservePermissionsOnly(svm *ScenarioVariationManager) {

	if runtime.GOOS == "windows" || runtime.GOOS == "macos" {
		return
	}

	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync}) // Calculate verb early to create the destination object early
	uid, gid := GetCurrentUIDAndGID(svm)

	dstContainer := GetAccount(svm, PremiumFileShareAcct).GetService(svm, common.ELocation.File()).GetContainer("aznfs3")
	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})

	rootDir := "dir_file_copy_test_" + randomString(6)
	dirsToCreate := []string{rootDir, rootDir + "/sub_dir_copy_test"}

	// Create destination directories
	srcObjs := make(ObjectResourceMappingFlat)
	srcObjRes := make(map[string]ObjectResourceManager)

	for _, dir := range dirsToCreate {
		obj := ResourceDefinitionObject{
			ObjectName: pointerTo(dir),
			ObjectProperties: ObjectProperties{
				EntityType: common.EEntityType.Folder(),
				FileNFSPermissions: &FileNFSPermissions{
					Owner:    pointerTo(uid),
					Group:    pointerTo(gid),
					FileMode: pointerTo("0755"),
				},
			},
		}
		srcObjRes[dir] = CreateResource[ObjectResourceManager](svm, srcContainer, obj)
		srcObjs[dir] = obj

		for i := range 2 {
			name := dir + "/test" + strconv.Itoa(i) + ".txt"
			obj := ResourceDefinitionObject{
				ObjectName: pointerTo(name),
				Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
				ObjectProperties: ObjectProperties{
					EntityType: common.EEntityType.File(),
					FileNFSPermissions: &FileNFSPermissions{
						Owner:    pointerTo(uid),
						Group:    pointerTo(gid),
						FileMode: pointerTo("0755"),
					},
				}}
			srcObjRes[name] = CreateResource[ObjectResourceManager](svm, srcContainer, obj)
			srcObjs[name] = obj
		}
	}

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	var dst ResourceManager

	// Sync file permissions
	if azCopyVerb == AzCopyVerbSync {
		dstObj := dstContainer.GetObject(svm, rootDir+"/sub_dir_copy_test/test1.txt", common.EEntityType.File())
		dstObj.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{
			FileNFSPermissions: &FileNFSPermissions{
				Owner:    pointerTo(gid),
				Group:    pointerTo(gid),
				FileMode: pointerTo("0644"),
			},
		})
		dstDirObj := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
		dst = dstDirObj
	} else {
		dst = dstContainer
	}

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
					Recursive:           pointerTo(true),
					NFS:                 pointerTo(true),
					PreserveInfo:        pointerTo(false),
					PreservePermissions: pointerTo(true),
				},
			},
		})

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, true)
}

func (s *FilesNFSTestSuite) Scenario_AzureNFSToLocal_PreservePropertiesAndPerms(svm *ScenarioVariationManager) {

	if runtime.GOOS == "windows" || runtime.GOOS == "macos" {
		return
	}

	uid, gid := GetCurrentUIDAndGID(svm)

	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{})

	srcContainer := GetAccount(svm, PremiumFileShareAcct).GetService(svm, common.ELocation.File()).GetContainer("aznfs3")
	rootDir := "dir_file_copy_test_" + randomString(6)
	dirsToCreate := []string{rootDir, rootDir + "/sub_dir_copy_test"}

	// Create destination directories
	srcObjs := make(ObjectResourceMappingFlat)
	for _, dir := range dirsToCreate {
		obj := ResourceDefinitionObject{
			ObjectName: pointerTo(dir),
			ObjectProperties: ObjectProperties{
				EntityType: common.EEntityType.Folder(),

				FileNFSProperties: &FileNFSProperties{
					FileCreationTime: pointerTo(time.Now()),
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
					EntityType: common.EEntityType.File(),
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
	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	sasOpts := GenericAccountSignatureValues{}
	_, _ = RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbCopy,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcDirObj, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
				dstContainer,
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive:           pointerTo(true),
					NFS:                 pointerTo(true),
					PreservePermissions: pointerTo(true),
					// --preserve-info flag will be true by default in case of linux
				},
			},
		})

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, true)
}

func (s *FilesNFSTestSuite) Scenario_AzureNFSToAzureNFS_PreservePropertiesAndPerms(svm *ScenarioVariationManager) {

	if runtime.GOOS == "windows" || runtime.GOOS == "macos" {
		return
	}

	dstContainer := GetAccount(svm, PremiumFileShareAcct).GetService(svm, common.ELocation.File()).GetContainer("aznfs2")
	srcContainer := GetAccount(svm, PremiumFileShareAcct).GetService(svm, common.ELocation.File()).GetContainer("aznfs3")

	uid, gid := GetCurrentUIDAndGID(svm)

	rootDir := "dir_file_copy_test_" + randomString(6)
	dirsToCreate := []string{rootDir, rootDir + "/sub_dir_copy_test"}

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
					EntityType: common.EEntityType.File(),
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

	_, _ = RunAzCopy(
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

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: srcObjs,
	}, true)
}
*/
