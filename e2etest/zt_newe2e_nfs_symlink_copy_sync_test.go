package e2etest

import (
	"runtime"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/google/uuid"
)

// setupSymLinkSyncContainersForFromTo returns (srcContainer, dstContainer, rootDir) with
// containers appropriate for the given fromTo direction.  Caller must defer
// cleanupHardlinkSyncForFromTo.
func setupSymLinkSyncContainersForFromTo(svm *ScenarioVariationManager, fromTo common.FromTo) (
	srcContainer ContainerResourceManager,
	dstContainer ContainerResourceManager,
	rootDir string,
) {
	getNFSDst := func() ContainerResourceManager {
		c := GetRootResource(svm, common.ELocation.FileNFS(), GetResourceOptions{
			PreferredAccount: pointerTo(PremiumFileShareAcct),
		}).(ServiceResourceManager).GetContainer("slsyncdst")
		if !c.Exists() {
			c.Create(svm, ContainerProperties{
				FileContainerProperties: FileContainerProperties{
					EnabledProtocols: pointerTo("NFS"),
				},
			})
		}
		return c
	}
	getNFSSrc := func() ContainerResourceManager {
		c := GetRootResource(svm, common.ELocation.FileNFS(), GetResourceOptions{
			PreferredAccount: pointerTo(PremiumFileShareAcct),
		}).(ServiceResourceManager).GetContainer("slsyncsrc")
		if !c.Exists() {
			c.Create(svm, ContainerProperties{
				FileContainerProperties: FileContainerProperties{
					EnabledProtocols: pointerTo("NFS"),
				},
			})
		}
		return c
	}
	getLocal := func() ContainerResourceManager {
		return CreateResource[ContainerResourceManager](svm, GetRootResource(
			svm, common.ELocation.Local()), ResourceDefinitionContainer{})
	}
	switch fromTo {
	case common.EFromTo.LocalFileNFS():
		srcContainer, dstContainer = getLocal(), getNFSDst()
	case common.EFromTo.FileNFSLocal():
		srcContainer, dstContainer = getNFSSrc(), getLocal()
	default: // FileNFSFileNFS
		srcContainer, dstContainer = getNFSSrc(), getNFSDst()
	}
	rootDir = "slsync_" + uuid.NewString()
	return
}

// runSyncForFromTo runs `azcopy sync` for any fromTo direction
//
// Pass preserveSymlinks=true to also set --preserve-symlinks.
func runSyncForFromTo(
	svm *ScenarioVariationManager,
	srcDirObj ResourceManager,
	dstDirObj ResourceManager,
	fromTo common.FromTo,
	deleteDestination bool,
	preserveSymlinks ...bool,
) AzCopyStdout {
	authIfRemote := func(rm ResourceManager) ResourceManager {
		if remote, ok := rm.(RemoteResourceManager); ok {
			return remote.WithSpecificAuthType(
				ResolveVariation(svm, []ExplicitCredentialTypes{
					EExplicitCredentialType.SASToken(),
					EExplicitCredentialType.OAuth(),
				}), svm, CreateAzCopyTargetOptions{})
		}
		return rm
	}
	flags := SyncFlags{
		CopySyncCommonFlags: CopySyncCommonFlags{
			Recursive: pointerTo(true),
			FromTo:    pointerTo(fromTo),
		},
		DeleteDestination: pointerTo(deleteDestination),
	}
	if len(preserveSymlinks) > 0 && preserveSymlinks[0] {
		flags.CopySyncCommonFlags.PreserveSymlinks = pointerTo(true)
	}
	stdOut, _ := RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbSync,
		Targets: []ResourceManager{authIfRemote(srcDirObj), authIfRemote(dstDirObj)},
		Flags:   flags,
	})
	return stdOut
}

// Scenario 1: Stale symlink at destination is removed by --delete-destination.
//
// Source:
//
//	target.txt          (regular file)
//
// Destination (before sync):
//
//	target.txt          (regular file)
//	stale_link.txt      (symlink → target.txt — not present at source)
//
// Expected:
// - stale_link.txt is deleted from destination,
// - target.txt remains.
//
// Variations:
//   - fromTo: LocalFileNFS, FileNFSFileNFS
//   - preserveSymlinks: true, false
func (s *FilesNFSTestSuite) Scenario_SymlinkSync_StaleSymlinkDeleted_WithDeleteDestination(svm *ScenarioVariationManager) {

	fromTo := NamedResolveVariation(svm, map[string]common.FromTo{
		"|fromTo=LocalFileNFS":   common.EFromTo.LocalFileNFS(),
		"|fromTo=FileNFSFileNFS": common.EFromTo.FileNFSFileNFS(),
		"|fromTo=FileNFSLocal":   common.EFromTo.FileNFSLocal(),
	})
	preserveSymlinks := NamedResolveVariation(svm, map[string]bool{
		"|preserveSymlinks=true":  true,
		"|preserveSymlinks=false": false,
	})

	if fromTo.From().IsLocal() && runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	if fromTo.To().IsLocal() && runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupSymLinkSyncContainersForFromTo(svm, fromTo)
	defer cleanupHardlinkSyncForFromTo(svm, fromTo, srcContainer, dstContainer, rootDir)

	targetName := rootDir + "/target.txt"
	staleLinkName := rootDir + "/stale_link.txt"

	// --- Destination: target.txt + stale_link.txt -> target.txt ---
	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	dstTarget := dstContainer.GetObject(svm, targetName, common.EEntityType.File())
	dstTarget.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	setOldLMT(svm, dstTarget, dstContainer)

	dstLink := dstContainer.GetObject(svm, staleLinkName, common.EEntityType.Symlink())
	dstLink.Create(svm, nil, ObjectProperties{
		EntityType:        common.EEntityType.Symlink(),
		SymlinkedFileName: targetName,
	})

	if !svm.Dryrun() {
		time.Sleep(5 * time.Second)
	}

	// --- Source: target.txt only, no symlink ---
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(targetName),
		Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.File(),
		},
	})

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	runSyncForFromTo(svm, srcDirObj, dstDir, fromTo, true, preserveSymlinks)

	// Validate: stale_link.txt deleted, target.txt remains.
	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			targetName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			staleLinkName: ResourceDefinitionObject{
				ObjectShouldExist: pointerTo(false),
			},
		},
	}, ValidateResourceOptions{})
}

// Scenario 2: Stale symlink at destination is not deleted when --delete-destination=false.
// Opposite to Scenario 1.
func (s *FilesNFSTestSuite) Scenario_SymlinkSync_StaleSymlinkSurvives_WhenDeleteDestinationFalse(svm *ScenarioVariationManager) {

	fromTo := NamedResolveVariation(svm, map[string]common.FromTo{
		"|fromTo=LocalFileNFS":   common.EFromTo.LocalFileNFS(),
		"|fromTo=FileNFSFileNFS": common.EFromTo.FileNFSFileNFS(),
		"|fromTo=FileNFSLocal":   common.EFromTo.FileNFSLocal(),
	})

	if fromTo.From().IsLocal() && runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	if fromTo.To().IsLocal() && runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}
	srcContainer, dstContainer, rootDir := setupSymLinkSyncContainersForFromTo(svm, fromTo)
	defer cleanupHardlinkSyncForFromTo(svm, fromTo, srcContainer, dstContainer, rootDir)

	targetName := rootDir + "/target.txt"
	staleLinkName := rootDir + "/stale_link.txt"

	// --- Destination: target.txt + stale_link.txt -> target.txt ---
	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	dstTarget := dstContainer.GetObject(svm, targetName, common.EEntityType.File())
	dstTarget.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})

	setOldLMT(svm, dstTarget, dstContainer)

	dstLink := dstContainer.GetObject(svm, staleLinkName, common.EEntityType.Symlink())
	dstLink.Create(svm, nil, ObjectProperties{
		EntityType:        common.EEntityType.Symlink(),
		SymlinkedFileName: targetName,
	})

	if !svm.Dryrun() {
		time.Sleep(5 * time.Second)
	}

	// --- Source: target.txt only, NO symlink ---
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(targetName),
		Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.File(),
		},
	})
	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	runSyncForFromTo(svm, srcDirObj, dstDir, fromTo, false)

	// Validate: stale_link.txt is not deleted. It MUST still be present.
	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			targetName: ResourceDefinitionObject{
				ObjectProperties:  ObjectProperties{EntityType: common.EEntityType.File()},
				ObjectShouldExist: pointerTo(true),
			},
			staleLinkName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:        common.EEntityType.Symlink(),
					SymlinkedFileName: targetName,
				},
				ObjectShouldExist: pointerTo(true),
			},
		},
	}, ValidateResourceOptions{})
}

// Scenario 3: Symlink present at BOTH source and destination must not be deleted.
//
// Source:
//
//	target.txt
//	link.txt → target.txt
//
// Destination (before sync):
//
//	target.txt
//	link.txt → target.txt
//
// Expected: both files remain at destination after sync --delete-destination=true.
// Validates the destSymlinks does not accidentally classify as Preserve
// symlinks as "extras".
func (s *FilesNFSTestSuite) Scenario_SymlinkSync_SymlinkPresentAtBothEnds_NotDeleted(svm *ScenarioVariationManager) {

	fromTo := NamedResolveVariation(svm, map[string]common.FromTo{
		"|fromTo=LocalFileNFS":   common.EFromTo.LocalFileNFS(),
		"|fromTo=FileNFSFileNFS": common.EFromTo.FileNFSFileNFS(),
		"|fromTo=FileNFSLocal":   common.EFromTo.FileNFSLocal(),
	})

	preserveSymlinks := NamedResolveVariation(svm, map[string]bool{
		"|preserveSymlinks=true":  true,
		"|preserveSymlinks=false": false,
	})

	if fromTo.From().IsLocal() && runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	if fromTo.To().IsLocal() && runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupSymLinkSyncContainersForFromTo(svm, fromTo)

	defer cleanupHardlinkSyncForFromTo(svm, fromTo, srcContainer, dstContainer, rootDir)

	targetName := rootDir + "/target.txt"
	linkName := rootDir + "/link.txt"

	// Destination
	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	dstTarget := dstContainer.GetObject(svm, targetName, common.EEntityType.File())
	dstTarget.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})

	setOldLMT(svm, dstTarget, dstContainer)

	dstLink := dstContainer.GetObject(svm, linkName, common.EEntityType.Symlink())
	dstLink.Create(svm, nil, ObjectProperties{
		EntityType:        common.EEntityType.Symlink(),
		SymlinkedFileName: targetName,
	})

	if !svm.Dryrun() {
		time.Sleep(5 * time.Second)
	}

	// Source
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(targetName),
		Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.File(),
		},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(linkName),
		ObjectProperties: ObjectProperties{
			EntityType:        common.EEntityType.Symlink(),
			SymlinkedFileName: targetName,
		},
	})

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	runSyncForFromTo(svm, srcDirObj, dstDir, fromTo,
		true, preserveSymlinks)

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			targetName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			linkName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:        common.EEntityType.Symlink(),
					SymlinkedFileName: targetName,
				},
			},
		},
	}, ValidateResourceOptions{})
}
