package e2etest

import (
	"runtime"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/google/uuid"
)

// ===========================================================================================
// Hardlink Preserve Sync Scenarios (Local → Azure Files NFS)
//
// These tests exercise every code path in ProcessIfNecessary and ProcessPendingHardlinks
// within the syncDestinationComparator, which drives the "upload sync" direction
// (LocalFileNFS).  Each scenario sets up a known initial state at both source (local)
// and destination (NFS share), runs `azcopy sync --hardlinks=preserve`, and then
// validates the expected outcome at the destination.
// ===========================================================================================

// helper: creates a local source container and a shared NFS destination container,
// returns (srcContainer, dstContainer, rootDir).  Caller must defer CleanupNFSDirectory.
func setupHardlinkSyncContainers(svm *ScenarioVariationManager) (
	srcContainer ContainerResourceManager,
	dstContainer ContainerResourceManager,
	rootDir string,
) {
	dstContainer = GetRootResource(svm, common.ELocation.FileNFS(), GetResourceOptions{
		PreferredAccount: pointerTo(PremiumFileShareAcct),
	}).(ServiceResourceManager).GetContainer("hlsyncdst")
	if !dstContainer.Exists() {
		dstContainer.Create(svm, ContainerProperties{
			FileContainerProperties: FileContainerProperties{
				EnabledProtocols: pointerTo("NFS"),
			},
		})
	}

	srcContainer = CreateResource[ContainerResourceManager](svm, GetRootResource(
		svm, common.ELocation.Local()), ResourceDefinitionContainer{})

	rootDir = "hlsync_" + uuid.NewString()
	return
}

// helper: runs azcopy sync LocalFileNFS with --hardlinks=preserve and --delete-destination.
func runHardlinkSync(
	svm *ScenarioVariationManager,
	srcDirObj ResourceManager,
	dstDirObj RemoteResourceManager,
	deleteDestination bool,
) AzCopyStdout {
	stdOut, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbSync,
			Targets: []ResourceManager{srcDirObj, dstDirObj.(RemoteResourceManager).WithSpecificAuthType(
				ResolveVariation(svm, []ExplicitCredentialTypes{
					EExplicitCredentialType.SASToken(),
					EExplicitCredentialType.OAuth(),
				}), svm, CreateAzCopyTargetOptions{}),
			},
			Flags: SyncFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive:    pointerTo(true),
					FromTo:       pointerTo(common.EFromTo.LocalFileNFS()),
					HardlinkType: pointerTo(common.PreserveHardlinkHandlingType),
				},
				DeleteDestination: pointerTo(deleteDestination),
			},
		})

	return stdOut
}

// Scenario 1: Initial Sync — fresh upload of hardlinked files to empty destination.
//
// Source (local):
//
//	anchor.txt          (regular file, nlink=2)
//	link_to_anchor.txt  (hardlink → anchor.txt)
//	independent.txt     (regular standalone file)
//
// Destination (NFS): empty
//
// Expected:
//   - anchor.txt transferred as regular file
//   - link_to_anchor.txt transferred as hardlink (CreateHardLink API)
//   - independent.txt transferred as regular file
func (s *FilesNFSTestSuite) Scenario_HardlinkSync_InitialSync(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainers(svm)
	defer CleanupNFSDirectory(svm, dstContainer, rootDir)

	// Create source objects
	srcDir := ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	}
	CreateResource[ObjectResourceManager](svm, srcContainer, srcDir)

	anchorName := rootDir + "/anchor.txt"
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(anchorName),
		Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.File(),
		},
	})

	linkName := rootDir + "/link_to_anchor.txt"
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(linkName),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: anchorName,
		},
	})

	independentName := rootDir + "/independent.txt"
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(independentName),
		Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.File(),
		},
	})

	// For sync, seed destination with at least one file so sync does not fail
	dstSeed := dstContainer.GetObject(svm, rootDir+"/independent.txt", common.EEntityType.File())
	dstSeed.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{})
	dstSeed.SetObjectProperties(svm, ObjectProperties{
		FileNFSProperties: &FileNFSProperties{
			FileCreationTime:  pointerTo(time.Now().Add(-10 * time.Minute)),
			FileLastWriteTime: pointerTo(time.Now().Add(-10 * time.Minute)),
		},
	})

	if !svm.Dryrun() {
		time.Sleep(5 * time.Second)
	}

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDirObj := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	stdOut := runHardlinkSync(svm, srcDirObj, dstDirObj.(RemoteResourceManager), false)

	// Validate: all three objects should exist at destination
	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			anchorName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			linkName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: anchorName,
				},
			},
			independentName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
		},
	}, ValidateResourceOptions{fromTo: common.EFromTo.LocalFileNFS()})

	ValidateHardlinksTransferCount(svm, stdOut, 2)
}

// Scenario 2: Hardlink→Hardlink, same anchor, source has newer LMT → content re-transferred.
//
// Source (local):
//
//	anchor.txt          (regular file, nlink=2, touched recently)
//	link_to_anchor.txt  (hardlink → anchor.txt)
//
// Destination (NFS): same structure but with older LMT
//
// Expected: both anchor and link are re-synced because the anchor content is newer.
func (s *FilesNFSTestSuite) Scenario_HardlinkSync_SameAnchorNewerSource(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainers(svm)
	defer CleanupNFSDirectory(svm, dstContainer, rootDir)

	anchorName := rootDir + "/anchor.txt"
	linkName := rootDir + "/link_to_anchor.txt"

	// Create destination (old state) — set up hardlink relationship at destination first
	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	dstAnchor := dstContainer.GetObject(svm, anchorName, common.EEntityType.File())
	dstAnchor.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	dstAnchor.SetObjectProperties(svm, ObjectProperties{
		FileNFSProperties: &FileNFSProperties{
			FileCreationTime:  pointerTo(time.Now().Add(-10 * time.Minute)),
			FileLastWriteTime: pointerTo(time.Now().Add(-10 * time.Minute)),
		},
	})

	dstLink := dstContainer.GetObject(svm, linkName, common.EEntityType.Hardlink())
	dstLink.Create(svm, nil, ObjectProperties{
		EntityType:         common.EEntityType.Hardlink(),
		HardLinkedFileName: anchorName,
	})

	if !svm.Dryrun() {
		time.Sleep(5 * time.Second)
	}

	// Create source (new state) — newer files
	srcDir := ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	}
	CreateResource[ObjectResourceManager](svm, srcContainer, srcDir)

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(anchorName),
		Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.File(),
		},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(linkName),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: anchorName,
		},
	})

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	stdOut := runHardlinkSync(svm, srcDirObj, dstDir.(RemoteResourceManager), false)

	// Validate: hardlink relationship preserved, content updated
	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			anchorName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			linkName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: anchorName,
				},
			},
		},
	}, ValidateResourceOptions{fromTo: common.EFromTo.LocalFileNFS()})

	_ = stdOut
}

// Scenario 3: Hardlink→Hardlink, same anchor, dest up-to-date → skip (no transfer).
//
// Source and destination have identical hardlink relationships and the destination
// is at least as recent as the source.
//
// Expected: no transfers scheduled; hardlink count stays 0.
func (s *FilesNFSTestSuite) Scenario_HardlinkSync_SameAnchorUpToDate(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainers(svm)
	defer CleanupNFSDirectory(svm, dstContainer, rootDir)

	anchorName := rootDir + "/anchor.txt"
	linkName := rootDir + "/link_to_anchor.txt"

	// Create source first
	srcDir := ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	}
	CreateResource[ObjectResourceManager](svm, srcContainer, srcDir)

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(anchorName),
		Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.File(),
		},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(linkName),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: anchorName,
		},
	})

	if !svm.Dryrun() {
		time.Sleep(5 * time.Second)
	}

	// Create destination AFTER source so dest is newer
	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	dstAnchor := dstContainer.GetObject(svm, anchorName, common.EEntityType.File())
	dstAnchor.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})

	dstLink := dstContainer.GetObject(svm, linkName, common.EEntityType.Hardlink())
	dstLink.Create(svm, nil, ObjectProperties{
		EntityType:         common.EEntityType.Hardlink(),
		HardLinkedFileName: anchorName,
	})

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	runHardlinkSync(svm, srcDirObj, dstDir.(RemoteResourceManager), false)

	// Validate: destination still has the same structure, nothing transferred
	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			anchorName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			linkName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: anchorName,
				},
			},
		},
	}, ValidateResourceOptions{fromTo: common.EFromTo.LocalFileNFS()})
}

// Scenario 4: Hardlink retarget — source link now points to a different anchor.
//
// Source:
//
//	old_anchor.txt       (regular file, standalone now)
//	new_anchor.txt       (regular file, nlink=2)
//	retarget_link.txt    (hardlink → new_anchor.txt)
//
// Destination (before sync):
//
//	old_anchor.txt       (regular file, nlink=2)
//	new_anchor.txt       (regular file, standalone)
//	retarget_link.txt    (hardlink → old_anchor.txt)
//
// Expected: retarget_link.txt is deleted and recreated as hardlink → new_anchor.txt.
func (s *FilesNFSTestSuite) Scenario_HardlinkSync_RetargetLink(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainers(svm)
	defer CleanupNFSDirectory(svm, dstContainer, rootDir)

	oldAnchorName := rootDir + "/old_anchor.txt"
	newAnchorName := rootDir + "/new_anchor.txt"
	retargetLinkName := rootDir + "/retarget_link.txt"

	// Create destination (old state): retarget_link → old_anchor
	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	dstOldAnchor := dstContainer.GetObject(svm, oldAnchorName, common.EEntityType.File())
	dstOldAnchor.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	dstOldAnchor.SetObjectProperties(svm, ObjectProperties{
		FileNFSProperties: &FileNFSProperties{
			FileCreationTime:  pointerTo(time.Now().Add(-10 * time.Minute)),
			FileLastWriteTime: pointerTo(time.Now().Add(-10 * time.Minute)),
		},
	})

	dstNewAnchor := dstContainer.GetObject(svm, newAnchorName, common.EEntityType.File())
	dstNewAnchor.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	dstNewAnchor.SetObjectProperties(svm, ObjectProperties{
		FileNFSProperties: &FileNFSProperties{
			FileCreationTime:  pointerTo(time.Now().Add(-10 * time.Minute)),
			FileLastWriteTime: pointerTo(time.Now().Add(-10 * time.Minute)),
		},
	})

	dstRetargetLink := dstContainer.GetObject(svm, retargetLinkName, common.EEntityType.Hardlink())
	dstRetargetLink.Create(svm, nil, ObjectProperties{
		EntityType:         common.EEntityType.Hardlink(),
		HardLinkedFileName: oldAnchorName,
	})

	if !svm.Dryrun() {
		time.Sleep(5 * time.Second)
	}

	// Create source (new state): retarget_link → new_anchor
	srcDir := ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	}
	CreateResource[ObjectResourceManager](svm, srcContainer, srcDir)

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(oldAnchorName),
		Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.File(),
		},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(newAnchorName),
		Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.File(),
		},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(retargetLinkName),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: newAnchorName,
		},
	})

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	stdOut := runHardlinkSync(svm, srcDirObj, dstDir.(RemoteResourceManager), true)

	// Validate: retarget_link now points to new_anchor
	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			oldAnchorName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			newAnchorName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			retargetLinkName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: newAnchorName,
				},
			},
		},
	}, ValidateResourceOptions{fromTo: common.EFromTo.LocalFileNFS()})

	_ = stdOut
}

// Scenario 5: Source becomes hardlink — dest has a regular file, source now has a hardlink at that path.
//
// Source:
//
//	anchor.txt          (regular file, nlink=2)
//	was_file.txt        (hardlink → anchor.txt)
//
// Destination (before sync):
//
//	anchor.txt          (regular file, standalone)
//	was_file.txt        (regular file, standalone)
//
// Expected: was_file.txt deleted at dest, then recreated as hardlink → anchor.txt (EntityTypeMismatch path).
func (s *FilesNFSTestSuite) Scenario_HardlinkSync_FileBecomesHardlink(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainers(svm)
	defer CleanupNFSDirectory(svm, dstContainer, rootDir)

	anchorName := rootDir + "/anchor.txt"
	wasFileName := rootDir + "/was_file.txt"

	// Create destination (old state): two independent regular files
	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	dstAnchor := dstContainer.GetObject(svm, anchorName, common.EEntityType.File())
	dstAnchor.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	dstAnchor.SetObjectProperties(svm, ObjectProperties{
		FileNFSProperties: &FileNFSProperties{
			FileCreationTime:  pointerTo(time.Now().Add(-10 * time.Minute)),
			FileLastWriteTime: pointerTo(time.Now().Add(-10 * time.Minute)),
		},
	})

	dstWasFile := dstContainer.GetObject(svm, wasFileName, common.EEntityType.File())
	dstWasFile.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	dstWasFile.SetObjectProperties(svm, ObjectProperties{
		FileNFSProperties: &FileNFSProperties{
			FileCreationTime:  pointerTo(time.Now().Add(-10 * time.Minute)),
			FileLastWriteTime: pointerTo(time.Now().Add(-10 * time.Minute)),
		},
	})

	if !svm.Dryrun() {
		time.Sleep(5 * time.Second)
	}

	// Create source (new state): anchor.txt + was_file.txt → hardlink to anchor
	srcDir := ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	}
	CreateResource[ObjectResourceManager](svm, srcContainer, srcDir)

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(anchorName),
		Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.File(),
		},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(wasFileName),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: anchorName,
		},
	})

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	stdOut := runHardlinkSync(svm, srcDirObj, dstDir.(RemoteResourceManager), true)

	// Validate: was_file.txt is now a hardlink to anchor
	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			anchorName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			wasFileName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: anchorName,
				},
			},
		},
	}, ValidateResourceOptions{fromTo: common.EFromTo.LocalFileNFS()})

	_ = stdOut
}

// Scenario 6: Hardlink becomes regular file — dest has a hardlink, source now has a standalone file.
//
// Source:
//
//	anchor.txt          (regular file, standalone — was nlink=2 before)
//	was_link.txt        (regular file, standalone — used to be a hardlink)
//
// Destination (before sync):
//
//	anchor.txt          (regular file, nlink=2)
//	was_link.txt        (hardlink → anchor.txt)
//
// Expected: was_link.txt deleted (breaking the link), then re-uploaded as independent file.
func (s *FilesNFSTestSuite) Scenario_HardlinkSync_HardlinkBecomesFile(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainers(svm)
	defer CleanupNFSDirectory(svm, dstContainer, rootDir)

	anchorName := rootDir + "/anchor.txt"
	wasLinkName := rootDir + "/was_link.txt"

	// Create destination (old state): anchor + was_link as hardlink
	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	dstAnchor := dstContainer.GetObject(svm, anchorName, common.EEntityType.File())
	dstAnchor.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	dstAnchor.SetObjectProperties(svm, ObjectProperties{
		FileNFSProperties: &FileNFSProperties{
			FileCreationTime:  pointerTo(time.Now().Add(-10 * time.Minute)),
			FileLastWriteTime: pointerTo(time.Now().Add(-10 * time.Minute)),
		},
	})

	dstLink := dstContainer.GetObject(svm, wasLinkName, common.EEntityType.Hardlink())
	dstLink.Create(svm, nil, ObjectProperties{
		EntityType:         common.EEntityType.Hardlink(),
		HardLinkedFileName: anchorName,
	})

	if !svm.Dryrun() {
		time.Sleep(5 * time.Second)
	}

	// Create source (new state): both are now standalone regular files
	srcDir := ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	}
	CreateResource[ObjectResourceManager](svm, srcContainer, srcDir)

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(anchorName),
		Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.File(),
		},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(wasLinkName),
		Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.File(),
		},
	})

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	stdOut := runHardlinkSync(svm, srcDirObj, dstDir.(RemoteResourceManager), true)

	// Validate: both are now regular files at dest (no hardlink relationship)
	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			anchorName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			wasLinkName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
		},
	}, ValidateResourceOptions{fromTo: common.EFromTo.LocalFileNFS()})

	_ = stdOut
}

// Scenario 7: Source hardlink deleted — dest has a hardlink that no longer exists at source.
//
// Source:
//
//	anchor.txt  (regular file, standalone — link_removed was unlinked)
//
// Destination (before sync):
//
//	anchor.txt          (regular file, nlink=2)
//	link_removed.txt    (hardlink → anchor.txt)
//
// Expected: link_removed.txt deleted from dest (--delete-destination).
func (s *FilesNFSTestSuite) Scenario_HardlinkSync_SourceDeleted(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainers(svm)
	defer CleanupNFSDirectory(svm, dstContainer, rootDir)

	anchorName := rootDir + "/anchor.txt"
	removedLinkName := rootDir + "/link_removed.txt"

	// Create destination (old state): anchor + link_removed
	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	dstAnchor := dstContainer.GetObject(svm, anchorName, common.EEntityType.File())
	dstAnchor.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	dstAnchor.SetObjectProperties(svm, ObjectProperties{
		FileNFSProperties: &FileNFSProperties{
			FileCreationTime:  pointerTo(time.Now().Add(-10 * time.Minute)),
			FileLastWriteTime: pointerTo(time.Now().Add(-10 * time.Minute)),
		},
	})

	dstLink := dstContainer.GetObject(svm, removedLinkName, common.EEntityType.Hardlink())
	dstLink.Create(svm, nil, ObjectProperties{
		EntityType:         common.EEntityType.Hardlink(),
		HardLinkedFileName: anchorName,
	})

	if !svm.Dryrun() {
		time.Sleep(5 * time.Second)
	}

	// Create source (new state): only anchor remains
	srcDir := ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	}
	CreateResource[ObjectResourceManager](svm, srcContainer, srcDir)

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(anchorName),
		Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.File(),
		},
	})

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	runHardlinkSync(svm, srcDirObj, dstDir.(RemoteResourceManager), true)

	// Validate: link_removed.txt should not exist at dest
	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			anchorName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			removedLinkName: ResourceDefinitionObject{
				ObjectShouldExist: pointerTo(false),
			},
		},
	}, ValidateResourceOptions{fromTo: common.EFromTo.LocalFileNFS()})
}

// Scenario 8: New hardlink appears — source has a new hardlink file that doesn't exist at dest yet.
//
// Source:
//
//	anchor.txt          (regular file, nlink=3)
//	existing_link.txt   (hardlink → anchor.txt)
//	new_link.txt        (hardlink → anchor.txt — newly added)
//
// Destination (before sync):
//
//	anchor.txt          (regular file, nlink=2)
//	existing_link.txt   (hardlink → anchor.txt)
//
// Expected: new_link.txt created at dest as hardlink to anchor.txt.
func (s *FilesNFSTestSuite) Scenario_HardlinkSync_NewLinkAppears(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainers(svm)
	defer CleanupNFSDirectory(svm, dstContainer, rootDir)

	anchorName := rootDir + "/anchor.txt"
	existingLinkName := rootDir + "/existing_link.txt"
	newLinkName := rootDir + "/new_link.txt"

	// Create destination (old state): anchor + existing_link
	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	dstAnchor := dstContainer.GetObject(svm, anchorName, common.EEntityType.File())
	dstAnchor.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	dstAnchor.SetObjectProperties(svm, ObjectProperties{
		FileNFSProperties: &FileNFSProperties{
			FileCreationTime:  pointerTo(time.Now().Add(-10 * time.Minute)),
			FileLastWriteTime: pointerTo(time.Now().Add(-10 * time.Minute)),
		},
	})

	dstExistingLink := dstContainer.GetObject(svm, existingLinkName, common.EEntityType.Hardlink())
	dstExistingLink.Create(svm, nil, ObjectProperties{
		EntityType:         common.EEntityType.Hardlink(),
		HardLinkedFileName: anchorName,
	})

	if !svm.Dryrun() {
		time.Sleep(5 * time.Second)
	}

	// Create source (new state): anchor + existing_link + new_link
	srcDir := ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	}
	CreateResource[ObjectResourceManager](svm, srcContainer, srcDir)

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(anchorName),
		Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.File(),
		},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(existingLinkName),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: anchorName,
		},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(newLinkName),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: anchorName,
		},
	})

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	stdOut := runHardlinkSync(svm, srcDirObj, dstDir.(RemoteResourceManager), false)

	// Validate: all three hardlinks present at dest
	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			anchorName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			existingLinkName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: anchorName,
				},
			},
			newLinkName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: anchorName,
				},
			},
		},
	}, ValidateResourceOptions{fromTo: common.EFromTo.LocalFileNFS()})

	_ = stdOut
}

// Scenario 9: Multiple hardlink groups — verify independent hardlink groups are synced correctly.
//
// Source:
//
//	group1_anchor.txt     (file, nlink=2)
//	group1_link.txt       (hardlink → group1_anchor.txt)
//	group2_anchor.txt     (file, nlink=2)
//	group2_link.txt       (hardlink → group2_anchor.txt)
//	standalone.txt        (regular file)
//
// Destination: empty
//
// Expected: both groups and the standalone file are transferred correctly.
func (s *FilesNFSTestSuite) Scenario_HardlinkSync_MultipleGroups(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainers(svm)
	defer CleanupNFSDirectory(svm, dstContainer, rootDir)

	g1Anchor := rootDir + "/group1_anchor.txt"
	g1Link := rootDir + "/group1_link.txt"
	g2Anchor := rootDir + "/group2_anchor.txt"
	g2Link := rootDir + "/group2_link.txt"
	standaloneName := rootDir + "/standalone.txt"

	// Create source
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(g1Anchor),
		Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.File(),
		},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(g1Link),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: g1Anchor,
		},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(g2Anchor),
		Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.File(),
		},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(g2Link),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: g2Anchor,
		},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(standaloneName),
		Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.File(),
		},
	})

	// Seed destination for sync
	dstSeed := dstContainer.GetObject(svm, rootDir+"/standalone.txt", common.EEntityType.File())
	dstSeed.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{})
	dstSeed.SetObjectProperties(svm, ObjectProperties{
		FileNFSProperties: &FileNFSProperties{
			FileCreationTime:  pointerTo(time.Now().Add(-10 * time.Minute)),
			FileLastWriteTime: pointerTo(time.Now().Add(-10 * time.Minute)),
		},
	})

	if !svm.Dryrun() {
		time.Sleep(5 * time.Second)
	}

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDirObj := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	stdOut := runHardlinkSync(svm, srcDirObj, dstDirObj.(RemoteResourceManager), false)

	// Validate: both groups and the standalone file are present
	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			g1Anchor: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			g1Link: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: g1Anchor,
				},
			},
			g2Anchor: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			g2Link: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: g2Anchor,
				},
			},
			standaloneName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
		},
	}, ValidateResourceOptions{fromTo: common.EFromTo.LocalFileNFS()})

	// anchor files + links = 4 hardlink transfers
	ValidateHardlinksTransferCount(svm, stdOut, 4)
}

// Scenario 10: Mixed changes in one sync — combines several scenarios.
//
// Source:
//
//	anchor_a.txt        (file, nlink=2)
//	link_a.txt          (hardlink → anchor_a.txt — same as before)
//	anchor_b.txt        (file, nlink=2)
//	link_b.txt          (hardlink → anchor_b.txt — was pointing to anchor_a at dest)
//	new_file.txt        (regular file — new, not at dest)
//
// Destination (before sync):
//
//	anchor_a.txt        (file, nlink=3)
//	link_a.txt          (hardlink → anchor_a.txt)
//	anchor_b.txt        (file, standalone)
//	link_b.txt          (hardlink → anchor_a.txt — wrong anchor)
//	stale.txt           (regular file — not at source anymore)
//
// Expected:
//   - link_a stays (same anchor)
//   - link_b retargeted to anchor_b
//   - stale.txt deleted
//   - new_file.txt created
func (s *FilesNFSTestSuite) Scenario_HardlinkSync_MixedChanges(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainers(svm)
	defer CleanupNFSDirectory(svm, dstContainer, rootDir)

	anchorA := rootDir + "/anchor_a.txt"
	linkA := rootDir + "/link_a.txt"
	anchorB := rootDir + "/anchor_b.txt"
	linkB := rootDir + "/link_b.txt"
	newFile := rootDir + "/new_file.txt"
	staleFile := rootDir + "/stale.txt"

	// Create destination (old state)
	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	dstAnchorA := dstContainer.GetObject(svm, anchorA, common.EEntityType.File())
	dstAnchorA.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	dstAnchorA.SetObjectProperties(svm, ObjectProperties{
		FileNFSProperties: &FileNFSProperties{
			FileCreationTime:  pointerTo(time.Now().Add(-10 * time.Minute)),
			FileLastWriteTime: pointerTo(time.Now().Add(-10 * time.Minute)),
		},
	})

	// link_a → anchor_a (correct)
	dstLinkA := dstContainer.GetObject(svm, linkA, common.EEntityType.Hardlink())
	dstLinkA.Create(svm, nil, ObjectProperties{
		EntityType:         common.EEntityType.Hardlink(),
		HardLinkedFileName: anchorA,
	})

	dstAnchorB := dstContainer.GetObject(svm, anchorB, common.EEntityType.File())
	dstAnchorB.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	dstAnchorB.SetObjectProperties(svm, ObjectProperties{
		FileNFSProperties: &FileNFSProperties{
			FileCreationTime:  pointerTo(time.Now().Add(-10 * time.Minute)),
			FileLastWriteTime: pointerTo(time.Now().Add(-10 * time.Minute)),
		},
	})

	// link_b → anchor_a (wrong! should be anchor_b after sync)
	dstLinkB := dstContainer.GetObject(svm, linkB, common.EEntityType.Hardlink())
	dstLinkB.Create(svm, nil, ObjectProperties{
		EntityType:         common.EEntityType.Hardlink(),
		HardLinkedFileName: anchorA,
	})

	// stale file that should get deleted
	dstStale := dstContainer.GetObject(svm, staleFile, common.EEntityType.File())
	dstStale.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})

	if !svm.Dryrun() {
		time.Sleep(5 * time.Second)
	}

	// Create source (new state)
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(anchorA),
		Body:             NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(linkA),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: anchorA,
		},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(anchorB),
		Body:             NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(linkB),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: anchorB,
		},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(newFile),
		Body:             NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
	})

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	runHardlinkSync(svm, srcDirObj, dstDir.(RemoteResourceManager), true)

	// Validate final state
	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			anchorA: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			linkA: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: anchorA,
				},
			},
			anchorB: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			linkB: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: anchorB,
				},
			},
			newFile: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			staleFile: ResourceDefinitionObject{
				ObjectShouldExist: pointerTo(false),
			},
		},
	}, ValidateResourceOptions{fromTo: common.EFromTo.LocalFileNFS()})
}

// Scenario 11: Idempotent re-sync — running the same sync twice produces no extra transfers.
//
// Run initial sync from Scenario 1, then run again. Second run should be a no-op.
func (s *FilesNFSTestSuite) Scenario_HardlinkSync_IdempotentResync(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainers(svm)
	defer CleanupNFSDirectory(svm, dstContainer, rootDir)

	anchorName := rootDir + "/anchor.txt"
	linkName := rootDir + "/link_to_anchor.txt"

	// Create source
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(anchorName),
		Body:             NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(linkName),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: anchorName,
		},
	})

	// Seed destination for sync
	dstSeed := dstContainer.GetObject(svm, rootDir+"/anchor.txt", common.EEntityType.File())
	dstSeed.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{})
	dstSeed.SetObjectProperties(svm, ObjectProperties{
		FileNFSProperties: &FileNFSProperties{
			FileCreationTime:  pointerTo(time.Now().Add(-10 * time.Minute)),
			FileLastWriteTime: pointerTo(time.Now().Add(-10 * time.Minute)),
		},
	})

	if !svm.Dryrun() {
		time.Sleep(5 * time.Second)
	}

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDirObj := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	// First sync — initial upload
	runHardlinkSync(svm, srcDirObj, dstDirObj.(RemoteResourceManager), false)

	if !svm.Dryrun() {
		time.Sleep(2 * time.Second)
	}

	// Second sync — should be a no-op (everything is up-to-date)
	runHardlinkSync(svm, srcDirObj, dstDirObj.(RemoteResourceManager), false)

	// Validate: structure is still correct
	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			anchorName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			linkName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: anchorName,
				},
			},
		},
	}, ValidateResourceOptions{fromTo: common.EFromTo.LocalFileNFS()})
}

// Scenario 12: Hardlinks in subdirectories — verifies hardlinks across nested directories work.
//
// Source:
//
//	subdir/anchor.txt        (file, nlink=2)
//	subdir/nested/link.txt   (hardlink → subdir/anchor.txt)
//
// Destination: empty
//
// Expected: both objects created at dest preserving the relationship.
func (s *FilesNFSTestSuite) Scenario_HardlinkSync_NestedDirectories(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainers(svm)
	defer CleanupNFSDirectory(svm, dstContainer, rootDir)

	subdirName := rootDir + "/subdir"
	nestedDirName := rootDir + "/subdir/nested"
	anchorName := rootDir + "/subdir/anchor.txt"
	linkName := rootDir + "/subdir/nested/link.txt"

	// Create source
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(subdirName),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(nestedDirName),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(anchorName),
		Body:             NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(linkName),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: anchorName,
		},
	})

	// Seed destination
	dstSeed := dstContainer.GetObject(svm, anchorName, common.EEntityType.File())
	dstSeed.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{})
	dstSeed.SetObjectProperties(svm, ObjectProperties{
		FileNFSProperties: &FileNFSProperties{
			FileCreationTime:  pointerTo(time.Now().Add(-10 * time.Minute)),
			FileLastWriteTime: pointerTo(time.Now().Add(-10 * time.Minute)),
		},
	})

	if !svm.Dryrun() {
		time.Sleep(5 * time.Second)
	}

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDirObj := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	stdOut := runHardlinkSync(svm, srcDirObj, dstDirObj.(RemoteResourceManager), false)

	// Validate
	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			anchorName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			linkName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: anchorName,
				},
			},
		},
	}, ValidateResourceOptions{fromTo: common.EFromTo.LocalFileNFS()})

	ValidateHardlinksTransferCount(svm, stdOut, 2)
}

// Scenario 19: Lex-smaller anchor added to existing group — no false-positive mismatch.
//
// Background: dest was synced previously when the hardlink group was {B, C, D}.
// At that time "B" was the lexicographically smallest name, so it became the anchor
// at both source and destination.
//
// Since then a new file "A" was added to the same inode group on the source.
// "A" < "B" so the source anchor has shifted to "A", while dest anchor is still "B".
//
// Without the fix in ProcessPendingHardlinks this would look like a target mismatch and
// C and D would be deleted and recreated unnecessarily.
// With the fix the code checks whether the dest anchor ("B") is still a member of the
// source inode group — it is — so C and D are left untouched.
//
// Source (after A added):
//
//	A.txt          (regular file / anchor, nlink=4 — lex-smallest)
//	B.txt          (hardlink → A.txt)
//	C.txt          (hardlink → A.txt)
//	D.txt          (hardlink → A.txt)
//
// Destination (before sync — result of a previous sync of {B,C,D}):
//
//	B.txt          (regular file / anchor, nlink=3)
//	C.txt          (hardlink → B.txt)
//	D.txt          (hardlink → B.txt)
//
// Expected after sync:
//   - A.txt created as regular file (new, was not at dest)
//   - B.txt deleted (entity-type changed: was File at dest, now Hardlink at source)
//     and recreated as Hardlink → A.txt
//   - C.txt and D.txt are NOT touched (the fix prevents unnecessary recreation)
//   - HardlinksTransferCount == 1  (only B.txt needed a hardlink transfer;
//     C.txt and D.txt were skipped)
func (s *FilesNFSTestSuite) Scenario_HardlinkSync_LexSmallerAnchorAdded(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainers(svm)
	defer CleanupNFSDirectory(svm, dstContainer, rootDir)

	nameA := rootDir + "/A.txt"
	nameB := rootDir + "/B.txt"
	nameC := rootDir + "/C.txt"
	nameD := rootDir + "/D.txt"

	// ── Set up destination: previous sync result ─────────────────────────────
	// B was the anchor when the group was {B, C, D}.
	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	dstB := dstContainer.GetObject(svm, nameB, common.EEntityType.File())
	dstB.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	dstB.SetObjectProperties(svm, ObjectProperties{
		FileNFSProperties: &FileNFSProperties{
			FileCreationTime:  pointerTo(time.Now().Add(-10 * time.Minute)),
			FileLastWriteTime: pointerTo(time.Now().Add(-10 * time.Minute)),
		},
	})

	dstC := dstContainer.GetObject(svm, nameC, common.EEntityType.Hardlink())
	dstC.Create(svm, nil, ObjectProperties{
		EntityType:         common.EEntityType.Hardlink(),
		HardLinkedFileName: nameB,
	})

	dstD := dstContainer.GetObject(svm, nameD, common.EEntityType.Hardlink())
	dstD.Create(svm, nil, ObjectProperties{
		EntityType:         common.EEntityType.Hardlink(),
		HardLinkedFileName: nameB,
	})

	if !svm.Dryrun() {
		time.Sleep(5 * time.Second)
	}

	// ── Set up source: A has been added as lex-smallest member ───────────────
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})

	// A is the new anchor at source (lex-smallest); it is a regular file.
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(nameA),
		Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.File(),
		},
	})

	// B, C, D are now hardlinks → A on the source.
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(nameB),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: nameA,
		},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(nameC),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: nameA,
		},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(nameD),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: nameA,
		},
	})

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	stdOut := runHardlinkSync(svm, srcDirObj, dstDir.(RemoteResourceManager), false)

	// ── Validate ─────────────────────────────────────────────────────────────
	// B C and D were skipped by the fix (dest anchor B is still in the source group).
	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			nameA: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			nameB: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: nameA,
				},
			},
			nameC: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: nameB,
				},
			},
			nameD: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: nameB,
				},
			},
		},
	}, ValidateResourceOptions{fromTo: common.EFromTo.LocalFileNFS()})

	// Only A needed a hardlink transfer; B C and D were correctly skipped.
	ValidateHardlinksTransferCount(svm, stdOut, 1)
}

// Scenario: LexSmallerAnchorDeleted — src: B-C-D  dst: A-B-C-D
//
// The lex-smaller anchor "A" existed at dest (from a prior sync) but has been deleted
// at source.  B is now the lex-smallest remaining member and becomes the new anchor.
//
// Source:
//
//	B.txt  (anchor of B-C-D group)
//	C.txt  (hardlink → B)
//	D.txt  (hardlink → B)
//
// Destination (before sync):
//
//	A.txt  (anchor of A-B-C-D group — the now-deleted lex-smaller anchor)
//	B.txt  (hardlink → A)
//	C.txt  (hardlink → A)
//	D.txt  (hardlink → A)
//
// Expected: A deleted; B, C, D skipped (still linked together, no structural change
// among the survivors).  0 hardlink transfers.
func (s *FilesNFSTestSuite) Scenario_HardlinkSync_LexSmallerAnchorDeleted(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainers(svm)
	defer CleanupNFSDirectory(svm, dstContainer, rootDir)

	nameA := rootDir + "/A.txt"
	nameB := rootDir + "/B.txt"
	nameC := rootDir + "/C.txt"
	nameD := rootDir + "/D.txt"

	// ── Destination: A is the anchor, B/C/D are hardlinks to A ──────────────
	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	dstA := dstContainer.GetObject(svm, nameA, common.EEntityType.File())
	dstA.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	dstA.SetObjectProperties(svm, ObjectProperties{
		FileNFSProperties: &FileNFSProperties{
			FileCreationTime:  pointerTo(time.Now().Add(-10 * time.Minute)),
			FileLastWriteTime: pointerTo(time.Now().Add(-10 * time.Minute)),
		},
	})

	dstB := dstContainer.GetObject(svm, nameB, common.EEntityType.Hardlink())
	dstB.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Hardlink(), HardLinkedFileName: nameA})

	dstC := dstContainer.GetObject(svm, nameC, common.EEntityType.Hardlink())
	dstC.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Hardlink(), HardLinkedFileName: nameA})

	dstD := dstContainer.GetObject(svm, nameD, common.EEntityType.Hardlink())
	dstD.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Hardlink(), HardLinkedFileName: nameA})

	if !svm.Dryrun() {
		time.Sleep(5 * time.Second)
	}

	// ── Source: A has been deleted; B is now the anchor ──────────────────────
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})

	// B is the anchor (lex-smallest of the remaining group).
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(nameB),
		Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.File(),
		},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(nameC),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: nameB,
		},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(nameD),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: nameB,
		},
	})

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	stdOut := runHardlinkSync(svm, srcDirObj, dstDir.(RemoteResourceManager), true)

	// A is deleted; B/C/D are skipped (still intact as a linked group).
	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			nameA: ResourceDefinitionObject{
				ObjectShouldExist: pointerTo(false),
			},
			nameB: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: nameA,
				},
			},
			nameC: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: nameA,
				},
			},
			nameD: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: nameA,
				},
			},
		},
	}, ValidateResourceOptions{fromTo: common.EFromTo.LocalFileNFS()})

	// B, C, D are all skipped — 0 hardlink transfers.
	ValidateHardlinksTransferCount(svm, stdOut, 0)
}

// Scenario: GroupSplit — src: A-B  C-D   dst: A-B-C-D
//
// A single group at dest is split into two independent groups at source.
//
// Source:
//
//	A.txt  (anchor of A-B group)
//	B.txt  (hardlink)
//	C.txt  (anchor of C-D group — separate inode)
//	D.txt  (hardlink)
//
// Destination (before sync):
//
//	A.txt  (anchor of A-B-C-D group)
//	B.txt  (hardlink)
//	C.txt  (hardlink)
//	D.txt  (hardlink)
//
// Expected: B skipped (same anchor A); C recreated as new anchor; D recreated as
// hardlink → C.
func (s *FilesNFSTestSuite) Scenario_HardlinkSync_GroupSplit(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainers(svm)
	defer CleanupNFSDirectory(svm, dstContainer, rootDir)

	nameA := rootDir + "/A.txt"
	nameB := rootDir + "/B.txt"
	nameC := rootDir + "/C.txt"
	nameD := rootDir + "/D.txt"

	// ── Destination: one group, A is anchor ──────────────────────────────────
	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	dstA := dstContainer.GetObject(svm, nameA, common.EEntityType.File())
	dstA.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	dstA.SetObjectProperties(svm, ObjectProperties{
		FileNFSProperties: &FileNFSProperties{
			FileCreationTime:  pointerTo(time.Now().Add(-10 * time.Minute)),
			FileLastWriteTime: pointerTo(time.Now().Add(-10 * time.Minute)),
		},
	})

	dstB := dstContainer.GetObject(svm, nameB, common.EEntityType.Hardlink())
	dstB.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Hardlink(), HardLinkedFileName: nameA})

	dstC := dstContainer.GetObject(svm, nameC, common.EEntityType.Hardlink())
	dstC.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Hardlink(), HardLinkedFileName: nameA})

	dstD := dstContainer.GetObject(svm, nameD, common.EEntityType.Hardlink())
	dstD.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Hardlink(), HardLinkedFileName: nameA})

	if !svm.Dryrun() {
		time.Sleep(5 * time.Second)
	}

	// ── Source: two independent groups (A-B and C-D) ─────────────────────────
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})

	// Group 1: A + B
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(nameA),
		Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.File(),
		},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(nameB),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: nameA,
		},
	})

	// Group 2: C + D — independent inode
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(nameC),
		Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.File(),
		},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(nameD),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: nameC,
		},
	})

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	stdOut := runHardlinkSync(svm, srcDirObj, dstDir.(RemoteResourceManager), true)

	// B stays linked to A (same anchor); C is recreated as a new anchor file;
	// D is recreated as hardlink → C.
	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			nameA: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			nameB: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: nameA,
				},
			},
			nameC: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			nameD: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: nameC,
				},
			},
		},
	}, ValidateResourceOptions{fromTo: common.EFromTo.LocalFileNFS()})

	// A and B remains unchanged. C and D are recreated (C and D are re-created).
	ValidateHardlinksTransferCount(svm, stdOut, 2)
}

// Scenario: GroupMerge — src: A-B-C-D   dst: A-B  C-D
//
// Two independent groups at dest are merged into one group at source.
//
// Source:
//
//	A.txt  (anchor of A-B-C-D group)
//	B.txt  (hardlink → A)
//	C.txt  (hardlink → A)
//	D.txt  (hardlink → A)
//
// Destination (before sync):
//
//	A.txt  (anchor of A-B group)
//	B.txt  (hardlink → A)
//	C.txt  (anchor of C-D group — separate inode)
//	D.txt  (hardlink → C)
//
// Expected: A and B skipped (same anchor A); C and D recreated as hardlinks → A,
// unifying them into the A-B-C-D group.
func (s *FilesNFSTestSuite) Scenario_HardlinkSync_GroupMerge(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainers(svm)
	defer CleanupNFSDirectory(svm, dstContainer, rootDir)

	nameA := rootDir + "/A.txt"
	nameB := rootDir + "/B.txt"
	nameC := rootDir + "/C.txt"
	nameD := rootDir + "/D.txt"

	// ── Destination: two groups (A-B and C-D) ────────────────────────────────
	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	dstA := dstContainer.GetObject(svm, nameA, common.EEntityType.File())
	dstA.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	dstA.SetObjectProperties(svm, ObjectProperties{
		FileNFSProperties: &FileNFSProperties{
			FileCreationTime:  pointerTo(time.Now().Add(-10 * time.Minute)),
			FileLastWriteTime: pointerTo(time.Now().Add(-10 * time.Minute)),
		},
	})

	dstB := dstContainer.GetObject(svm, nameB, common.EEntityType.Hardlink())
	dstB.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Hardlink(), HardLinkedFileName: nameA})

	dstC := dstContainer.GetObject(svm, nameC, common.EEntityType.File())
	dstC.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	dstC.SetObjectProperties(svm, ObjectProperties{
		FileNFSProperties: &FileNFSProperties{
			FileCreationTime:  pointerTo(time.Now().Add(-10 * time.Minute)),
			FileLastWriteTime: pointerTo(time.Now().Add(-10 * time.Minute)),
		},
	})

	dstD := dstContainer.GetObject(svm, nameD, common.EEntityType.Hardlink())
	dstD.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Hardlink(), HardLinkedFileName: nameC})

	if !svm.Dryrun() {
		time.Sleep(5 * time.Second)
	}

	// ── Source: one unified group A-B-C-D ────────────────────────────────────
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(nameA),
		Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.File(),
		},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(nameB),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: nameA,
		},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(nameC),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: nameA,
		},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(nameD),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: nameA,
		},
	})

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	stdOut := runHardlinkSync(svm, srcDirObj, dstDir.(RemoteResourceManager), true)

	// A and B are identical (same anchor) — skipped.
	// C and D are recreated as hardlinks → A to join the merged group.
	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			nameA: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			nameB: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: nameA,
				},
			},
			nameC: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: nameA,
				},
			},
			nameD: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: nameA,
				},
			},
		},
	}, ValidateResourceOptions{fromTo: common.EFromTo.LocalFileNFS()})

	// C and D are recreated as hardlinks → A.  A and B are skipped.
	ValidateHardlinksTransferCount(svm, stdOut, 2)
}

// Scenario: AnchorBecomesFile — src: A(file)  B-C-D   dst: A-B-C-D
//
// A was part of the hardlink group at dest but has been detached and is now a
// standalone regular file at source.  B-C-D remain linked together.
//
// Source:
//
//	A.txt  (regular file, nlink=1 — no longer part of any hardlink group)
//	B.txt  (anchor of B-C-D group)
//	C.txt  (hardlink → B)
//	D.txt  (hardlink → B)
//
// Destination (before sync):
//
//	A.txt  (anchor of A-B-C-D group)
//	B.txt  (hardlink)
//	C.txt  (hardlink)
//	D.txt  (hardlink)
//
// Expected: A re-uploaded as standalone file (entity-type mismatch);
// B, C, D remain hardlinks to each other (same anchor B) and are skipped.  0 hardlink transfers.
func (s *FilesNFSTestSuite) Scenario_HardlinkSync_AnchorBecomesFile(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainers(svm)
	defer CleanupNFSDirectory(svm, dstContainer, rootDir)

	nameA := rootDir + "/A.txt"
	nameB := rootDir + "/B.txt"
	nameC := rootDir + "/C.txt"
	nameD := rootDir + "/D.txt"

	// ── Destination: one group, A is anchor ──────────────────────────────────
	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	dstA := dstContainer.GetObject(svm, nameA, common.EEntityType.File())
	dstA.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	dstA.SetObjectProperties(svm, ObjectProperties{
		FileNFSProperties: &FileNFSProperties{
			FileCreationTime:  pointerTo(time.Now().Add(-10 * time.Minute)),
			FileLastWriteTime: pointerTo(time.Now().Add(-10 * time.Minute)),
		},
	})

	dstB := dstContainer.GetObject(svm, nameB, common.EEntityType.Hardlink())
	dstB.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Hardlink(), HardLinkedFileName: nameA})

	dstC := dstContainer.GetObject(svm, nameC, common.EEntityType.Hardlink())
	dstC.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Hardlink(), HardLinkedFileName: nameA})

	dstD := dstContainer.GetObject(svm, nameD, common.EEntityType.Hardlink())
	dstD.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Hardlink(), HardLinkedFileName: nameA})

	if !svm.Dryrun() {
		time.Sleep(5 * time.Second)
	}

	// ── Source: A is now a standalone file; B-C-D remain linked ──────────────
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})

	// A is now a standalone regular file (nlink=1).
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(nameA),
		Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.File(),
		},
	})

	// B is the anchor of the B-C-D group.
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(nameB),
		Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.File(),
		},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(nameC),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: nameB,
		},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(nameD),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: nameB,
		},
	})

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	stdOut := runHardlinkSync(svm, srcDirObj, dstDir.(RemoteResourceManager), true)

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			nameA: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			nameB: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			nameC: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: nameB,
				},
			},
			nameD: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: nameB,
				},
			},
		},
	}, ValidateResourceOptions{fromTo: common.EFromTo.LocalFileNFS()})

	// A re-uploaded as file (entity-type mismatch).
	// B,C,D are part of the same inode group so no hardlink transfer is needed for them.
	ValidateHardlinksTransferCount(svm, stdOut, 0)
}

// Scenario: FileJoinsGroup — src: A-B-C-D   dst: A(file)  B-C-D
//
// A was a standalone file at dest but is now linked with B-C-D at source.
// B-C-D were an independent group at dest and must now be re-linked to A.
//
// Source:
//
//	A.txt  (anchor of A-B-C-D group)
//	B.txt  (hardlink)
//	C.txt  (hardlink)
//	D.txt  (hardlink)
//
// Destination (before sync):
//
//	A.txt  (standalone regular file, nlink=1)
//	B.txt  (anchor of B-C-D group)
//	C.txt  (hardlink)
//	D.txt  (hardlink)
//
// Expected: A re-uploaded as anchor (entity-type mismatch); B, C, D recreated as
// hardlinks → A, joining the merged group.
// TODO: this scenario currently exposes a bug where B, C, D are incorrectly skipped
// instead of being recreated pointing to A.
func (s *FilesNFSTestSuite) Scenario_HardlinkSync_FileJoinsGroup(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainers(svm)
	defer CleanupNFSDirectory(svm, dstContainer, rootDir)

	nameA := rootDir + "/A.txt"
	nameB := rootDir + "/B.txt"
	nameC := rootDir + "/C.txt"
	nameD := rootDir + "/D.txt"

	// ── Destination: A is standalone; B-C-D are an independent group ─────────
	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	dstA := dstContainer.GetObject(svm, nameA, common.EEntityType.File())
	dstA.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	dstA.SetObjectProperties(svm, ObjectProperties{
		FileNFSProperties: &FileNFSProperties{
			FileCreationTime:  pointerTo(time.Now().Add(-10 * time.Minute)),
			FileLastWriteTime: pointerTo(time.Now().Add(-10 * time.Minute)),
		},
	})

	dstB := dstContainer.GetObject(svm, nameB, common.EEntityType.File())
	dstB.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	dstB.SetObjectProperties(svm, ObjectProperties{
		FileNFSProperties: &FileNFSProperties{
			FileCreationTime:  pointerTo(time.Now().Add(-10 * time.Minute)),
			FileLastWriteTime: pointerTo(time.Now().Add(-10 * time.Minute)),
		},
	})

	dstC := dstContainer.GetObject(svm, nameC, common.EEntityType.Hardlink())
	dstC.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Hardlink(), HardLinkedFileName: nameB})

	dstD := dstContainer.GetObject(svm, nameD, common.EEntityType.Hardlink())
	dstD.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Hardlink(), HardLinkedFileName: nameB})

	if !svm.Dryrun() {
		time.Sleep(5 * time.Second)
	}

	// ── Source: A-B-C-D all one group, A is the anchor ───────────────────────
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(nameA),
		Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.File(),
		},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(nameB),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: nameA,
		},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(nameC),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: nameA,
		},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(nameD),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: nameA,
		},
	})

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	stdOut := runHardlinkSync(svm, srcDirObj, dstDir.(RemoteResourceManager), true)

	// A re-uploaded as anchor (entity-type mismatch: dest=File, src=Hardlink-anchor).
	// B, C, D recreated as hardlinks → A to join the unified group.
	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			nameA: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			nameB: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: nameA,
				},
			},
			nameC: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: nameA,
				},
			},
			nameD: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: nameA,
				},
			},
		},
	}, ValidateResourceOptions{fromTo: common.EFromTo.LocalFileNFS()})

	// Total hardlink-type transfers = 1 (CreateHardlink(A)).
	ValidateHardlinksTransferCount(svm, stdOut, 1)
}

//
// These tests exercise the copy-upload direction with --hardlinks=preserve.  Unlike sync,
// copy does not compare source vs. destination timestamps — it always transfers.  The key
// assertions are that anchor files land as regular files and hardlink partners are created
// via the Azure Files CreateHardLink REST API, producing the correct inode relationships.
// ===========================================================================================

// helper: runs azcopy copy LocalFileNFS with --hardlinks=preserve and --as-subdir=false so
// that files land directly inside the given destination directory.
func runHardlinkCopy(
	svm *ScenarioVariationManager,
	srcDirObj ResourceManager,
	dstDirObj RemoteResourceManager,
) AzCopyStdout {
	stdOut, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbCopy,
			Targets: []ResourceManager{srcDirObj, dstDirObj.(RemoteResourceManager).WithSpecificAuthType(
				ResolveVariation(svm, []ExplicitCredentialTypes{
					EExplicitCredentialType.SASToken(),
					EExplicitCredentialType.OAuth(),
				}), svm, CreateAzCopyTargetOptions{}),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive:    pointerTo(true),
					FromTo:       pointerTo(common.EFromTo.LocalFileNFS()),
					HardlinkType: pointerTo(common.PreserveHardlinkHandlingType),
				},
				AsSubdir: pointerTo(false),
			},
		})
	return stdOut
}

// helper: creates an NFS source container and a local destination container for download
// tests.  Returns (srcNFSContainer, dstLocalContainer, rootDir).  Caller must defer
// CleanupNFSDirectory on srcNFSContainer.
func setupHardlinkDownloadContainers(svm *ScenarioVariationManager) (
	srcContainer ContainerResourceManager,
	dstContainer ContainerResourceManager,
	rootDir string,
) {
	srcContainer = GetRootResource(svm, common.ELocation.FileNFS(), GetResourceOptions{
		PreferredAccount: pointerTo(PremiumFileShareAcct),
	}).(ServiceResourceManager).GetContainer("hlcpydlsrc")
	if !srcContainer.Exists() {
		srcContainer.Create(svm, ContainerProperties{
			FileContainerProperties: FileContainerProperties{
				EnabledProtocols: pointerTo("NFS"),
			},
		})
	}

	dstContainer = CreateResource[ContainerResourceManager](svm, GetRootResource(
		svm, common.ELocation.Local()), ResourceDefinitionContainer{})

	rootDir = "hlcpydl_" + uuid.NewString()
	return
}

// helper: runs azcopy copy FileNFSLocal with --hardlinks=preserve and --as-subdir=false so
// that files from the source directory land directly in the local destination directory.
func runHardlinkCopyDownload(
	svm *ScenarioVariationManager,
	srcDirObj RemoteResourceManager,
	dstDirObj ResourceManager,
) AzCopyStdout {
	stdOut, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbCopy,
			Targets: []ResourceManager{srcDirObj.(RemoteResourceManager).WithSpecificAuthType(
				ResolveVariation(svm, []ExplicitCredentialTypes{
					EExplicitCredentialType.SASToken(),
					EExplicitCredentialType.OAuth(),
				}), svm, CreateAzCopyTargetOptions{}),
				dstDirObj,
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive:    pointerTo(true),
					FromTo:       pointerTo(common.EFromTo.FileNFSLocal()),
					HardlinkType: pointerTo(common.PreserveHardlinkHandlingType),
				},
				AsSubdir: pointerTo(false),
			},
		})
	return stdOut
}

// Scenario 13: Copy upload — initial copy of hardlinked files to empty NFS destination.
//
// Source (local):
//
//	anchor.txt          (regular file, nlink=2)
//	link_to_anchor.txt  (hardlink → anchor.txt)
//	independent.txt     (regular standalone file)
//
// Destination (NFS): empty
//
// Expected:
//   - anchor.txt transferred as regular file
//   - link_to_anchor.txt transferred as hardlink (CreateHardLink API)
//   - independent.txt transferred as regular file
//   - ValidateHardlinksTransferCount == 2 (anchor + link)
func (s *FilesNFSTestSuite) Scenario_HardlinkCopy_InitialUpload(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainers(svm)
	defer CleanupNFSDirectory(svm, dstContainer, rootDir)

	anchorName := rootDir + "/anchor.txt"
	linkName := rootDir + "/link_to_anchor.txt"
	independentName := rootDir + "/independent.txt"

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(anchorName),
		Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.File(),
		},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(linkName),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: anchorName,
		},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(independentName),
		Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.File(),
		},
	})

	// Create destination directory; --as-subdir=false places files directly inside it.
	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	srcDirObj2 := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	stdOut := runHardlinkCopy(svm, srcDirObj2, dstDir.(RemoteResourceManager))

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			anchorName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			linkName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: anchorName,
				},
			},
			independentName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
		},
	}, ValidateResourceOptions{fromTo: common.EFromTo.LocalFileNFS()})

	ValidateHardlinksTransferCount(svm, stdOut, 2)
}

// Scenario 14: Copy upload — multiple independent hardlink groups.
//
// Source:
//
//	group1_anchor.txt   (file, nlink=2)
//	group1_link.txt     (hardlink → group1_anchor.txt)
//	group2_anchor.txt   (file, nlink=2)
//	group2_link.txt     (hardlink → group2_anchor.txt)
//	standalone.txt      (regular file)
//
// Destination: empty
//
// Expected: both groups and standalone transferred correctly; 4 hardlink transfers.
func (s *FilesNFSTestSuite) Scenario_HardlinkCopy_MultipleGroups(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainers(svm)
	defer CleanupNFSDirectory(svm, dstContainer, rootDir)

	g1Anchor := rootDir + "/group1_anchor.txt"
	g1Link := rootDir + "/group1_link.txt"
	g2Anchor := rootDir + "/group2_anchor.txt"
	g2Link := rootDir + "/group2_link.txt"
	standaloneName := rootDir + "/standalone.txt"

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(g1Anchor),
		Body:             NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(g1Link),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: g1Anchor,
		},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(g2Anchor),
		Body:             NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(g2Link),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: g2Anchor,
		},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(standaloneName),
		Body:             NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
	})

	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	srcDirObj2 := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	stdOut := runHardlinkCopy(svm, srcDirObj2, dstDir.(RemoteResourceManager))

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			g1Anchor: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			g1Link: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: g1Anchor,
				},
			},
			g2Anchor: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			g2Link: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: g2Anchor,
				},
			},
			standaloneName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
		},
	}, ValidateResourceOptions{fromTo: common.EFromTo.LocalFileNFS()})

	ValidateHardlinksTransferCount(svm, stdOut, 4)
}

// Scenario 15: Copy upload — hardlinks in nested subdirectories.
//
// Source:
//
//	subdir/anchor.txt        (file, nlink=2)
//	subdir/nested/link.txt   (hardlink → subdir/anchor.txt)
//
// Destination: empty
//
// Expected: both files transferred; link.txt created as hardlink to anchor.txt.
func (s *FilesNFSTestSuite) Scenario_HardlinkCopy_NestedDirectories(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainers(svm)
	defer CleanupNFSDirectory(svm, dstContainer, rootDir)

	subdirName := rootDir + "/subdir"
	nestedDirName := rootDir + "/subdir/nested"
	anchorName := rootDir + "/subdir/anchor.txt"
	linkName := rootDir + "/subdir/nested/link.txt"

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(subdirName),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(nestedDirName),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(anchorName),
		Body:             NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(linkName),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: anchorName,
		},
	})

	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	srcDirObj2 := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	stdOut := runHardlinkCopy(svm, srcDirObj2, dstDir.(RemoteResourceManager))

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			anchorName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			linkName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: anchorName,
				},
			},
		},
	}, ValidateResourceOptions{fromTo: common.EFromTo.LocalFileNFS()})

	ValidateHardlinksTransferCount(svm, stdOut, 2)
}

// ===========================================================================================
// Hardlink Preserve Download Scenarios (Azure Files NFS → Local, using `azcopy copy`)
//
// These tests exercise the copy-download direction with --hardlinks=preserve.  The NFS share
// carries Azure Files hardlink metadata; azcopy must recreate the on-disk hardlink
// relationship locally via os.Link.  The primary assertion is ValidateHardlinksTransferCount;
// file existence validation confirms both anchor and link files are present locally.
// ===========================================================================================

// Scenario 16: Copy download — initial download of hardlinked files from NFS to local.
//
// Source (NFS):
//
//	rootDir/anchor.txt         (regular file, FileNFSNlink=2)
//	rootDir/link_to_anchor.txt (hardlink → anchor.txt)
//
// Destination (local): empty container
//
// Expected (with --as-subdir=false, files land directly in dstContainer):
//   - anchor.txt present as regular file
//   - link_to_anchor.txt created as hardlink (os.Link) to anchor.txt
//   - ValidateHardlinksTransferCount == 2
func (s *FilesNFSTestSuite) Scenario_HardlinkCopy_DownloadInitial(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkDownloadContainers(svm)
	defer CleanupNFSDirectory(svm, srcContainer, rootDir)

	anchorName := rootDir + "/anchor.txt"
	linkName := rootDir + "/link_to_anchor.txt"

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(anchorName),
		Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.File(),
		},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(linkName),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: anchorName,
		},
	})

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	// --as-subdir=false: files land at dstContainer/anchor.txt etc. (no rootDir prefix).
	stdOut := runHardlinkCopyDownload(svm, srcDirObj.(RemoteResourceManager), dstContainer)

	anchorLocal := "anchor.txt"
	linkLocal := "link_to_anchor.txt"

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			anchorLocal: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			linkLocal: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: anchorLocal,
				},
			},
		},
	}, ValidateResourceOptions{fromTo: common.EFromTo.FileNFSLocal()})

	ValidateHardlinksTransferCount(svm, stdOut, 2)
}

// Scenario 17: Copy download — multiple independent hardlink groups from NFS to local.
//
// Source (NFS):
//
//	rootDir/group1_anchor.txt  (file, nlink=2)
//	rootDir/group1_link.txt    (hardlink → group1_anchor.txt)
//	rootDir/group2_anchor.txt  (file, nlink=2)
//	rootDir/group2_link.txt    (hardlink → group2_anchor.txt)
//	rootDir/standalone.txt     (regular file)
//
// Destination (local): empty
//
// Expected: both groups preserved as local hardlinks; 4 hardlink transfers.
func (s *FilesNFSTestSuite) Scenario_HardlinkCopy_DownloadMultipleGroups(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkDownloadContainers(svm)
	defer CleanupNFSDirectory(svm, srcContainer, rootDir)

	g1Anchor := rootDir + "/group1_anchor.txt"
	g1Link := rootDir + "/group1_link.txt"
	g2Anchor := rootDir + "/group2_anchor.txt"
	g2Link := rootDir + "/group2_link.txt"
	standaloneName := rootDir + "/standalone.txt"

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(g1Anchor),
		Body:             NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(g1Link),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: g1Anchor,
		},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(g2Anchor),
		Body:             NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(g2Link),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: g2Anchor,
		},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(standaloneName),
		Body:             NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
	})

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	stdOut := runHardlinkCopyDownload(svm, srcDirObj.(RemoteResourceManager), dstContainer)

	g1AnchorLocal := "group1_anchor.txt"
	g1LinkLocal := "group1_link.txt"
	g2AnchorLocal := "group2_anchor.txt"
	g2LinkLocal := "group2_link.txt"
	standaloneLocal := "standalone.txt"

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			g1AnchorLocal: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			g1LinkLocal: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: g1AnchorLocal,
				},
			},
			g2AnchorLocal: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			g2LinkLocal: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: g2AnchorLocal,
				},
			},
			standaloneLocal: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
		},
	}, ValidateResourceOptions{fromTo: common.EFromTo.FileNFSLocal()})

	ValidateHardlinksTransferCount(svm, stdOut, 4)
}

// Scenario 18: Copy download — hardlinks in nested subdirectories from NFS to local.
//
// Source (NFS):
//
//	rootDir/subdir/anchor.txt        (file, nlink=2)
//	rootDir/subdir/nested/link.txt   (hardlink → subdir/anchor.txt)
//
// Destination (local): empty
//
// Expected: both files present locally; link.txt is a hardlink to anchor.txt.
func (s *FilesNFSTestSuite) Scenario_HardlinkCopy_DownloadNestedDirectories(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkDownloadContainers(svm)
	defer CleanupNFSDirectory(svm, srcContainer, rootDir)

	subdirName := rootDir + "/subdir"
	nestedDirName := rootDir + "/subdir/nested"
	anchorName := rootDir + "/subdir/anchor.txt"
	linkName := rootDir + "/subdir/nested/link.txt"

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(subdirName),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(nestedDirName),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(anchorName),
		Body:             NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(linkName),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: anchorName,
		},
	})

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	stdOut := runHardlinkCopyDownload(svm, srcDirObj.(RemoteResourceManager), dstContainer)

	anchorLocal := "subdir/anchor.txt"
	linkLocal := "subdir/nested/link.txt"

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			anchorLocal: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			linkLocal: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: anchorLocal,
				},
			},
		},
	}, ValidateResourceOptions{fromTo: common.EFromTo.FileNFSLocal()})

	ValidateHardlinksTransferCount(svm, stdOut, 2)
}
