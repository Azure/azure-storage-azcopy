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
					//EExplicitCredentialType.OAuth(),
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
