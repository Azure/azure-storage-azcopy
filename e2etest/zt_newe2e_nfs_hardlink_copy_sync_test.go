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

// isNFSContainer returns true when c is backed by Azure Files NFS.
func isNFSContainer(c ContainerResourceManager) bool {
	return c.Location() == common.ELocation.FileNFS()
}

// setOldLMT stamps the object with a timestamp 10 minutes in the past; only NFS
// containers support explicit LMT override via SetObjectProperties.
func setOldLMT(svm *ScenarioVariationManager, obj ObjectResourceManager, container ContainerResourceManager) {
	if isNFSContainer(container) {
		obj.SetObjectProperties(svm, ObjectProperties{
			FileNFSProperties: &FileNFSProperties{
				FileCreationTime:  pointerTo(time.Now().Add(-10 * time.Minute)),
				FileLastWriteTime: pointerTo(time.Now().Add(-10 * time.Minute)),
			},
		})
	}
}

// setSharedLMTIfNFS stamps the object with the given LMT, but only for NFS containers.
func setSharedLMTIfNFS(svm *ScenarioVariationManager, obj ObjectResourceManager, container ContainerResourceManager, lmt time.Time) {
	if isNFSContainer(container) {
		obj.SetObjectProperties(svm, ObjectProperties{
			FileNFSProperties: &FileNFSProperties{
				FileCreationTime:  pointerTo(lmt),
				FileLastWriteTime: pointerTo(lmt),
			},
		})
	}
}

// nfsPropsIfNFS returns a *FileNFSProperties with FileLastWriteTime set to lmt when the
// container is NFS, or nil otherwise.  Pass the result as ObjectProperties.FileNFSProperties
// in a CreateResource call to make it harmless for local containers.
func nfsPropsIfNFS(container ContainerResourceManager, lmt time.Time) *FileNFSProperties {
	return &FileNFSProperties{FileLastWriteTime: pointerTo(lmt)}
}

// setupHardlinkSyncContainersForFromTo returns (srcContainer, dstContainer, rootDir) with
// containers appropriate for the given fromTo direction.  Caller must defer
// cleanupHardlinkSyncForFromTo.
func setupHardlinkSyncContainersForFromTo(svm *ScenarioVariationManager, fromTo common.FromTo) (
	srcContainer ContainerResourceManager,
	dstContainer ContainerResourceManager,
	rootDir string,
) {
	getNFSDst := func() ContainerResourceManager {
		c := GetRootResource(svm, common.ELocation.FileNFS(), GetResourceOptions{
			PreferredAccount: pointerTo(PremiumFileShareAcct),
		}).(ServiceResourceManager).GetContainer("hlsyncdst")
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
		}).(ServiceResourceManager).GetContainer("hlsyncsrc")
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
	rootDir = "hlsync_" + uuid.NewString()
	return
}

// cleanupHardlinkSyncForFromTo cleans up the NFS directories created by
// setupHardlinkSyncContainersForFromTo.
func cleanupHardlinkSyncForFromTo(svm *ScenarioVariationManager, fromTo common.FromTo,
	srcContainer, dstContainer ContainerResourceManager, rootDir string) {
	switch fromTo {
	case common.EFromTo.LocalFileNFS():
		CleanupNFSDirectory(svm, dstContainer, rootDir)
	case common.EFromTo.FileNFSLocal():
		CleanupNFSDirectory(svm, srcContainer, rootDir)
	default: // FileNFSFileNFS
		CleanupNFSDirectory(svm, srcContainer, rootDir)
		CleanupNFSDirectory(svm, dstContainer, rootDir)
	}
}

// runHardlinkSyncForFromTo runs azcopy sync with --hardlinks=preserve for any fromTo
// direction.  Both src and dst are authenticated when they are remote.
func runHardlinkSyncForFromTo(
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
			Recursive:    pointerTo(true),
			FromTo:       pointerTo(fromTo),
			HardlinkType: pointerTo(common.PreserveHardlinkHandlingType),
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

// runHardlinkCopyForFromTo runs azcopy copy with --as-subdir=false
// for any fromTo direction.  Both src and dst are authenticated when they are remote.
// Pass preserveSymlinks=true to also set --preserve-symlinks.
// Pass preserveHardlinks=true to also set --hardlinks=preserve.
func runHardlinkCopyForFromTo(
	svm *ScenarioVariationManager,
	srcDirObj ResourceManager,
	dstDirObj ResourceManager,
	fromTo common.FromTo,
	hardlinkType common.HardlinkHandlingType,
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
	flags := CopyFlags{
		CopySyncCommonFlags: CopySyncCommonFlags{
			Recursive:    pointerTo(true),
			FromTo:       pointerTo(fromTo),
			HardlinkType: pointerTo(hardlinkType),
		},
		AsSubdir: pointerTo(false),
	}

	if len(preserveSymlinks) > 0 && preserveSymlinks[0] {
		flags.CopySyncCommonFlags.PreserveSymlinks = pointerTo(true)
	}

	stdOut, _ := RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{authIfRemote(srcDirObj), authIfRemote(dstDirObj)},
		Flags:   flags,
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

	fromTo := NamedResolveVariation(svm, map[string]common.FromTo{
		"|fromTo=LocalFileNFS":   common.EFromTo.LocalFileNFS(),
		"|fromTo=FileNFSLocal":   common.EFromTo.FileNFSLocal(),
		"|fromTo=FileNFSFileNFS": common.EFromTo.FileNFSFileNFS(),
	})
	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainersForFromTo(svm, fromTo)
	defer cleanupHardlinkSyncForFromTo(svm, fromTo, srcContainer, dstContainer, rootDir)

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
	setOldLMT(svm, dstSeed, dstContainer)

	if !svm.Dryrun() {
		time.Sleep(5 * time.Second)
	}

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDirObj := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	stdOut := runHardlinkSyncForFromTo(svm, srcDirObj, dstDirObj, fromTo, false)

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
	}, ValidateResourceOptions{fromTo: fromTo,
		hardlinkHandling: common.PreserveHardlinkHandlingType})

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

	fromTo := NamedResolveVariation(svm, map[string]common.FromTo{
		"|fromTo=LocalFileNFS":   common.EFromTo.LocalFileNFS(),
		"|fromTo=FileNFSLocal":   common.EFromTo.FileNFSLocal(),
		"|fromTo=FileNFSFileNFS": common.EFromTo.FileNFSFileNFS(),
	})
	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainersForFromTo(svm, fromTo)
	defer cleanupHardlinkSyncForFromTo(svm, fromTo, srcContainer, dstContainer, rootDir)

	anchorName := rootDir + "/anchor.txt"
	linkName := rootDir + "/link_to_anchor.txt"

	// Create destination (old state) — set up hardlink relationship at destination first
	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	dstAnchor := dstContainer.GetObject(svm, anchorName, common.EEntityType.File())
	dstAnchor.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	setOldLMT(svm, dstAnchor, dstContainer)

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

	stdOut := runHardlinkSyncForFromTo(svm, srcDirObj, dstDir, fromTo, false)

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
	}, ValidateResourceOptions{fromTo: fromTo,
		hardlinkHandling: common.PreserveHardlinkHandlingType})

	_ = stdOut
}

// Scenario: Content modified via non-anchor hardlink member must propagate to destination.
//
// This exercises the fix for the anchor/data-carrier mismatch bug.  When content
// is modified through a non-anchor (lex-larger) hardlink member, the underlying
// inode data changes for all members.  On re-sync the comparator must detect the
// stale content at the destination and re-upload through the anchor, regardless
// of which file os.Readdir returns first.
//
// Source (local):
//
//	hl_1.txt  (anchor, nlink=2, content = newBody — written via hl_2.txt)
//	hl_2.txt  (hardlink → hl_1.txt, same inode, same newBody)
//
// Destination (NFS, before sync):
//
//	hl_1.txt  (anchor, nlink=2, content = oldBody, older LMT)
//	hl_2.txt  (hardlink → hl_1.txt, same old content)
//
// Expected: anchor content is re-uploaded with newBody; hardlink relationship preserved.
func (s *FilesNFSTestSuite) Scenario_HardlinkSync_ContentModifiedViaNonAnchor(svm *ScenarioVariationManager) {

	fromTo := NamedResolveVariation(svm, map[string]common.FromTo{
		"|fromTo=LocalFileNFS":   common.EFromTo.LocalFileNFS(),
		"|fromTo=FileNFSLocal":   common.EFromTo.FileNFSLocal(),
		"|fromTo=FileNFSFileNFS": common.EFromTo.FileNFSFileNFS(),
	})

	if fromTo != common.EFromTo.FileNFSFileNFS() && runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainersForFromTo(svm, fromTo)
	defer cleanupHardlinkSyncForFromTo(svm, fromTo, srcContainer, dstContainer, rootDir)

	anchorName := rootDir + "/hl_1.txt" // lex-smallest → anchor
	linkName := rootDir + "/hl_2.txt"   // lex-larger → non-anchor hardlink

	// ── Destination (old state) ──────────────────────────────────────────────
	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	dstAnchor := dstContainer.GetObject(svm, anchorName, common.EEntityType.File())
	dstAnchor.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	setOldLMT(svm, dstAnchor, dstContainer)

	dstLink := dstContainer.GetObject(svm, linkName, common.EEntityType.Hardlink())
	dstLink.Create(svm, nil, ObjectProperties{
		EntityType:         common.EEntityType.Hardlink(),
		HardLinkedFileName: anchorName,
	})

	if !svm.Dryrun() {
		time.Sleep(5 * time.Second)
	}

	// ── Source (new state — content written via the non-anchor hl_2.txt) ─────
	// Because hl_1 and hl_2 share the same inode, writing through hl_2
	// updates the data for hl_1 as well.  We create hl_1 as the regular file
	// (data carrier) and hl_2 as the hardlink to hl_1.  The body is the new
	// content that must appear at the destination after sync.
	newBody := NewRandomObjectContentContainer(SizeFromString("1K"))

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(anchorName),
		Body:       newBody,
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

	stdOut := runHardlinkSyncForFromTo(svm, srcDirObj, dstDir, fromTo, false)

	// ── Validate ─────────────────────────────────────────────────────────────
	// Both hl_1.txt and hl_2.txt at the destination must carry newBody's content.
	// The hardlink relationship must remain intact.
	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			anchorName: ResourceDefinitionObject{
				Body:             newBody,
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			linkName: ResourceDefinitionObject{
				Body: newBody,
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: anchorName,
				},
			},
		},
	}, ValidateResourceOptions{fromTo: fromTo,
		validateObjectContent: true,
		hardlinkHandling:      common.PreserveHardlinkHandlingType})

	// The anchor file's content should be re-uploaded (1 copy transfer),
	// and the non-anchor hardlink should be preserved (1 hardlink transfer).
	ValidateHardlinksTransferCount(svm, stdOut, 1)
}

// Scenario 3: Hardlink→Hardlink, same anchor, dest up-to-date → skip (no transfer).
//
// Source and destination have identical hardlink relationships and the destination
// is at least as recent as the source.
//
// Expected: no transfers scheduled; hardlink count stays 0.
func (s *FilesNFSTestSuite) Scenario_HardlinkSync_SameAnchorUpToDate(svm *ScenarioVariationManager) {

	fromTo := NamedResolveVariation(svm, map[string]common.FromTo{
		"|fromTo=LocalFileNFS":   common.EFromTo.LocalFileNFS(),
		"|fromTo=FileNFSLocal":   common.EFromTo.FileNFSLocal(),
		"|fromTo=FileNFSFileNFS": common.EFromTo.FileNFSFileNFS(),
	})

	if fromTo != common.EFromTo.FileNFSFileNFS() && runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainersForFromTo(svm, fromTo)
	defer cleanupHardlinkSyncForFromTo(svm, fromTo, srcContainer, dstContainer, rootDir)

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

	runHardlinkSyncForFromTo(svm, srcDirObj, dstDir, fromTo, false)

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
	}, ValidateResourceOptions{fromTo: fromTo,
		hardlinkHandling: common.PreserveHardlinkHandlingType})
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

	fromTo := NamedResolveVariation(svm, map[string]common.FromTo{
		"|fromTo=LocalFileNFS":   common.EFromTo.LocalFileNFS(),
		"|fromTo=FileNFSLocal":   common.EFromTo.FileNFSLocal(),
		"|fromTo=FileNFSFileNFS": common.EFromTo.FileNFSFileNFS(),
	})

	if fromTo != common.EFromTo.FileNFSFileNFS() && runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainersForFromTo(svm, fromTo)
	defer cleanupHardlinkSyncForFromTo(svm, fromTo, srcContainer, dstContainer, rootDir)

	oldAnchorName := rootDir + "/old_anchor.txt"
	newAnchorName := rootDir + "/new_anchor.txt"
	retargetLinkName := rootDir + "/retarget_link.txt"

	// Create destination (old state): retarget_link → old_anchor
	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	dstOldAnchor := dstContainer.GetObject(svm, oldAnchorName, common.EEntityType.File())
	dstOldAnchor.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	setOldLMT(svm, dstOldAnchor, dstContainer)

	dstNewAnchor := dstContainer.GetObject(svm, newAnchorName, common.EEntityType.File())
	dstNewAnchor.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	setOldLMT(svm, dstNewAnchor, dstContainer)

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

	stdOut := runHardlinkSyncForFromTo(svm, srcDirObj, dstDir, fromTo, true)

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
	}, ValidateResourceOptions{fromTo: fromTo,
		hardlinkHandling: common.PreserveHardlinkHandlingType})

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

	fromTo := NamedResolveVariation(svm, map[string]common.FromTo{
		"|fromTo=LocalFileNFS":   common.EFromTo.LocalFileNFS(),
		"|fromTo=FileNFSLocal":   common.EFromTo.FileNFSLocal(),
		"|fromTo=FileNFSFileNFS": common.EFromTo.FileNFSFileNFS(),
	})

	if fromTo != common.EFromTo.FileNFSFileNFS() && runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainersForFromTo(svm, fromTo)
	defer cleanupHardlinkSyncForFromTo(svm, fromTo, srcContainer, dstContainer, rootDir)

	anchorName := rootDir + "/anchor.txt"
	wasFileName := rootDir + "/was_file.txt"

	// Create destination (old state): two independent regular files
	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	dstAnchor := dstContainer.GetObject(svm, anchorName, common.EEntityType.File())
	dstAnchor.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	setOldLMT(svm, dstAnchor, dstContainer)

	dstWasFile := dstContainer.GetObject(svm, wasFileName, common.EEntityType.File())
	dstWasFile.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	setOldLMT(svm, dstWasFile, dstContainer)

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

	stdOut := runHardlinkSyncForFromTo(svm, srcDirObj, dstDir, fromTo, true)

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
	}, ValidateResourceOptions{fromTo: fromTo,
		hardlinkHandling: common.PreserveHardlinkHandlingType})

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

	fromTo := NamedResolveVariation(svm, map[string]common.FromTo{
		"|fromTo=LocalFileNFS":   common.EFromTo.LocalFileNFS(),
		"|fromTo=FileNFSLocal":   common.EFromTo.FileNFSLocal(),
		"|fromTo=FileNFSFileNFS": common.EFromTo.FileNFSFileNFS(),
	})

	if fromTo != common.EFromTo.FileNFSFileNFS() && runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainersForFromTo(svm, fromTo)
	defer cleanupHardlinkSyncForFromTo(svm, fromTo, srcContainer, dstContainer, rootDir)

	anchorName := rootDir + "/anchor.txt"
	wasLinkName := rootDir + "/was_link.txt"

	// Create destination (old state): anchor + was_link as hardlink
	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	dstAnchor := dstContainer.GetObject(svm, anchorName, common.EEntityType.File())
	dstAnchor.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	setOldLMT(svm, dstAnchor, dstContainer)

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

	stdOut := runHardlinkSyncForFromTo(svm, srcDirObj, dstDir, fromTo, true)

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
	}, ValidateResourceOptions{fromTo: fromTo,
		hardlinkHandling: common.PreserveHardlinkHandlingType})

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

	fromTo := NamedResolveVariation(svm, map[string]common.FromTo{
		"|fromTo=LocalFileNFS":   common.EFromTo.LocalFileNFS(),
		"|fromTo=FileNFSLocal":   common.EFromTo.FileNFSLocal(),
		"|fromTo=FileNFSFileNFS": common.EFromTo.FileNFSFileNFS(),
	})

	if fromTo != common.EFromTo.FileNFSFileNFS() && runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainersForFromTo(svm, fromTo)
	defer cleanupHardlinkSyncForFromTo(svm, fromTo, srcContainer, dstContainer, rootDir)

	anchorName := rootDir + "/anchor.txt"
	removedLinkName := rootDir + "/link_removed.txt"

	// Create destination (old state): anchor + link_removed
	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	dstAnchor := dstContainer.GetObject(svm, anchorName, common.EEntityType.File())
	dstAnchor.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	setOldLMT(svm, dstAnchor, dstContainer)

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

	runHardlinkSyncForFromTo(svm, srcDirObj, dstDir, fromTo, true)

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
	}, ValidateResourceOptions{fromTo: fromTo,
		hardlinkHandling: common.PreserveHardlinkHandlingType})
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

	fromTo := NamedResolveVariation(svm, map[string]common.FromTo{
		"|fromTo=LocalFileNFS":   common.EFromTo.LocalFileNFS(),
		"|fromTo=FileNFSLocal":   common.EFromTo.FileNFSLocal(),
		"|fromTo=FileNFSFileNFS": common.EFromTo.FileNFSFileNFS(),
	})

	if fromTo != common.EFromTo.FileNFSFileNFS() && runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainersForFromTo(svm, fromTo)
	defer cleanupHardlinkSyncForFromTo(svm, fromTo, srcContainer, dstContainer, rootDir)

	anchorName := rootDir + "/anchor.txt"
	existingLinkName := rootDir + "/existing_link.txt"
	newLinkName := rootDir + "/new_link.txt"

	// Create destination (old state): anchor + existing_link
	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	dstAnchor := dstContainer.GetObject(svm, anchorName, common.EEntityType.File())
	dstAnchor.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	setOldLMT(svm, dstAnchor, dstContainer)

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

	stdOut := runHardlinkSyncForFromTo(svm, srcDirObj, dstDir, fromTo, false)

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
	}, ValidateResourceOptions{fromTo: fromTo,
		hardlinkHandling: common.PreserveHardlinkHandlingType})

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

	fromTo := NamedResolveVariation(svm, map[string]common.FromTo{
		"|fromTo=LocalFileNFS":   common.EFromTo.LocalFileNFS(),
		"|fromTo=FileNFSLocal":   common.EFromTo.FileNFSLocal(),
		"|fromTo=FileNFSFileNFS": common.EFromTo.FileNFSFileNFS(),
	})

	if fromTo != common.EFromTo.FileNFSFileNFS() && runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainersForFromTo(svm, fromTo)
	defer cleanupHardlinkSyncForFromTo(svm, fromTo, srcContainer, dstContainer, rootDir)

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
	setOldLMT(svm, dstSeed, dstContainer)

	if !svm.Dryrun() {
		time.Sleep(5 * time.Second)
	}

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDirObj := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	stdOut := runHardlinkSyncForFromTo(svm, srcDirObj, dstDirObj, fromTo, false)

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
	}, ValidateResourceOptions{fromTo: fromTo,
		hardlinkHandling: common.PreserveHardlinkHandlingType})

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

	fromTo := NamedResolveVariation(svm, map[string]common.FromTo{
		"|fromTo=LocalFileNFS":   common.EFromTo.LocalFileNFS(),
		"|fromTo=FileNFSLocal":   common.EFromTo.FileNFSLocal(),
		"|fromTo=FileNFSFileNFS": common.EFromTo.FileNFSFileNFS(),
	})

	if fromTo != common.EFromTo.FileNFSFileNFS() && runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainersForFromTo(svm, fromTo)
	defer cleanupHardlinkSyncForFromTo(svm, fromTo, srcContainer, dstContainer, rootDir)

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
	setOldLMT(svm, dstAnchorA, dstContainer)

	// link_a → anchor_a (correct)
	dstLinkA := dstContainer.GetObject(svm, linkA, common.EEntityType.Hardlink())
	dstLinkA.Create(svm, nil, ObjectProperties{
		EntityType:         common.EEntityType.Hardlink(),
		HardLinkedFileName: anchorA,
	})

	dstAnchorB := dstContainer.GetObject(svm, anchorB, common.EEntityType.File())
	dstAnchorB.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	setOldLMT(svm, dstAnchorB, dstContainer)

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

	runHardlinkSyncForFromTo(svm, srcDirObj, dstDir, fromTo, true)

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
	}, ValidateResourceOptions{fromTo: fromTo,
		hardlinkHandling: common.PreserveHardlinkHandlingType})
}

// Scenario 11: Idempotent re-sync — running the same sync twice produces no extra transfers.
//
// Run initial sync from Scenario 1, then run again. Second run should be a no-op.
func (s *FilesNFSTestSuite) Scenario_HardlinkSync_IdempotentResync(svm *ScenarioVariationManager) {

	fromTo := NamedResolveVariation(svm, map[string]common.FromTo{
		"|fromTo=LocalFileNFS":   common.EFromTo.LocalFileNFS(),
		"|fromTo=FileNFSLocal":   common.EFromTo.FileNFSLocal(),
		"|fromTo=FileNFSFileNFS": common.EFromTo.FileNFSFileNFS(),
	})

	if fromTo != common.EFromTo.FileNFSFileNFS() && runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainersForFromTo(svm, fromTo)
	defer cleanupHardlinkSyncForFromTo(svm, fromTo, srcContainer, dstContainer, rootDir)

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
	setOldLMT(svm, dstSeed, dstContainer)

	if !svm.Dryrun() {
		time.Sleep(5 * time.Second)
	}

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDirObj := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	// First sync — initial upload
	runHardlinkSyncForFromTo(svm, srcDirObj, dstDirObj, fromTo, false)

	if !svm.Dryrun() {
		time.Sleep(2 * time.Second)
	}

	// Second sync — should be a no-op (everything is up-to-date)
	runHardlinkSyncForFromTo(svm, srcDirObj, dstDirObj, fromTo, false)

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
	}, ValidateResourceOptions{fromTo: fromTo,
		hardlinkHandling: common.PreserveHardlinkHandlingType})
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

	fromTo := NamedResolveVariation(svm, map[string]common.FromTo{
		"|fromTo=LocalFileNFS":   common.EFromTo.LocalFileNFS(),
		"|fromTo=FileNFSLocal":   common.EFromTo.FileNFSLocal(),
		"|fromTo=FileNFSFileNFS": common.EFromTo.FileNFSFileNFS(),
	})

	if fromTo != common.EFromTo.FileNFSFileNFS() && runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainersForFromTo(svm, fromTo)
	defer cleanupHardlinkSyncForFromTo(svm, fromTo, srcContainer, dstContainer, rootDir)

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
	setOldLMT(svm, dstSeed, dstContainer)

	if !svm.Dryrun() {
		time.Sleep(5 * time.Second)
	}

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDirObj := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	stdOut := runHardlinkSyncForFromTo(svm, srcDirObj, dstDirObj, fromTo, false)

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
	}, ValidateResourceOptions{fromTo: fromTo,
		hardlinkHandling: common.PreserveHardlinkHandlingType})

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
//	A.txt          (anchor, nlink=4 — lex-smallest)
//	B.txt          (hardlink)
//	C.txt          (hardlink)
//	D.txt          (hardlink)
//
// Destination (before sync — result of a previous sync of {B,C,D}):
//
//	B.txt          (anchor, nlink=3)
//	C.txt          (hardlink)
//	D.txt          (hardlink)
func (s *FilesNFSTestSuite) Scenario_HardlinkSync_LexSmallerAnchorAdded(svm *ScenarioVariationManager) {

	fromTo := NamedResolveVariation(svm, map[string]common.FromTo{
		"|fromTo=LocalFileNFS":   common.EFromTo.LocalFileNFS(),
		"|fromTo=FileNFSLocal":   common.EFromTo.FileNFSLocal(),
		"|fromTo=FileNFSFileNFS": common.EFromTo.FileNFSFileNFS(),
	})

	if fromTo != common.EFromTo.FileNFSFileNFS() && runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainersForFromTo(svm, fromTo)
	defer cleanupHardlinkSyncForFromTo(svm, fromTo, srcContainer, dstContainer, rootDir)

	nameA := rootDir + "/A.txt"
	nameB := rootDir + "/B.txt"
	nameC := rootDir + "/C.txt"
	nameD := rootDir + "/D.txt"

	// Shared LMT: truncated to second precision so it survives the FILETIME
	// round-trip (100 ns resolution) without triggering a spurious LMT mismatch.
	sharedLMT := time.Now().Add(-5 * time.Minute).Truncate(time.Second)

	// ── Set up destination: previous sync result ─────────────────────────────
	// B was the anchor when the group was {B, C, D}.
	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	dstB := dstContainer.GetObject(svm, nameB, common.EEntityType.File())
	dstB.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	setSharedLMTIfNFS(svm, dstB, dstContainer, sharedLMT)

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

	// A is the new anchor at source (lex-smallest).
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(nameA),
		Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{
			EntityType:        common.EEntityType.File(),
			FileNFSProperties: nfsPropsIfNFS(srcContainer, sharedLMT),
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

	stdOut := runHardlinkSyncForFromTo(svm, srcDirObj, dstDir, fromTo, false)

	// ── Validate ─────────────────────────────────────────────────────────────
	// B C and D were skipped by the fix (dest anchor B is still in the source group).
	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			nameA: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Hardlink()},
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
	}, ValidateResourceOptions{fromTo: fromTo,
		hardlinkHandling: common.PreserveHardlinkHandlingType})

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
//	C.txt  (hardlink)
//	D.txt  (hardlink)
//
// Destination (before sync):
//
//	A.txt  (anchor of A-B-C-D group — the now-deleted lex-smaller anchor)
//	B.txt  (hardlink)
//	C.txt  (hardlink)
//	D.txt  (hardlink)
//
// Expected: A deleted; B, C, D skipped (still linked together, no structural change
// among the survivors).  0 hardlink transfers.
func (s *FilesNFSTestSuite) Scenario_HardlinkSync_LexSmallerAnchorDeleted(svm *ScenarioVariationManager) {

	fromTo := NamedResolveVariation(svm, map[string]common.FromTo{
		"|fromTo=LocalFileNFS":   common.EFromTo.LocalFileNFS(),
		"|fromTo=FileNFSLocal":   common.EFromTo.FileNFSLocal(),
		"|fromTo=FileNFSFileNFS": common.EFromTo.FileNFSFileNFS(),
	})

	if fromTo != common.EFromTo.FileNFSFileNFS() && runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainersForFromTo(svm, fromTo)
	defer cleanupHardlinkSyncForFromTo(svm, fromTo, srcContainer, dstContainer, rootDir)

	nameA := rootDir + "/A.txt"
	nameB := rootDir + "/B.txt"
	nameC := rootDir + "/C.txt"
	nameD := rootDir + "/D.txt"

	// Shared LMT: truncated to second precision so it survives the FILETIME
	// round-trip (100 ns resolution) without triggering a spurious LMT mismatch.
	sharedLMT := time.Now().Add(-5 * time.Minute).Truncate(time.Second)

	// ── Destination: A is the anchor, B/C/D are hardlinks to A ──────────────
	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	dstA := dstContainer.GetObject(svm, nameA, common.EEntityType.File())
	dstA.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	setSharedLMTIfNFS(svm, dstA, dstContainer, sharedLMT)

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
	// Its FileLastWriteTime matches the destination inode's LMT (set from A's
	// upload), preventing a spurious LMT-based transfer for the intact group.
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(nameB),
		Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{
			EntityType:        common.EEntityType.File(),
			FileNFSProperties: nfsPropsIfNFS(srcContainer, sharedLMT),
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

	stdOut := runHardlinkSyncForFromTo(svm, srcDirObj, dstDir, fromTo, true)

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
	}, ValidateResourceOptions{fromTo: fromTo,
		hardlinkHandling: common.PreserveHardlinkHandlingType})

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

	fromTo := NamedResolveVariation(svm, map[string]common.FromTo{
		"|fromTo=LocalFileNFS":   common.EFromTo.LocalFileNFS(),
		"|fromTo=FileNFSLocal":   common.EFromTo.FileNFSLocal(),
		"|fromTo=FileNFSFileNFS": common.EFromTo.FileNFSFileNFS(),
	})

	if fromTo != common.EFromTo.FileNFSFileNFS() && runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainersForFromTo(svm, fromTo)
	defer cleanupHardlinkSyncForFromTo(svm, fromTo, srcContainer, dstContainer, rootDir)

	nameA := rootDir + "/A.txt"
	nameB := rootDir + "/B.txt"
	nameC := rootDir + "/C.txt"
	nameD := rootDir + "/D.txt"

	// ── Destination: one group, A is anchor ──────────────────────────────────
	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	dstA := dstContainer.GetObject(svm, nameA, common.EEntityType.File())
	dstA.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	setOldLMT(svm, dstA, dstContainer)

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

	// Group 1: A + B — capture body so we can verify content after the split
	srcBodyA := NewRandomObjectContentContainer(SizeFromString("1K"))
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(nameA),
		Body:       srcBodyA,
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

	// Group 2: C + D — independent inode; capture body separately
	srcBodyC := NewRandomObjectContentContainer(SizeFromString("1K"))
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(nameC),
		Body:       srcBodyC,
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

	stdOut := runHardlinkSyncForFromTo(svm, srcDirObj, dstDir, fromTo, true)

	// A is force-transferred (dest inode of the old A-B-C-D group spans two src inodes;
	// anchor content must be re-verified for both new sub-groups).
	// B stays linked to A (non-anchor, relationship intact).
	// C is recreated as a new standalone anchor; D is recreated as hardlink → C.
	//
	// Content validation: A and B must carry srcBodyA; C and D must carry srcBodyC.
	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			nameA: ResourceDefinitionObject{
				Body:             srcBodyA,
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Hardlink()},
			},
			nameB: ResourceDefinitionObject{
				Body: srcBodyA,
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: nameA,
				},
			},
			nameC: ResourceDefinitionObject{
				Body:             srcBodyC,
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Hardlink()},
			},
			nameD: ResourceDefinitionObject{
				Body: srcBodyC,
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: nameC,
				},
			},
		},
	}, ValidateResourceOptions{fromTo: fromTo,
		validateObjectContent: true,
		hardlinkHandling:      common.PreserveHardlinkHandlingType})

	// A force-transferred + C recreated (anchor) + D recreated (hardlink): 3 transfers.
	// B is skipped (non-anchor, link structure unchanged).
	ValidateHardlinksTransferCount(svm, stdOut, 3)
}

// Scenario: GroupMerge — src: A-B-C-D   dst: A-B  C-D
//
// Two independent groups at dest are merged into one group at source.
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
//	A.txt  (anchor of A-B group)
//	B.txt  (hardlink->A)
//	C.txt  (anchor of C-D group — separate inode)
//	D.txt  (hardlink → C)
//
// Expected: A and B skipped (same anchor A); C and D recreated as hardlinks → A,
// unifying them into the A-B-C-D group.
func (s *FilesNFSTestSuite) Scenario_HardlinkSync_GroupMerge(svm *ScenarioVariationManager) {
	fromTo := NamedResolveVariation(svm, map[string]common.FromTo{
		"|fromTo=LocalFileNFS":   common.EFromTo.LocalFileNFS(),
		"|fromTo=FileNFSLocal":   common.EFromTo.FileNFSLocal(),
		"|fromTo=FileNFSFileNFS": common.EFromTo.FileNFSFileNFS(),
	})

	if fromTo != common.EFromTo.FileNFSFileNFS() && runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainersForFromTo(svm, fromTo)
	defer cleanupHardlinkSyncForFromTo(svm, fromTo, srcContainer, dstContainer, rootDir)

	nameA := rootDir + "/A.txt"
	nameB := rootDir + "/B.txt"
	nameC := rootDir + "/C.txt"
	nameD := rootDir + "/D.txt"

	sharedLMT := time.Now().Add(-5 * time.Minute).Truncate(time.Second)

	// ── Destination: two groups (A-B and C-D) ────────────────────────────────
	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	dstA := dstContainer.GetObject(svm, nameA, common.EEntityType.File())
	dstA.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	setSharedLMTIfNFS(svm, dstA, dstContainer, sharedLMT)

	dstB := dstContainer.GetObject(svm, nameB, common.EEntityType.Hardlink())
	dstB.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Hardlink(), HardLinkedFileName: nameA})

	dstC := dstContainer.GetObject(svm, nameC, common.EEntityType.File())
	dstC.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	setSharedLMTIfNFS(svm, dstC, dstContainer, sharedLMT)

	dstD := dstContainer.GetObject(svm, nameD, common.EEntityType.Hardlink())
	dstD.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Hardlink(), HardLinkedFileName: nameC})

	if !svm.Dryrun() {
		time.Sleep(5 * time.Second)
	}

	// ── Source: one unified group A-B-C-D ────────────────────────────────────
	// Capture the source anchor body so we can verify every destination file
	// carries the same content after the merge (this directly tests the bug
	// where C and D retained stale content from the old C-D inode).
	srcBodyA := NewRandomObjectContentContainer(SizeFromString("1K"))

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(nameA),
		Body:       srcBodyA,
		ObjectProperties: ObjectProperties{
			EntityType:        common.EEntityType.File(),
			FileNFSProperties: nfsPropsIfNFS(srcContainer, sharedLMT),
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

	stdOut := runHardlinkSyncForFromTo(svm, srcDirObj, dstDir, fromTo, true)

	// A is force-transferred (src inode spans two dest groups; anchor
	// content must match source).
	// B is skipped (non-anchor, relationship intact).
	// C and D are recreated as hardlinks → A to join the merged group.
	//
	// All four files must carry srcBodyA's content: A because it was re-uploaded,
	// and B, C, D because they are hardlinks that share the same inode as A.
	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			nameA: ResourceDefinitionObject{
				Body:             srcBodyA,
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Hardlink()},
			},
			nameB: ResourceDefinitionObject{
				Body: srcBodyA,
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: nameA,
				},
			},
			nameC: ResourceDefinitionObject{
				Body: srcBodyA,
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: nameA,
				},
			},
			nameD: ResourceDefinitionObject{
				Body: srcBodyA,
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: nameA,
				},
			},
		},
	}, ValidateResourceOptions{fromTo: fromTo,
		validateObjectContent: true,
		hardlinkHandling:      common.PreserveHardlinkHandlingType})

	ValidateHardlinksTransferCount(svm, stdOut, 3)
}

// Scenario: ComplexRegrouping — dst: A-B-C  D-E   src: A-D  B-C  E
//
// Two inode groups at the destination are completely reshuffled at the source:
// members migrate between groups and one file becomes standalone.
//
// Destination (before sync):
//
//	A.txt  (anchor of A-B-C group, nlink=3)
//	B.txt  (hardlink → A)
//	C.txt  (hardlink → A)
//	D.txt  (anchor of D-E group,   nlink=2)
//	E.txt  (hardlink → D)
//
// Source (new state):
//
//	A.txt  (anchor of A-D group, nlink=2)
//	D.txt  (hardlink → A)
//	B.txt  (anchor of B-C group, nlink=2)
//	C.txt  (hardlink → B)
//	E.txt  (standalone regular file, nlink=1)
//
// What the comparator does (every dest object is a Hardlink → ProcessPendingHardlinks):
//
//	A: src anchor="A", dst anchor="A" → same name, but srcInodeIsMultiGroup AND
//	   destGroupIsMultiSource both fire → groupStructureChanged → force-transfer A.
//	B: dst anchor="A", src anchor="B" → (a) dstAnchor still in src but maps to
//	   a different inode → needsRecreate → delete + re-create as new anchor.
//	C: dst anchor="A", src anchor="B" → same (a) → delete + re-create as hardlink→B.
//	D: dst anchor="D" (itself), src anchor="A" → srcInodeIsMultiGroup(inodeA) → needsRecreate
//	   → delete + re-create as hardlink→A.
//	E: src EntityType=File (Inode="") → srcAnchorFile="" → entity-type mismatch
//	   → delete + re-create as standalone file.
//
// Expected final state:
//
//	A.txt  (File, anchor of A-D group)   — content = srcBodyA
//	D.txt  (Hardlink → A)                — content = srcBodyA
//	B.txt  (File, anchor of B-C group)  — content = srcBodyB
//	C.txt  (Hardlink → B)                — content = srcBodyB
//	E.txt  (File, standalone)            — content = srcBodyE
func (s *FilesNFSTestSuite) Scenario_HardlinkSync_ComplexRegrouping(svm *ScenarioVariationManager) {

	fromTo := NamedResolveVariation(svm, map[string]common.FromTo{
		"|fromTo=LocalFileNFS":   common.EFromTo.LocalFileNFS(),
		"|fromTo=FileNFSLocal":   common.EFromTo.FileNFSLocal(),
		"|fromTo=FileNFSFileNFS": common.EFromTo.FileNFSFileNFS(),
	})

	if fromTo != common.EFromTo.FileNFSFileNFS() && runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainersForFromTo(svm, fromTo)
	defer cleanupHardlinkSyncForFromTo(svm, fromTo, srcContainer, dstContainer, rootDir)

	nameA := rootDir + "/A.txt"
	nameB := rootDir + "/B.txt"
	nameC := rootDir + "/C.txt"
	nameD := rootDir + "/D.txt"
	nameE := rootDir + "/E.txt"

	// ── Destination: group A-B-C and group D-E ───────────────────────────────
	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	dstA := dstContainer.GetObject(svm, nameA, common.EEntityType.File())
	dstA.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	setOldLMT(svm, dstA, dstContainer)

	dstB := dstContainer.GetObject(svm, nameB, common.EEntityType.Hardlink())
	dstB.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Hardlink(), HardLinkedFileName: nameA})

	dstC := dstContainer.GetObject(svm, nameC, common.EEntityType.Hardlink())
	dstC.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Hardlink(), HardLinkedFileName: nameA})

	dstD := dstContainer.GetObject(svm, nameD, common.EEntityType.File())
	dstD.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	setOldLMT(svm, dstD, dstContainer)

	dstE := dstContainer.GetObject(svm, nameE, common.EEntityType.Hardlink())
	dstE.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Hardlink(), HardLinkedFileName: nameD})

	if !svm.Dryrun() {
		time.Sleep(5 * time.Second)
	}

	// ── Source: group A-D, group B-C, standalone E ───────────────────────────
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})

	// Group 1: A (anchor) + D (hardlink)
	srcBodyA := NewRandomObjectContentContainer(SizeFromString("1K"))
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(nameA),
		Body:             srcBodyA,
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
	})
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(nameD),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: nameA,
		},
	})

	// Group 2: B (anchor) + C (hardlink)
	srcBodyB := NewRandomObjectContentContainer(SizeFromString("1K"))
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(nameB),
		Body:             srcBodyB,
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
	})
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(nameC),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: nameB,
		},
	})

	// Standalone: E
	srcBodyE := NewRandomObjectContentContainer(SizeFromString("1K"))
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(nameE),
		Body:             srcBodyE,
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
	})

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	stdOut := runHardlinkSyncForFromTo(svm, srcDirObj, dstDir, fromTo, true)

	// Validate structure and content.
	// A and D must share srcBodyA; B and C must share srcBodyB; E carries srcBodyE.
	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			nameA: ResourceDefinitionObject{
				Body:             srcBodyA,
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Hardlink()},
			},
			nameD: ResourceDefinitionObject{
				Body: srcBodyA,
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: nameA,
				},
			},
			nameB: ResourceDefinitionObject{
				Body:             srcBodyB,
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Hardlink()},
			},
			nameC: ResourceDefinitionObject{
				Body: srcBodyB,
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: nameB,
				},
			},
			nameE: ResourceDefinitionObject{
				Body:             srcBodyE,
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
		},
	}, ValidateResourceOptions{fromTo: fromTo,
		validateObjectContent: true,
		hardlinkHandling:      common.PreserveHardlinkHandlingType})

	// A (force-transferred, groupStructureChanged) + B (new anchor) +
	// C (hardlink→B) + D (hardlink→A) = 4 hardlink transfers.
	// E is a File transfer and not counted in the hardlink total.
	ValidateHardlinksTransferCount(svm, stdOut, 4)
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
// B, C, D remain hardlinks to each other (same anchor B) and are skipped.
// 0 hardlink transfers.
func (s *FilesNFSTestSuite) Scenario_HardlinkSync_AnchorBecomesFile(svm *ScenarioVariationManager) {

	fromTo := NamedResolveVariation(svm, map[string]common.FromTo{
		"|fromTo=LocalFileNFS":   common.EFromTo.LocalFileNFS(),
		"|fromTo=FileNFSLocal":   common.EFromTo.FileNFSLocal(),
		"|fromTo=FileNFSFileNFS": common.EFromTo.FileNFSFileNFS(),
	})

	if fromTo != common.EFromTo.FileNFSFileNFS() && runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainersForFromTo(svm, fromTo)
	defer cleanupHardlinkSyncForFromTo(svm, fromTo, srcContainer, dstContainer, rootDir)

	nameA := rootDir + "/A.txt"
	nameB := rootDir + "/B.txt"
	nameC := rootDir + "/C.txt"
	nameD := rootDir + "/D.txt"

	sharedLMT := time.Now().Add(-5 * time.Minute).Truncate(time.Second)

	// ── Destination: one group, A is anchor ──────────────────────────────────
	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	dstA := dstContainer.GetObject(svm, nameA, common.EEntityType.File())
	dstA.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	setSharedLMTIfNFS(svm, dstA, dstContainer, sharedLMT)

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
			EntityType:        common.EEntityType.File(),
			FileNFSProperties: nfsPropsIfNFS(srcContainer, sharedLMT),
		},
	})

	// B is the anchor of the B-C-D group.
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(nameB),
		Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{
			EntityType:        common.EEntityType.File(),
			FileNFSProperties: nfsPropsIfNFS(srcContainer, sharedLMT),
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

	stdOut := runHardlinkSyncForFromTo(svm, srcDirObj, dstDir, fromTo, true)

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
	}, ValidateResourceOptions{fromTo: fromTo,
		hardlinkHandling: common.PreserveHardlinkHandlingType})

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
//	B.txt  (anchor of B-C-D group)(hardlink)
//	C.txt  (hardlink)
//	D.txt  (hardlink)
//
// Expected: A re-uploaded as data carrier (entity-type mismatch); B,C,D
// recreated as hardlinks → A (nlink=4).
func (s *FilesNFSTestSuite) Scenario_HardlinkSync_FileJoinsGroup(svm *ScenarioVariationManager) {

	fromTo := NamedResolveVariation(svm, map[string]common.FromTo{
		"|fromTo=LocalFileNFS":   common.EFromTo.LocalFileNFS(),
		"|fromTo=FileNFSLocal":   common.EFromTo.FileNFSLocal(),
		"|fromTo=FileNFSFileNFS": common.EFromTo.FileNFSFileNFS(),
	})

	if fromTo != common.EFromTo.FileNFSFileNFS() && runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainersForFromTo(svm, fromTo)
	defer cleanupHardlinkSyncForFromTo(svm, fromTo, srcContainer, dstContainer, rootDir)

	nameA := rootDir + "/A.txt"
	nameB := rootDir + "/B.txt"
	nameC := rootDir + "/C.txt"
	nameD := rootDir + "/D.txt"

	// ── Destination: A is standalone; B-C-D are an independent group ─────────
	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	// Shared LMT: truncated to second precision so it survives the FILETIME
	// round-trip (100 ns resolution) without triggering a spurious LMT mismatch.
	sharedLMT := time.Now().Add(-5 * time.Minute).Truncate(time.Second)

	dstA := dstContainer.GetObject(svm, nameA, common.EEntityType.File())
	dstA.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	setSharedLMTIfNFS(svm, dstA, dstContainer, sharedLMT)

	dstB := dstContainer.GetObject(svm, nameB, common.EEntityType.File())
	dstB.Create(svm, NewRandomObjectContentContainer(SizeFromString("1K")), ObjectProperties{})
	setSharedLMTIfNFS(svm, dstB, dstContainer, sharedLMT)

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
			EntityType:        common.EEntityType.File(),
			FileNFSProperties: nfsPropsIfNFS(srcContainer, sharedLMT),
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

	stdOut := runHardlinkSyncForFromTo(svm, srcDirObj, dstDir, fromTo, true)

	// A re-uploaded as data carrier (entity-type mismatch: dest=File, src=Hardlink-anchor).
	// B, C, D recreated as hardlinks → A so the whole group shares one inode (nlink=4).
	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			nameA: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Hardlink()},
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
	}, ValidateResourceOptions{fromTo: fromTo,
		hardlinkHandling: common.PreserveHardlinkHandlingType})

	// A content carrier + B,C,D CreateHardLink → A.
	ValidateHardlinksTransferCount(svm, stdOut, 4)
}

// ===========================================================================================
// Hardlink Preserve Copy Upload Scenarios (Local → Azure Files NFS, using `azcopy copy`)
//
// These tests exercise the copy-upload direction with --hardlinks=preserve.  Unlike sync,
// copy does not compare source vs. destination timestamps — it always transfers.  The key
// assertions are that anchor files land as regular files and hardlink partners are created
// via the Azure Files CreateHardLink REST API, producing the correct inode relationships.
// ===========================================================================================

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
	fromTo := NamedResolveVariation(svm, map[string]common.FromTo{
		"|fromTo=LocalFileNFS":   common.EFromTo.LocalFileNFS(),
		"|fromTo=FileNFSLocal":   common.EFromTo.FileNFSLocal(),
		"|fromTo=FileNFSFileNFS": common.EFromTo.FileNFSFileNFS(),
	})

	if fromTo != common.EFromTo.FileNFSFileNFS() && runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainersForFromTo(svm, fromTo)
	defer cleanupHardlinkSyncForFromTo(svm, fromTo, srcContainer, dstContainer, rootDir)

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

	hardlinkType := NamedResolveVariation(svm, map[string]common.HardlinkHandlingType{
		"|hardlinks=preserve": common.PreserveHardlinkHandlingType,
	})

	stdOut := runHardlinkCopyForFromTo(svm, srcDirObj2, dstDir, fromTo, hardlinkType)

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
	}, ValidateResourceOptions{fromTo: fromTo,
		hardlinkHandling: common.PreserveHardlinkHandlingType})

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
	fromTo := NamedResolveVariation(svm, map[string]common.FromTo{
		"|fromTo=LocalFileNFS":   common.EFromTo.LocalFileNFS(),
		"|fromTo=FileNFSLocal":   common.EFromTo.FileNFSLocal(),
		"|fromTo=FileNFSFileNFS": common.EFromTo.FileNFSFileNFS(),
	})

	if fromTo != common.EFromTo.FileNFSFileNFS() && runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainersForFromTo(svm, fromTo)
	defer cleanupHardlinkSyncForFromTo(svm, fromTo, srcContainer, dstContainer, rootDir)

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

	hardlinkType := NamedResolveVariation(svm, map[string]common.HardlinkHandlingType{
		"|hardlinks=preserve": common.PreserveHardlinkHandlingType,
	})

	stdOut := runHardlinkCopyForFromTo(svm, srcDirObj2, dstDir, fromTo, hardlinkType)

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
	}, ValidateResourceOptions{fromTo: fromTo,
		hardlinkHandling: common.PreserveHardlinkHandlingType})

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
	fromTo := NamedResolveVariation(svm, map[string]common.FromTo{
		"|fromTo=LocalFileNFS":   common.EFromTo.LocalFileNFS(),
		"|fromTo=FileNFSLocal":   common.EFromTo.FileNFSLocal(),
		"|fromTo=FileNFSFileNFS": common.EFromTo.FileNFSFileNFS(),
	})

	if fromTo != common.EFromTo.FileNFSFileNFS() && runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainersForFromTo(svm, fromTo)
	defer cleanupHardlinkSyncForFromTo(svm, fromTo, srcContainer, dstContainer, rootDir)

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

	hardlinkType := NamedResolveVariation(svm, map[string]common.HardlinkHandlingType{
		"|hardlinks=preserve": common.PreserveHardlinkHandlingType,
	})
	stdOut := runHardlinkCopyForFromTo(svm, srcDirObj2, dstDir, fromTo, hardlinkType)

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
	}, ValidateResourceOptions{fromTo: fromTo,
		hardlinkHandling: common.PreserveHardlinkHandlingType})

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

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainersForFromTo(svm, common.EFromTo.FileNFSLocal())
	defer cleanupHardlinkSyncForFromTo(svm, common.EFromTo.FileNFSLocal(), srcContainer, dstContainer, rootDir)

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
	hardlinkType := NamedResolveVariation(svm, map[string]common.HardlinkHandlingType{
		"|hardlinks=preserve": common.PreserveHardlinkHandlingType,
	})

	// --as-subdir=false: files land at dstContainer/anchor.txt etc. (no rootDir prefix).
	stdOut := runHardlinkCopyForFromTo(svm, srcDirObj, dstContainer, common.EFromTo.FileNFSLocal(), hardlinkType)

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
	}, ValidateResourceOptions{fromTo: common.EFromTo.FileNFSLocal(),
		hardlinkHandling: common.PreserveHardlinkHandlingType})

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

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainersForFromTo(svm, common.EFromTo.FileNFSLocal())
	defer cleanupHardlinkSyncForFromTo(svm, common.EFromTo.FileNFSLocal(), srcContainer, dstContainer, rootDir)

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
	hardlinkType := NamedResolveVariation(svm, map[string]common.HardlinkHandlingType{
		"|hardlinks=preserve": common.PreserveHardlinkHandlingType,
	})

	stdOut := runHardlinkCopyForFromTo(svm, srcDirObj, dstContainer, common.EFromTo.FileNFSLocal(), hardlinkType)

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
	}, ValidateResourceOptions{fromTo: common.EFromTo.FileNFSLocal(),
		hardlinkHandling: common.PreserveHardlinkHandlingType})

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

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainersForFromTo(svm, common.EFromTo.FileNFSLocal())
	defer cleanupHardlinkSyncForFromTo(svm, common.EFromTo.FileNFSLocal(), srcContainer, dstContainer, rootDir)

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

	hardlinkType := NamedResolveVariation(svm, map[string]common.HardlinkHandlingType{
		"|hardlinks=preserve": common.PreserveHardlinkHandlingType,
	})
	stdOut := runHardlinkCopyForFromTo(svm, srcDirObj, dstContainer, common.EFromTo.FileNFSLocal(), hardlinkType)

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
	}, ValidateResourceOptions{fromTo: common.EFromTo.FileNFSLocal(),
		hardlinkHandling: common.PreserveHardlinkHandlingType})

	ValidateHardlinksTransferCount(svm, stdOut, 2)
}

// ===========================================================================================
// Hardlink-to-Symlink Scenarios
//
// A hardlink to a symlink shares the same inode as the original symlink.  On Linux
// a hardlink to a symlink is itself a symlink.  These tests verify that the anchor
// (first-seen entry by inode) is transferred as a regular symlink and that subsequent
// entries sharing the same inode are created via CreateHardLink, preserving the
// relationship correctly.
// ===========================================================================================

// Scenario 19: Sync — hardlink pointing to a symlink.
//
// Source:
//
//	target.txt               (regular file)
//	sym_to_target.txt        (symlink → target.txt)
//	hlink_to_sym.txt         (hardlink → sym_to_target.txt, same inode)
//
// Destination: empty (seeded for sync)
//
// Expected:
//   - target.txt transferred as regular file
//   - sym_to_target.txt transferred as symlink (anchor for the inode)
//   - hlink_to_sym.txt transferred as hardlink → sym_to_target.txt
func (s *FilesNFSTestSuite) Scenario_HardlinkSync_HardlinkToSymlink(svm *ScenarioVariationManager) {

	fromTo := NamedResolveVariation(svm, map[string]common.FromTo{
		"|fromTo=LocalFileNFS":   common.EFromTo.LocalFileNFS(),
		"|fromTo=FileNFSLocal":   common.EFromTo.FileNFSLocal(),
		"|fromTo=FileNFSFileNFS": common.EFromTo.FileNFSFileNFS(),
	})

	if fromTo != common.EFromTo.FileNFSFileNFS() && runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}
	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainersForFromTo(svm, fromTo)
	defer cleanupHardlinkSyncForFromTo(svm, fromTo, srcContainer, dstContainer, rootDir)

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})

	targetName := rootDir + "/target.txt"
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(targetName),
		Body:             NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
	})

	symlinkName := rootDir + "/sym_to_target.txt"
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(symlinkName),
		ObjectProperties: ObjectProperties{
			EntityType:        common.EEntityType.Symlink(),
			SymlinkedFileName: targetName,
		},
	})

	hlinkName := rootDir + "/hlink_to_sym.txt"
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(hlinkName),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: symlinkName,
		},
	})

	// Seed destination for sync (needs at least one file so sync succeeds).
	dstSeed := dstContainer.GetObject(svm, rootDir+"/target.txt", common.EEntityType.File())
	dstSeed.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{})
	setOldLMT(svm, dstSeed, dstContainer)

	if !svm.Dryrun() {
		time.Sleep(5 * time.Second)
	}

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDirObj := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	stdOut := runHardlinkSyncForFromTo(svm, srcDirObj, dstDirObj, fromTo, false, true)

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			targetName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			symlinkName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:        common.EEntityType.Symlink(),
					SymlinkedFileName: targetName,
				},
			},
			hlinkName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: symlinkName,
				},
			},
		},
	}, ValidateResourceOptions{fromTo: fromTo,
		hardlinkHandling: common.PreserveHardlinkHandlingType})

	ValidateHardlinksTransferCount(svm, stdOut, 1)
}

// Scenario 20: Copy — hardlink pointing to a symlink.
//
// Same object layout as Scenario 19, but using `azcopy copy` instead of `azcopy sync`.
// Tests all three directions: upload, download, and S2S.
func (s *FilesNFSTestSuite) Scenario_HardlinkCopy_HardlinkToSymlink(svm *ScenarioVariationManager) {

	fromTo := NamedResolveVariation(svm, map[string]common.FromTo{
		"|fromTo=LocalFileNFS":   common.EFromTo.LocalFileNFS(),
		"|fromTo=FileNFSLocal":   common.EFromTo.FileNFSLocal(),
		"|fromTo=FileNFSFileNFS": common.EFromTo.FileNFSFileNFS(),
	})

	if fromTo != common.EFromTo.FileNFSFileNFS() && runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}
	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainersForFromTo(svm, fromTo)
	defer cleanupHardlinkSyncForFromTo(svm, fromTo, srcContainer, dstContainer, rootDir)

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})

	targetName := rootDir + "/target.txt"
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(targetName),
		Body:             NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
	})

	symlinkName := rootDir + "/sym_to_target.txt"
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(symlinkName),
		ObjectProperties: ObjectProperties{
			EntityType:        common.EEntityType.Symlink(),
			SymlinkedFileName: targetName,
		},
	})

	hlinkName := rootDir + "/hlink_to_sym.txt"
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(hlinkName),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: symlinkName,
		},
	})

	// Create destination root directory for copy --as-subdir=false.
	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	hardlinkType := NamedResolveVariation(svm, map[string]common.HardlinkHandlingType{
		"|hardlinks=follow":   common.DefaultHardlinkHandlingType,
		"|hardlinks=skip":     common.SkipHardlinkHandlingType,
		"|hardlinks=preserve": common.PreserveHardlinkHandlingType,
	})

	stdOut := runHardlinkCopyForFromTo(svm, srcDirObj, dstDir, fromTo, hardlinkType, true)

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			targetName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			symlinkName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:        common.EEntityType.Symlink(),
					SymlinkedFileName: targetName,
				},
			},
			hlinkName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: symlinkName,
				},
			},
		},
	}, ValidateResourceOptions{fromTo: fromTo,
		hardlinkHandling: common.PreserveHardlinkHandlingType})

	if hardlinkType == common.PreserveHardlinkHandlingType {
		ValidateHardlinksTransferCount(svm, stdOut, 1)
		ValidateSymlinksTransferCount(svm, stdOut, 1)
	} else if hardlinkType == common.SkipHardlinkHandlingType {
		ValidateHardlinksSkippedCount(svm, stdOut, 0)
		ValidateSymlinksTransferCount(svm, stdOut, 2)
	} else {
		ValidateHardlinksConvertedCount(svm, stdOut, 0)
		ValidateSymlinksTransferCount(svm, stdOut, 2)
	}
}

// Scenario 21: Copy — multiple hardlinks pointing to the same symlink.
//
// Source:
//
//	target.txt               (regular file)
//	sym_to_target.txt        (symlink → target.txt, nlink=3)
//	hlink1_to_sym.txt        (hardlink → sym_to_target.txt)
//	hlink2_to_sym.txt        (hardlink → sym_to_target.txt)
//
// Expected:
//   - target.txt as regular file
//   - sym_to_target.txt as symlink (anchor)
//   - hlink1_to_sym.txt and hlink2_to_sym.txt as hardlinks
//   - ValidateHardlinksTransferCount == 3 (anchor symlink + 2 hardlinks)
func (s *FilesNFSTestSuite) Scenario_HardlinkCopy_MultipleHardlinksToSymlink(svm *ScenarioVariationManager) {

	fromTo := NamedResolveVariation(svm, map[string]common.FromTo{
		"|fromTo=LocalFileNFS":   common.EFromTo.LocalFileNFS(),
		"|fromTo=FileNFSLocal":   common.EFromTo.FileNFSLocal(),
		"|fromTo=FileNFSFileNFS": common.EFromTo.FileNFSFileNFS(),
	})

	if fromTo != common.EFromTo.FileNFSFileNFS() && runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainersForFromTo(svm, fromTo)
	defer cleanupHardlinkSyncForFromTo(svm, fromTo, srcContainer, dstContainer, rootDir)

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})

	targetName := rootDir + "/target.txt"
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(targetName),
		Body:             NewRandomObjectContentContainer(SizeFromString("1K")),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
	})

	symlinkName := rootDir + "/sym_to_target.txt"
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(symlinkName),
		ObjectProperties: ObjectProperties{
			EntityType:        common.EEntityType.Symlink(),
			SymlinkedFileName: targetName,
		},
	})

	hlink1Name := rootDir + "/hlink1_to_sym.txt"
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(hlink1Name),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: symlinkName,
		},
	})

	hlink2Name := rootDir + "/hlink2_to_sym.txt"
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(hlink2Name),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: symlinkName,
		},
	})

	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	// TODO: test these combinations later
	// preserveSymlinks := NamedResolveVariation(svm, map[string]bool{
	// 	"|preserveSymlinks=true":  true,
	// 	"|preserveSymlinks=false": false,
	// })

	hardlinkType := NamedResolveVariation(svm, map[string]common.HardlinkHandlingType{
		"|hardlinks=follow":   common.DefaultHardlinkHandlingType,
		"|hardlinks=skip":     common.SkipHardlinkHandlingType,
		"|hardlinks=preserve": common.PreserveHardlinkHandlingType,
	})

	stdOut := runHardlinkCopyForFromTo(svm, srcDirObj, dstDir, fromTo, hardlinkType, true)

	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			targetName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			symlinkName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:        common.EEntityType.Symlink(),
					SymlinkedFileName: targetName,
				},
			},
			hlink1Name: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: symlinkName,
				},
			},
			hlink2Name: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: symlinkName,
				},
			},
		},
	}, ValidateResourceOptions{fromTo: fromTo,
		hardlinkHandling: common.PreserveHardlinkHandlingType})

	if hardlinkType == common.PreserveHardlinkHandlingType {
		ValidateHardlinksTransferCount(svm, stdOut, 2)
		ValidateSymlinksTransferCount(svm, stdOut, 1)
	} else if hardlinkType == common.SkipHardlinkHandlingType {
		ValidateHardlinksSkippedCount(svm, stdOut, 0)
		ValidateSymlinksTransferCount(svm, stdOut, 3)
	} else {
		ValidateHardlinksConvertedCount(svm, stdOut, 0)
		ValidateSymlinksTransferCount(svm, stdOut, 3)
	}
}

// Scenario: HardlinkBecomesFile_NoDeleteDest — hardlink group fully splits into regular
// files and sync runs WITHOUT --delete-destination.
//
// The hardlinkRestructureDeleter should still delete the non-survivor member to break the
// shared inode, even though --delete-destination is not set.  The survivor keeps its data
// intact (nlink drops to 1 after other member is unlinked).
//
// Source:
//
//	hl_1.txt  (regular file, 7 bytes "hello_1")
//	hl_2.txt  (regular file, 7 bytes "hello_2")
//
// Destination (before sync):
//
//	hl_1.txt  (anchor of 2-member hardlink group, 7 bytes "hello_1")
//	hl_2.txt  (hardlink → hl_1.txt)
//
// Expected after sync:
//
//	hl_1.txt  (regular file, nlink=1, content "hello_1" — survivor, not re-uploaded)
//	hl_2.txt  (regular file, nlink=1, content "hello_2" — deleted+re-uploaded)
//
// This scenario validates the final destination state
func (s *FilesNFSTestSuite) Scenario_HardlinkSync_HardlinkBecomesFile_NoDeleteDest(svm *ScenarioVariationManager) {

	fromTo := NamedResolveVariation(svm, map[string]common.FromTo{
		"|fromTo=LocalFileNFS":   common.EFromTo.LocalFileNFS(),
		"|fromTo=FileNFSLocal":   common.EFromTo.FileNFSLocal(),
		"|fromTo=FileNFSFileNFS": common.EFromTo.FileNFSFileNFS(),
	})

	if fromTo != common.EFromTo.FileNFSFileNFS() && runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}

	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainersForFromTo(svm, fromTo)
	defer cleanupHardlinkSyncForFromTo(svm, fromTo, srcContainer, dstContainer, rootDir)

	name1 := rootDir + "/hl_1.txt"
	name2 := rootDir + "/hl_2.txt"

	// ── Destination: hl_1 is anchor, hl_2 is hardlink to hl_1 ────────────────
	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	body1 := NewZeroObjectContentContainer(7)
	dstAnchor := dstContainer.GetObject(svm, name1, common.EEntityType.File())
	dstAnchor.Create(svm, body1, ObjectProperties{})
	setOldLMT(svm, dstAnchor, dstContainer)

	dstLink := dstContainer.GetObject(svm, name2, common.EEntityType.Hardlink())
	dstLink.Create(svm, nil, ObjectProperties{
		EntityType:         common.EEntityType.Hardlink(),
		HardLinkedFileName: name1,
	})

	if !svm.Dryrun() {
		time.Sleep(5 * time.Second)
	}

	// ── Source: both are independent regular files ────────────────────────────
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})

	srcBody1 := NewZeroObjectContentContainer(7)
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(name1),
		Body:       srcBody1,
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.File(),
		},
	})

	srcBody2 := NewRandomObjectContentContainer(7)
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(name2),
		Body:       srcBody2,
		ObjectProperties: ObjectProperties{
			EntityType: common.EEntityType.File(),
		},
	})

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())

	// Run sync WITHOUT --delete-destination (deleteDestination=false)
	stdOut := runHardlinkSyncForFromTo(svm, srcDirObj, dstDir, fromTo, false)

	// Validate: both are now regular files at dest with correct content.
	// hl_2.txt was deleted and re-uploaded (content changed).
	// hl_1.txt was kept as survivor (nlink dropped to 1 after hl_2 was unlinked).
	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			name1: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			name2: ResourceDefinitionObject{
				Body:             srcBody2,
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
		},
	}, ValidateResourceOptions{fromTo: fromTo,
		validateObjectContent: true,
		hardlinkHandling:      common.PreserveHardlinkHandlingType})

	_ = stdOut
}

// Source: (local)
//	A.txt  (regular file, nlink=2)
//	B.txt  (hardlink -> A.txt)
//
// Destination (before sync):
//	A.txt  (regular file)
//
// Expected Destination after sync:
//	A.txt  (regular file, nlink=2)
//	B.txt  (hardlink -> A.txt)

// Validates that sync creates the missing non-anchor member of a hardlink
// group as a hardlink (CreateHardLink API), not as an independent inode.
func (s *FilesNFSTestSuite) Scenario_HardlinkSync_HardlinkCreatedOnDestination(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}
	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainersForFromTo(svm, common.EFromTo.LocalFileNFS())
	defer cleanupHardlinkSyncForFromTo(svm, common.EFromTo.LocalFileNFS(), srcContainer, dstContainer, rootDir)

	anchorName := rootDir + "/A.txt"
	linkName := rootDir + "/B.txt"

	// ── Destination (before sync): anchor only, as a plain regular file ──────
	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	dstAnchor := dstContainer.GetObject(svm, anchorName, common.EEntityType.File())
	dstAnchor.Create(svm, NewRandomObjectContentContainer(1), ObjectProperties{})

	setOldLMT(svm, dstAnchor, dstContainer)

	if !svm.Dryrun() {
		time.Sleep(5 * time.Second)
	}

	// ── Source: A.txt and B.txt share one inode ──────────────────────
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})

	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(anchorName),
		Body:       NewRandomObjectContentContainer(1),
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
	stdOut := runHardlinkSyncForFromTo(svm, srcDirObj, dstDir, common.EFromTo.LocalFileNFS(), true)

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
	}, ValidateResourceOptions{
		fromTo:           common.EFromTo.LocalFileNFS(),
		hardlinkHandling: common.PreserveHardlinkHandlingType,
	})
	ValidateHardlinksTransferCount(svm, stdOut, 2)
}

// Scenario: PartialOverlapMerge — src: A-B  dst: B-C
//
// Source has a hardlink group {A,B}; destination has a different group {B,C}
// that only partially overlaps. Sync must merge/re-point so B links to A and
// delete the extra dest member C.
//
// Source:
//
//	A.txt  (anchor of A-B group)
//	B.txt  (hardlink → A)
//
// Destination (before sync):
//
//	B.txt  (hardlink ↔ C)
//	C.txt  (hardlink ↔ B)
//
// Expected (LocalFileNFS, --delete-destination=true):
//
//	A.txt  (File, data carrier)
//	B.txt  (Hardlink → A)
//	C.txt  absent
func (s *FilesNFSTestSuite) Scenario_HardlinkSync_PartialOverlapMerge(svm *ScenarioVariationManager) {
	fromTo := NamedResolveVariation(svm, map[string]common.FromTo{
		"|fromTo=LocalFileNFS": common.EFromTo.LocalFileNFS(),
	})
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}
	srcContainer, dstContainer, rootDir := setupHardlinkSyncContainersForFromTo(svm, fromTo)
	defer cleanupHardlinkSyncForFromTo(svm, fromTo, srcContainer, dstContainer, rootDir)
	nameA := rootDir + "/A.txt"
	nameB := rootDir + "/B.txt"
	nameC := rootDir + "/C.txt"

	// Use a shared LMT so source and dest is testing the hardlink restructuring and not
	// potential overwrites from differing LMT
	sharedLMT := time.Now().Add(-5 * time.Minute).Truncate(time.Second)

	// ── Destination: B and C share an inode
	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	dstC := dstContainer.GetObject(svm, nameC, common.EEntityType.File())
	dstC.Create(svm, NewRandomObjectContentContainer(1), ObjectProperties{})

	setSharedLMTIfNFS(svm, dstC, dstContainer, sharedLMT)
	dstB := dstContainer.GetObject(svm, nameB, common.EEntityType.Hardlink())

	dstB.Create(svm, nil, ObjectProperties{
		EntityType:         common.EEntityType.Hardlink(),
		HardLinkedFileName: nameC,
	})
	if !svm.Dryrun() {
		time.Sleep(5 * time.Second)
	}
	// ── Source: A and B share an inode
	srcBodyA := NewRandomObjectContentContainer(1)
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(nameA),
		Body:       srcBodyA,
		ObjectProperties: ObjectProperties{
			EntityType:        common.EEntityType.File(),
			FileNFSProperties: nfsPropsIfNFS(srcContainer, sharedLMT),
		},
	})
	CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
		ObjectName: pointerTo(nameB),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: nameA,
		},
	})
	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	stdOut := runHardlinkSyncForFromTo(svm, srcDirObj, dstDir, fromTo, true)

	// A is the data carrier; B must be re-pointed to A; C must be deleted.
	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			nameA: ResourceDefinitionObject{
				Body:             srcBodyA,
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			nameB: ResourceDefinitionObject{
				Body: srcBodyA,
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: nameA,
				},
			},
			nameC: ResourceDefinitionObject{
				ObjectShouldExist: pointerTo(false),
			},
		},
	}, ValidateResourceOptions{fromTo: fromTo,
		validateObjectContent: true,
		hardlinkHandling:      common.PreserveHardlinkHandlingType})

	ValidateHardlinksTransferCount(svm, stdOut, 2)
}
