package e2etest

import (
	"fmt"
	"os"
	"runtime"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/google/uuid"
)

// ===========================================================================================
// Hardlink Preserve Resume Scenarios
//
// These tests verify that resuming a previously failed job correctly preserves
// hardlink relationships for both copy and sync operations across all three
// transfer directions: upload (Local→NFS), download (NFS→Local), and S2S
// (NFS→NFS).
//
// For NFS-destination tests (upload, S2S) the share is filled near its quota
// so the first run fails; the quota is then increased before resume.
//
// For local-destination tests (download) the destination directory is made
// read-only so writes fail; permissions are restored before resume.
// ===========================================================================================

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// fillNFSShareNearQuota allocates a large filler file on the NFS share to
// consume the given number of bytes, leaving minimal headroom.
func fillNFSShareNearQuota(svm *ScenarioVariationManager, container ContainerResourceManager, fillBytes int64) {
	if svm.Dryrun() {
		return
	}
	fillerName := "filler_" + uuid.NewString() + ".dat"
	CreateResource[ObjectResourceManager](svm, container, ResourceDefinitionObject{
		ObjectName:       pointerTo(fillerName),
		Body:             NewRandomObjectContentContainer(fillBytes),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
	})
}

// setNFSShareQuota updates the quota (in GB) for an NFS file share.
func setNFSShareQuota(svm *ScenarioVariationManager, container ContainerResourceManager, quotaGB *int32) {
	if svm.Dryrun() {
		return
	}
	if rm, ok := container.(*FileShareResourceManager); ok {
		rm.SetProperties(svm, &ContainerProperties{
			FileContainerProperties: FileContainerProperties{
				Quota: quotaGB,
			},
		})
	} else {
		svm.NoError("set NFS share quota", fmt.Errorf("container is not a FileShareResourceManager"))
	}
}

// authTarget wraps a resource manager with SAS auth if it is remote.
func authTarget(svm *ScenarioVariationManager, rm ResourceManager) ResourceManager {
	if remote, ok := rm.(RemoteResourceManager); ok {
		return remote.WithSpecificAuthType(
			ResolveVariation(svm, []ExplicitCredentialTypes{
				EExplicitCredentialType.SASToken(),
			}), svm, CreateAzCopyTargetOptions{})
	}
	return rm
}

// setupNFSShareWithQuota creates a fresh NFS share with the given quota (GB).
func setupNFSShareWithQuota(svm *ScenarioVariationManager, quotaGB int32) ContainerResourceManager {
	return CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.FileNFS(), GetResourceOptions{
		PreferredAccount: pointerTo(PremiumFileShareAcct),
	}), ResourceDefinitionContainer{
		Properties: ContainerProperties{
			FileContainerProperties: FileContainerProperties{
				EnabledProtocols: pointerTo("NFS"),
				Quota:            &quotaGB,
			},
		},
	})
}

// getOrCreateNFSShare returns a named, pre-existing NFS share (creating it if
// needed).
func getOrCreateNFSShare(svm *ScenarioVariationManager, name string) ContainerResourceManager {
	c := GetRootResource(svm, common.ELocation.FileNFS(), GetResourceOptions{
		PreferredAccount: pointerTo(PremiumFileShareAcct),
	}).(ServiceResourceManager).GetContainer(name)
	if !c.Exists() {
		c.Create(svm, ContainerProperties{
			FileContainerProperties: FileContainerProperties{
				EnabledProtocols: pointerTo("NFS"),
			},
		})
	}
	return c
}

// createHardlinkSourceSet populates container with a root dir, two regular
// files, and one hardlink:
//
//	<rootDir>/fileA.txt          (regular file, fileSize)
//	<rootDir>/fileB.txt          (regular file, fileSize)
//	<rootDir>/link_to_A.txt      (hardlink → fileA.txt)
func createHardlinkSourceSet(
	svm *ScenarioVariationManager,
	container ContainerResourceManager,
	rootDir string,
	fileSize int64,
) (fileAName, fileBName, linkName string) {
	fileAName = rootDir + "/fileA.txt"
	fileBName = rootDir + "/fileB.txt"
	linkName = rootDir + "/link_to_A.txt"

	CreateResource[ObjectResourceManager](svm, container, ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})
	CreateResource[ObjectResourceManager](svm, container, ResourceDefinitionObject{
		ObjectName:       pointerTo(fileAName),
		Body:             NewRandomObjectContentContainer(fileSize),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
	})
	CreateResource[ObjectResourceManager](svm, container, ResourceDefinitionObject{
		ObjectName:       pointerTo(fileBName),
		Body:             NewRandomObjectContentContainer(fileSize),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
	})
	CreateResource[ObjectResourceManager](svm, container, ResourceDefinitionObject{
		ObjectName: pointerTo(linkName),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: fileAName,
		},
	})
	return
}

// extractJobID extracts the job identifier from the parsed stdout.
func extractJobID(svm *ScenarioVariationManager, stdOut AzCopyStdout) string {
	var jobId string
	if parsedOut, ok := stdOut.(*AzCopyParsedCopySyncRemoveStdout); ok {
		jobId = parsedOut.InitMsg.JobID
	}
	svm.Assert("job ID must be captured for resume", Not{Equal{}}, jobId, "")
	return jobId
}

// assertResumeCompleted asserts that a resumed job finished successfully.
func assertResumeCompleted(svm *ScenarioVariationManager, stdOut AzCopyStdout) {
	if svm.Dryrun() {
		return
	}
	resumeParsed, ok := stdOut.(*AzCopyParsedCopySyncRemoveStdout)
	svm.Assert("parse resume stdout", Equal{}, ok, true)
	svm.Assert("resume completed", Equal{}, resumeParsed.FinalStatus.JobStatus, common.EJobStatus.Completed())
}

// validateHardlinkResult validates the destination has the expected hardlink
// structure: fileA (file), fileB (file), link_to_A (hardlink → fileA).
func validateHardlinkResult(
	svm *ScenarioVariationManager,
	dstContainer ContainerResourceManager,
	fileAName, fileBName, linkName string,
	fromTo common.FromTo,
) {
	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			fileAName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			fileBName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			linkName: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: fileAName,
				},
			},
		},
	}, ValidateResourceOptions{
		fromTo:           fromTo,
		hardlinkHandling: common.PreserveHardlinkHandlingType,
	})
}

// ---------------------------------------------------------------------------
// Copy Resume Scenarios
// ---------------------------------------------------------------------------

// Scenario: Copy upload resume (Local → FileNFS).
//
// The NFS destination share is nearly full so the first run fails.
// After increasing the quota the job is resumed and all hardlinks are
// preserved.
func (s *FilesNFSTestSuite) Scenario_HardlinkCopyResume_Upload(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}
	if svm.Dryrun() {
		return
	}

	dstContainer := setupNFSShareWithQuota(svm, 1)
	rootDir := "hlcpyresup_" + uuid.NewString()
	defer CleanupNFSDirectory(svm, dstContainer, rootDir)

	fillNFSShareNearQuota(svm, dstContainer, 950*common.MegaByte)

	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(
		svm, common.ELocation.Local()), ResourceDefinitionContainer{})
	fileA, fileB, link := createHardlinkSourceSet(svm, srcContainer, rootDir, 10*common.MegaByte)

	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	env := &AzCopyEnvironment{InheritEnvironment: map[string]bool{"*": true}}

	stdOut, _ := RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder()), authTarget(svm, dstDir)},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive:    pointerTo(true),
				FromTo:       pointerTo(common.EFromTo.LocalFileNFS()),
				HardlinkType: pointerTo(common.PreserveHardlinkHandlingType),
			},
			AsSubdir: pointerTo(false),
		},
		ShouldFail:  true,
		Environment: env,
	})

	jobId := extractJobID(svm, stdOut)

	setNFSShareQuota(svm, dstContainer, pointerTo(int32(2)))

	resStdOut, _ := RunAzCopy(svm, AzCopyCommand{
		Verb:           AzCopyVerbJobsResume,
		PositionalArgs: []string{jobId},
		Environment:    env,
	})
	assertResumeCompleted(svm, resStdOut)
	validateHardlinkResult(svm, dstContainer, fileA, fileB, link, common.EFromTo.LocalFileNFS())
}

// Scenario: Copy download resume (FileNFS → Local).
//
// The local destination directory is made read-only so the first run
// fails.  After restoring write permissions the job is resumed.
func (s *FilesNFSTestSuite) Scenario_HardlinkCopyResume_Download(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}
	if svm.Dryrun() {
		return
	}

	srcContainer := getOrCreateNFSShare(svm, "hlcpyresdlsrc")
	rootDir := "hlcpyresdl_" + uuid.NewString()
	defer CleanupNFSDirectory(svm, srcContainer, rootDir)

	createHardlinkSourceSet(svm, srcContainer, rootDir, SizeFromString("1K"))

	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(
		svm, common.ELocation.Local()), ResourceDefinitionContainer{})

	// Make the destination directory read-only so downloads fail.
	dstPath := dstContainer.GetObject(svm, "", common.EEntityType.Folder()).URI()
	svm.NoError("chmod read-only", os.Chmod(dstPath, 0o555))

	env := &AzCopyEnvironment{InheritEnvironment: map[string]bool{"*": true}}

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	stdOut, _ := RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{authTarget(svm, srcDirObj), dstContainer},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive:    pointerTo(true),
				FromTo:       pointerTo(common.EFromTo.FileNFSLocal()),
				HardlinkType: pointerTo(common.PreserveHardlinkHandlingType),
			},
			AsSubdir: pointerTo(false),
		},
		ShouldFail:  true,
		Environment: env,
	})

	jobId := extractJobID(svm, stdOut)

	// Restore write permissions.
	svm.NoError("chmod writable", os.Chmod(dstPath, 0o755))

	resStdOut, _ := RunAzCopy(svm, AzCopyCommand{
		Verb:           AzCopyVerbJobsResume,
		PositionalArgs: []string{jobId},
		Environment:    env,
	})
	assertResumeCompleted(svm, resStdOut)

	validateHardlinkResult(svm, dstContainer,
		"fileA.txt", "fileB.txt", "link_to_A.txt",
		common.EFromTo.FileNFSLocal())
}

// Scenario: Copy S2S resume (FileNFS → FileNFS).
//
// The destination NFS share is nearly full so the first run fails.
// After increasing the quota the job is resumed.
func (s *FilesNFSTestSuite) Scenario_HardlinkCopyResume_S2S(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}
	if svm.Dryrun() {
		return
	}

	srcContainer := getOrCreateNFSShare(svm, "hlcpyress2ssrc")
	rootDir := "hlcpyress2s_" + uuid.NewString()
	defer CleanupNFSDirectory(svm, srcContainer, rootDir)

	fileA, fileB, link := createHardlinkSourceSet(svm, srcContainer, rootDir, 10*common.MegaByte)

	dstContainer := setupNFSShareWithQuota(svm, 1)
	defer CleanupNFSDirectory(svm, dstContainer, rootDir)

	fillNFSShareNearQuota(svm, dstContainer, 950*common.MegaByte)

	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	env := &AzCopyEnvironment{InheritEnvironment: map[string]bool{"*": true}}

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	stdOut, _ := RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{authTarget(svm, srcDirObj), authTarget(svm, dstDir)},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive:    pointerTo(true),
				FromTo:       pointerTo(common.EFromTo.FileNFSFileNFS()),
				HardlinkType: pointerTo(common.PreserveHardlinkHandlingType),
			},
			AsSubdir: pointerTo(false),
		},
		ShouldFail:  true,
		Environment: env,
	})

	jobId := extractJobID(svm, stdOut)

	setNFSShareQuota(svm, dstContainer, pointerTo(int32(2)))

	resStdOut, _ := RunAzCopy(svm, AzCopyCommand{
		Verb:           AzCopyVerbJobsResume,
		PositionalArgs: []string{jobId},
		Environment:    env,
	})
	assertResumeCompleted(svm, resStdOut)
	validateHardlinkResult(svm, dstContainer, fileA, fileB, link, common.EFromTo.FileNFSFileNFS())
}

// ---------------------------------------------------------------------------
// Sync Resume Scenarios
// ---------------------------------------------------------------------------

// Scenario: Sync upload resume (Local → FileNFS).
//
// The NFS destination share is nearly full so the first run fails.
// After increasing the quota the job is resumed.
func (s *FilesNFSTestSuite) Scenario_HardlinkSyncResume_Upload(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}
	if svm.Dryrun() {
		return
	}

	dstContainer := setupNFSShareWithQuota(svm, 1)
	rootDir := "hlsynresup_" + uuid.NewString()
	defer CleanupNFSDirectory(svm, dstContainer, rootDir)

	fillNFSShareNearQuota(svm, dstContainer, 950*common.MegaByte)

	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(
		svm, common.ELocation.Local()), ResourceDefinitionContainer{})
	fileA, fileB, link := createHardlinkSourceSet(svm, srcContainer, rootDir, 10*common.MegaByte)

	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	env := &AzCopyEnvironment{InheritEnvironment: map[string]bool{"*": true}}

	stdOut, _ := RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbSync,
		Targets: []ResourceManager{srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder()), authTarget(svm, dstDir)},
		Flags: SyncFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive:    pointerTo(true),
				FromTo:       pointerTo(common.EFromTo.LocalFileNFS()),
				HardlinkType: pointerTo(common.PreserveHardlinkHandlingType),
			},
		},
		ShouldFail:  true,
		Environment: env,
	})

	jobId := extractJobID(svm, stdOut)

	setNFSShareQuota(svm, dstContainer, pointerTo(int32(2)))

	resStdOut, _ := RunAzCopy(svm, AzCopyCommand{
		Verb:           AzCopyVerbJobsResume,
		PositionalArgs: []string{jobId},
		Environment:    env,
	})
	assertResumeCompleted(svm, resStdOut)
	validateHardlinkResult(svm, dstContainer, fileA, fileB, link, common.EFromTo.LocalFileNFS())
}

// Scenario: Sync download resume (FileNFS → Local).
//
// The local destination directory is made read-only so the first run
// fails.  After restoring write permissions the job is resumed.
func (s *FilesNFSTestSuite) Scenario_HardlinkSyncResume_Download(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}
	if svm.Dryrun() {
		return
	}

	srcContainer := getOrCreateNFSShare(svm, "hlsynresdlsrc")
	rootDir := "hlsynresdl_" + uuid.NewString()
	defer CleanupNFSDirectory(svm, srcContainer, rootDir)

	createHardlinkSourceSet(svm, srcContainer, rootDir, SizeFromString("1K"))

	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(
		svm, common.ELocation.Local()), ResourceDefinitionContainer{})

	// Seed destination so sync does not reject the empty target.
	dstSeed := dstContainer.GetObject(svm, "fileA.txt", common.EEntityType.File())
	dstSeed.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{})

	// Make the destination directory read-only so downloads fail.
	dstPath := dstContainer.GetObject(svm, "", common.EEntityType.Folder()).URI()
	svm.NoError("chmod read-only", os.Chmod(dstPath, 0o555))

	env := &AzCopyEnvironment{InheritEnvironment: map[string]bool{"*": true}}

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	stdOut, _ := RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbSync,
		Targets: []ResourceManager{authTarget(svm, srcDirObj), dstContainer},
		Flags: SyncFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive:    pointerTo(true),
				FromTo:       pointerTo(common.EFromTo.FileNFSLocal()),
				HardlinkType: pointerTo(common.PreserveHardlinkHandlingType),
			},
		},
		ShouldFail:  true,
		Environment: env,
	})

	jobId := extractJobID(svm, stdOut)

	// Restore write permissions.
	svm.NoError("chmod writable", os.Chmod(dstPath, 0o755))
	// Remove the seed so the resumed job is free to write all files.
	_ = os.Remove(dstSeed.URI())

	resStdOut, _ := RunAzCopy(svm, AzCopyCommand{
		Verb:           AzCopyVerbJobsResume,
		PositionalArgs: []string{jobId},
		Environment:    env,
	})
	assertResumeCompleted(svm, resStdOut)

	validateHardlinkResult(svm, dstContainer,
		"fileA.txt", "fileB.txt", "link_to_A.txt",
		common.EFromTo.FileNFSLocal())
}

// Scenario: Sync S2S resume (FileNFS → FileNFS).
//
// The destination NFS share is nearly full so the first run fails.
// After increasing the quota the job is resumed.
func (s *FilesNFSTestSuite) Scenario_HardlinkSyncResume_S2S(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}
	if svm.Dryrun() {
		return
	}

	srcContainer := getOrCreateNFSShare(svm, "hlsynress2ssrc")
	rootDir := "hlsynress2s_" + uuid.NewString()
	defer CleanupNFSDirectory(svm, srcContainer, rootDir)

	fileA, fileB, link := createHardlinkSourceSet(svm, srcContainer, rootDir, 10*common.MegaByte)

	dstContainer := setupNFSShareWithQuota(svm, 1)
	defer CleanupNFSDirectory(svm, dstContainer, rootDir)

	fillNFSShareNearQuota(svm, dstContainer, 950*common.MegaByte)

	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	env := &AzCopyEnvironment{InheritEnvironment: map[string]bool{"*": true}}

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	stdOut, _ := RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbSync,
		Targets: []ResourceManager{authTarget(svm, srcDirObj), authTarget(svm, dstDir)},
		Flags: SyncFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive:    pointerTo(true),
				FromTo:       pointerTo(common.EFromTo.FileNFSFileNFS()),
				HardlinkType: pointerTo(common.PreserveHardlinkHandlingType),
			},
		},
		ShouldFail:  true,
		Environment: env,
	})

	jobId := extractJobID(svm, stdOut)

	setNFSShareQuota(svm, dstContainer, pointerTo(int32(2)))

	resStdOut, _ := RunAzCopy(svm, AzCopyCommand{
		Verb:           AzCopyVerbJobsResume,
		PositionalArgs: []string{jobId},
		Environment:    env,
	})
	assertResumeCompleted(svm, resStdOut)
	validateHardlinkResult(svm, dstContainer, fileA, fileB, link, common.EFromTo.FileNFSFileNFS())
}
