package e2etest

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"time"

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

// diverseSourceSet holds the relative names of objects created by
// createDiverseSourceSet.  All names include the rootDir prefix.
type diverseSourceSet struct {
	rootDir   string
	subDir    string // subfolder
	fileA     string // regular file
	fileB     string // regular file
	fileC     string // regular file in subDir
	hardlinkA string // hardlink → fileA
	hardlinkB string // hardlink → fileB
	symlinkA  string // symlink → fileA
	pipe      string // special file (FIFO)
}

// flat returns a copy with the rootDir prefix stripped from all names.
// Useful for download tests where the destination receives objects relative
// to the source directory.
func (d diverseSourceSet) flat() diverseSourceSet {
	p := d.rootDir + "/"
	return diverseSourceSet{
		subDir:    strings.TrimPrefix(d.subDir, p),
		fileA:     strings.TrimPrefix(d.fileA, p),
		fileB:     strings.TrimPrefix(d.fileB, p),
		fileC:     strings.TrimPrefix(d.fileC, p),
		hardlinkA: strings.TrimPrefix(d.hardlinkA, p),
		hardlinkB: strings.TrimPrefix(d.hardlinkB, p),
		symlinkA:  strings.TrimPrefix(d.symlinkA, p),
		pipe:      strings.TrimPrefix(d.pipe, p),
	}
}

// createDiverseSourceSet populates container with a diverse set of objects:
//
//	<rootDir>/                   (folder)
//	<rootDir>/subdir/            (subfolder)
//	<rootDir>/fileA.txt          (regular file, fileSize)
//	<rootDir>/fileB.txt          (regular file, fileSize)
//	<rootDir>/subdir/fileC.txt   (regular file, fileSize)
//	<rootDir>/link_to_A.txt      (hardlink → fileA.txt)
//	<rootDir>/link_to_B.txt      (hardlink → fileB.txt)
//	<rootDir>/sym_to_A.txt       (symlink  → fileA.txt)
//	<rootDir>/mypipe             (special file / FIFO — skipped during transfer)
func createDiverseSourceSet(
	svm *ScenarioVariationManager,
	container ContainerResourceManager,
	rootDir string,
	fileSize int64,
) diverseSourceSet {
	s := diverseSourceSet{
		rootDir:   rootDir,
		subDir:    rootDir + "/subdir",
		fileA:     rootDir + "/fileA.txt",
		fileB:     rootDir + "/fileB.txt",
		fileC:     rootDir + "/subdir/fileC.txt",
		hardlinkA: rootDir + "/link_to_A.txt",
		hardlinkB: rootDir + "/link_to_B.txt",
		symlinkA:  rootDir + "/sym_to_A.txt",
		pipe:      rootDir + "/mypipe",
	}

	// Root directory
	CreateResource[ObjectResourceManager](svm, container, ResourceDefinitionObject{
		ObjectName:       pointerTo(rootDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})
	// Subdirectory
	CreateResource[ObjectResourceManager](svm, container, ResourceDefinitionObject{
		ObjectName:       pointerTo(s.subDir),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()},
	})
	// Regular files
	CreateResource[ObjectResourceManager](svm, container, ResourceDefinitionObject{
		ObjectName:       pointerTo(s.fileA),
		Body:             NewRandomObjectContentContainer(fileSize),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
	})
	CreateResource[ObjectResourceManager](svm, container, ResourceDefinitionObject{
		ObjectName:       pointerTo(s.fileB),
		Body:             NewRandomObjectContentContainer(fileSize),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
	})
	CreateResource[ObjectResourceManager](svm, container, ResourceDefinitionObject{
		ObjectName:       pointerTo(s.fileC),
		Body:             NewRandomObjectContentContainer(fileSize),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
	})
	// Hardlinks
	CreateResource[ObjectResourceManager](svm, container, ResourceDefinitionObject{
		ObjectName: pointerTo(s.hardlinkA),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: s.fileA,
		},
	})
	CreateResource[ObjectResourceManager](svm, container, ResourceDefinitionObject{
		ObjectName: pointerTo(s.hardlinkB),
		ObjectProperties: ObjectProperties{
			EntityType:         common.EEntityType.Hardlink(),
			HardLinkedFileName: s.fileB,
		},
	})
	// Symlink
	CreateResource[ObjectResourceManager](svm, container, ResourceDefinitionObject{
		ObjectName: pointerTo(s.symlinkA),
		ObjectProperties: ObjectProperties{
			EntityType:        common.EEntityType.Symlink(),
			SymlinkedFileName: s.fileA,
		},
	})
	// Special file (FIFO) — will be skipped during transfer
	CreateResource[ObjectResourceManager](svm, container, ResourceDefinitionObject{
		ObjectName:       pointerTo(s.pipe),
		ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Other()},
	})

	return s
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

// validateDiverseResult validates that the destination has the full diverse
// object set: three regular files, a subfolder, two hardlinks, and a symlink.
// The special file (FIFO) is intentionally omitted because it is skipped during
// transfer.
func validateDiverseResult(
	svm *ScenarioVariationManager,
	dstContainer ContainerResourceManager,
	set diverseSourceSet,
	fromTo common.FromTo,
) {
	ValidateResource[ContainerResourceManager](svm, dstContainer, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			set.fileA: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			set.fileB: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			set.fileC: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{EntityType: common.EEntityType.File()},
			},
			set.hardlinkA: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: set.fileA,
				},
			},
			set.hardlinkB: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:         common.EEntityType.Hardlink(),
					HardLinkedFileName: set.fileB,
				},
			},
			set.symlinkA: ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:        common.EEntityType.Symlink(),
					SymlinkedFileName: set.fileA,
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

	fillNFSShareNearQuota(svm, dstContainer, 1000*common.MegaByte)

	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(
		svm, common.ELocation.Local()), ResourceDefinitionContainer{})
	srcSet := createDiverseSourceSet(svm, srcContainer, rootDir, 10*common.MegaByte)

	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	env := &AzCopyEnvironment{InheritEnvironment: map[string]bool{"*": true}}

	stdOut, _ := RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder()), authTarget(svm, dstDir)},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive:        pointerTo(true),
				FromTo:           pointerTo(common.EFromTo.LocalFileNFS()),
				HardlinkType:     pointerTo(common.PreserveHardlinkHandlingType),
				PreserveSymlinks: pointerTo(true),
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
	validateDiverseResult(svm, dstContainer, srcSet, common.EFromTo.LocalFileNFS())
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

	srcSet := createDiverseSourceSet(svm, srcContainer, rootDir, SizeFromString("1K"))

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
				Recursive:        pointerTo(true),
				FromTo:           pointerTo(common.EFromTo.FileNFSLocal()),
				HardlinkType:     pointerTo(common.PreserveHardlinkHandlingType),
				PreserveSymlinks: pointerTo(true),
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

	validateDiverseResult(svm, dstContainer, srcSet.flat(), common.EFromTo.FileNFSLocal())
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

	srcSet := createDiverseSourceSet(svm, srcContainer, rootDir, 10*common.MegaByte)

	dstContainer := setupNFSShareWithQuota(svm, 1)
	defer CleanupNFSDirectory(svm, dstContainer, rootDir)

	fillNFSShareNearQuota(svm, dstContainer, 1000*common.MegaByte)

	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	env := &AzCopyEnvironment{InheritEnvironment: map[string]bool{"*": true}}

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	stdOut, _ := RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{authTarget(svm, srcDirObj), authTarget(svm, dstDir)},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive:        pointerTo(true),
				FromTo:           pointerTo(common.EFromTo.FileNFSFileNFS()),
				HardlinkType:     pointerTo(common.PreserveHardlinkHandlingType),
				PreserveSymlinks: pointerTo(true),
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
	validateDiverseResult(svm, dstContainer, srcSet, common.EFromTo.FileNFSFileNFS())
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

	fillNFSShareNearQuota(svm, dstContainer, 1000*common.MegaByte)

	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(
		svm, common.ELocation.Local()), ResourceDefinitionContainer{})
	srcSet := createDiverseSourceSet(svm, srcContainer, rootDir, 10*common.MegaByte)

	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	env := &AzCopyEnvironment{InheritEnvironment: map[string]bool{"*": true}}

	stdOut, _ := RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbSync,
		Targets: []ResourceManager{srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder()), authTarget(svm, dstDir)},
		Flags: SyncFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive:        pointerTo(true),
				FromTo:           pointerTo(common.EFromTo.LocalFileNFS()),
				HardlinkType:     pointerTo(common.PreserveHardlinkHandlingType),
				PreserveSymlinks: pointerTo(true),
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
	validateDiverseResult(svm, dstContainer, srcSet, common.EFromTo.LocalFileNFS())
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

	srcSet := createDiverseSourceSet(svm, srcContainer, rootDir, SizeFromString("1K"))

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
				Recursive:        pointerTo(true),
				FromTo:           pointerTo(common.EFromTo.FileNFSLocal()),
				HardlinkType:     pointerTo(common.PreserveHardlinkHandlingType),
				PreserveSymlinks: pointerTo(true),
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

	validateDiverseResult(svm, dstContainer, srcSet.flat(), common.EFromTo.FileNFSLocal())
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

	srcSet := createDiverseSourceSet(svm, srcContainer, rootDir, 10*common.MegaByte)

	dstContainer := setupNFSShareWithQuota(svm, 1)
	defer CleanupNFSDirectory(svm, dstContainer, rootDir)

	fillNFSShareNearQuota(svm, dstContainer, 1000*common.MegaByte)

	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	env := &AzCopyEnvironment{InheritEnvironment: map[string]bool{"*": true}}

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	stdOut, _ := RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbSync,
		Targets: []ResourceManager{authTarget(svm, srcDirObj), authTarget(svm, dstDir)},
		Flags: SyncFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive:        pointerTo(true),
				FromTo:           pointerTo(common.EFromTo.FileNFSFileNFS()),
				HardlinkType:     pointerTo(common.PreserveHardlinkHandlingType),
				PreserveSymlinks: pointerTo(true),
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
	validateDiverseResult(svm, dstContainer, srcSet, common.EFromTo.FileNFSFileNFS())
}

// ===========================================================================
// Cancel + Resume Scenarios
//
// These tests start a transfer with --cancel-from-stdin and --cap-mbps to
// slow it down, cancel the job mid-flight by writing "cancel" to stdin,
// and then resume the cancelled job to completion.
// ===========================================================================

// cancelAfter returns an AfterStart callback that writes "cancel\n" to stdin
// after the given delay, triggering a graceful cancel via --cancel-from-stdin.
func cancelAfter(delay time.Duration) func(stdin io.WriteCloser) {
	return func(stdin io.WriteCloser) {
		go func() {
			time.Sleep(delay)
			_, _ = io.WriteString(stdin, "cancel\n")
		}()
	}
}

// ---------------------------------------------------------------------------
// Copy Cancel + Resume
// ---------------------------------------------------------------------------

// Scenario: Copy upload cancel + resume (Local → FileNFS).
//
// The transfer is throttled with --cap-mbps and cancelled via stdin after a
// short delay.  The resumed job must complete and preserve all hardlinks.
func (s *FilesNFSTestSuite) Scenario_HardlinkCopyCancel_Upload(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}
	if svm.Dryrun() {
		return
	}

	dstContainer := setupNFSShareWithQuota(svm, 100)
	rootDir := "hlcpycnlup_" + uuid.NewString()
	defer CleanupNFSDirectory(svm, dstContainer, rootDir)

	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(
		svm, common.ELocation.Local()), ResourceDefinitionContainer{})
	srcSet := createDiverseSourceSet(svm, srcContainer, rootDir, 50*common.MegaByte)

	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	env := &AzCopyEnvironment{InheritEnvironment: map[string]bool{"*": true}}

	stdOut, _ := RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder()), authTarget(svm, dstDir)},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive:        pointerTo(true),
				FromTo:           pointerTo(common.EFromTo.LocalFileNFS()),
				HardlinkType:     pointerTo(common.PreserveHardlinkHandlingType),
				PreserveSymlinks: pointerTo(true),
				GlobalFlags: GlobalFlags{
					CancelFromStdin: pointerTo(true),
					CapMbps:         pointerTo(float64(2)),
				},
			},
			AsSubdir: pointerTo(false),
		},
		AfterStart:  cancelAfter(10 * time.Second),
		ShouldFail:  true,
		Environment: env,
	})

	jobId := extractJobID(svm, stdOut)

	resStdOut, _ := RunAzCopy(svm, AzCopyCommand{
		Verb:           AzCopyVerbJobsResume,
		PositionalArgs: []string{jobId},
		Environment:    env,
	})
	assertResumeCompleted(svm, resStdOut)
	validateDiverseResult(svm, dstContainer, srcSet, common.EFromTo.LocalFileNFS())
}

// Scenario: Copy download cancel + resume (FileNFS → Local).
func (s *FilesNFSTestSuite) Scenario_HardlinkCopyCancel_Download(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}
	if svm.Dryrun() {
		return
	}

	srcContainer := getOrCreateNFSShare(svm, "hlcpycnldlsrc")
	rootDir := "hlcpycnldl_" + uuid.NewString()
	defer CleanupNFSDirectory(svm, srcContainer, rootDir)

	srcSet := createDiverseSourceSet(svm, srcContainer, rootDir, 50*common.MegaByte)

	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(
		svm, common.ELocation.Local()), ResourceDefinitionContainer{})

	env := &AzCopyEnvironment{InheritEnvironment: map[string]bool{"*": true}}

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	stdOut, _ := RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{authTarget(svm, srcDirObj), dstContainer},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive:        pointerTo(true),
				FromTo:           pointerTo(common.EFromTo.FileNFSLocal()),
				HardlinkType:     pointerTo(common.PreserveHardlinkHandlingType),
				PreserveSymlinks: pointerTo(true),
				GlobalFlags: GlobalFlags{
					CancelFromStdin: pointerTo(true),
					CapMbps:         pointerTo(float64(2)),
				},
			},
			AsSubdir: pointerTo(false),
		},
		AfterStart:  cancelAfter(10 * time.Second),
		ShouldFail:  true,
		Environment: env,
	})

	jobId := extractJobID(svm, stdOut)

	resStdOut, _ := RunAzCopy(svm, AzCopyCommand{
		Verb:           AzCopyVerbJobsResume,
		PositionalArgs: []string{jobId},
		Environment:    env,
	})
	assertResumeCompleted(svm, resStdOut)
	validateDiverseResult(svm, dstContainer, srcSet.flat(), common.EFromTo.FileNFSLocal())
}

// Scenario: Copy S2S cancel + resume (FileNFS → FileNFS).
func (s *FilesNFSTestSuite) Scenario_HardlinkCopyCancel_S2S(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}
	if svm.Dryrun() {
		return
	}

	srcContainer := getOrCreateNFSShare(svm, "hlcpycnls2ssrc")
	rootDir := "hlcpycnls2s_" + uuid.NewString()
	defer CleanupNFSDirectory(svm, srcContainer, rootDir)

	srcSet := createDiverseSourceSet(svm, srcContainer, rootDir, 50*common.MegaByte)

	dstContainer := setupNFSShareWithQuota(svm, 100)
	defer CleanupNFSDirectory(svm, dstContainer, rootDir)

	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	env := &AzCopyEnvironment{InheritEnvironment: map[string]bool{"*": true}}

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	stdOut, _ := RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{authTarget(svm, srcDirObj), authTarget(svm, dstDir)},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive:        pointerTo(true),
				FromTo:           pointerTo(common.EFromTo.FileNFSFileNFS()),
				HardlinkType:     pointerTo(common.PreserveHardlinkHandlingType),
				PreserveSymlinks: pointerTo(true),
				GlobalFlags: GlobalFlags{
					CancelFromStdin: pointerTo(true),
					CapMbps:         pointerTo(float64(2)),
				},
			},
			AsSubdir: pointerTo(false),
		},
		AfterStart:  cancelAfter(10 * time.Second),
		ShouldFail:  true,
		Environment: env,
	})

	jobId := extractJobID(svm, stdOut)

	resStdOut, _ := RunAzCopy(svm, AzCopyCommand{
		Verb:           AzCopyVerbJobsResume,
		PositionalArgs: []string{jobId},
		Environment:    env,
	})
	assertResumeCompleted(svm, resStdOut)
	validateDiverseResult(svm, dstContainer, srcSet, common.EFromTo.FileNFSFileNFS())
}

// ---------------------------------------------------------------------------
// Sync Cancel + Resume
// ---------------------------------------------------------------------------

// Scenario: Sync upload cancel + resume (Local → FileNFS).
func (s *FilesNFSTestSuite) Scenario_HardlinkSyncCancel_Upload(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}
	if svm.Dryrun() {
		return
	}

	dstContainer := setupNFSShareWithQuota(svm, 100)
	rootDir := "hlsyncnlup_" + uuid.NewString()
	defer CleanupNFSDirectory(svm, dstContainer, rootDir)

	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(
		svm, common.ELocation.Local()), ResourceDefinitionContainer{})
	srcSet := createDiverseSourceSet(svm, srcContainer, rootDir, 50*common.MegaByte)

	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	env := &AzCopyEnvironment{InheritEnvironment: map[string]bool{"*": true}}

	stdOut, _ := RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbSync,
		Targets: []ResourceManager{srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder()), authTarget(svm, dstDir)},
		Flags: SyncFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive:        pointerTo(true),
				FromTo:           pointerTo(common.EFromTo.LocalFileNFS()),
				HardlinkType:     pointerTo(common.PreserveHardlinkHandlingType),
				PreserveSymlinks: pointerTo(true),
				GlobalFlags: GlobalFlags{
					CancelFromStdin: pointerTo(true),
					CapMbps:         pointerTo(float64(2)),
				},
			},
		},
		AfterStart:  cancelAfter(10 * time.Second),
		ShouldFail:  true,
		Environment: env,
	})

	jobId := extractJobID(svm, stdOut)

	resStdOut, _ := RunAzCopy(svm, AzCopyCommand{
		Verb:           AzCopyVerbJobsResume,
		PositionalArgs: []string{jobId},
		Environment:    env,
	})
	assertResumeCompleted(svm, resStdOut)
	validateDiverseResult(svm, dstContainer, srcSet, common.EFromTo.LocalFileNFS())
}

// Scenario: Sync download cancel + resume (FileNFS → Local).
func (s *FilesNFSTestSuite) Scenario_HardlinkSyncCancel_Download(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}
	if svm.Dryrun() {
		return
	}

	srcContainer := getOrCreateNFSShare(svm, "hlsyncnldlsrc")
	rootDir := "hlsyncnldl_" + uuid.NewString()
	defer CleanupNFSDirectory(svm, srcContainer, rootDir)

	srcSet := createDiverseSourceSet(svm, srcContainer, rootDir, 50*common.MegaByte)

	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(
		svm, common.ELocation.Local()), ResourceDefinitionContainer{})

	// Seed destination so sync does not reject the empty target.
	dstSeed := dstContainer.GetObject(svm, "fileA.txt", common.EEntityType.File())
	dstSeed.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{})

	env := &AzCopyEnvironment{InheritEnvironment: map[string]bool{"*": true}}

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	stdOut, _ := RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbSync,
		Targets: []ResourceManager{authTarget(svm, srcDirObj), dstContainer},
		Flags: SyncFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive:        pointerTo(true),
				FromTo:           pointerTo(common.EFromTo.FileNFSLocal()),
				HardlinkType:     pointerTo(common.PreserveHardlinkHandlingType),
				PreserveSymlinks: pointerTo(true),
				GlobalFlags: GlobalFlags{
					CancelFromStdin: pointerTo(true),
					CapMbps:         pointerTo(float64(2)),
				},
			},
		},
		AfterStart:  cancelAfter(10 * time.Second),
		ShouldFail:  true,
		Environment: env,
	})

	jobId := extractJobID(svm, stdOut)

	// Remove the seed so the resumed job is free to write all files.
	_ = os.Remove(dstSeed.URI())

	resStdOut, _ := RunAzCopy(svm, AzCopyCommand{
		Verb:           AzCopyVerbJobsResume,
		PositionalArgs: []string{jobId},
		Environment:    env,
	})
	assertResumeCompleted(svm, resStdOut)
	validateDiverseResult(svm, dstContainer, srcSet.flat(), common.EFromTo.FileNFSLocal())
}

// Scenario: Sync S2S cancel + resume (FileNFS → FileNFS).
func (s *FilesNFSTestSuite) Scenario_HardlinkSyncCancel_S2S(svm *ScenarioVariationManager) {
	if runtime.GOOS != "linux" {
		svm.InvalidateScenario()
		return
	}
	if svm.Dryrun() {
		return
	}

	srcContainer := getOrCreateNFSShare(svm, "hlsyncnls2ssrc")
	rootDir := "hlsyncnls2s_" + uuid.NewString()
	defer CleanupNFSDirectory(svm, srcContainer, rootDir)

	srcSet := createDiverseSourceSet(svm, srcContainer, rootDir, 50*common.MegaByte)

	dstContainer := setupNFSShareWithQuota(svm, 100)
	defer CleanupNFSDirectory(svm, dstContainer, rootDir)

	dstDir := dstContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	dstDir.Create(svm, nil, ObjectProperties{EntityType: common.EEntityType.Folder()})

	env := &AzCopyEnvironment{InheritEnvironment: map[string]bool{"*": true}}

	srcDirObj := srcContainer.GetObject(svm, rootDir, common.EEntityType.Folder())
	stdOut, _ := RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbSync,
		Targets: []ResourceManager{authTarget(svm, srcDirObj), authTarget(svm, dstDir)},
		Flags: SyncFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive:        pointerTo(true),
				FromTo:           pointerTo(common.EFromTo.FileNFSFileNFS()),
				HardlinkType:     pointerTo(common.PreserveHardlinkHandlingType),
				PreserveSymlinks: pointerTo(true),
				GlobalFlags: GlobalFlags{
					CancelFromStdin: pointerTo(true),
					CapMbps:         pointerTo(float64(2)),
				},
			},
		},
		AfterStart:  cancelAfter(10 * time.Second),
		ShouldFail:  true,
		Environment: env,
	})

	jobId := extractJobID(svm, stdOut)

	resStdOut, _ := RunAzCopy(svm, AzCopyCommand{
		Verb:           AzCopyVerbJobsResume,
		PositionalArgs: []string{jobId},
		Environment:    env,
	})
	assertResumeCompleted(svm, resStdOut)
	validateDiverseResult(svm, dstContainer, srcSet, common.EFromTo.FileNFSFileNFS())
}
