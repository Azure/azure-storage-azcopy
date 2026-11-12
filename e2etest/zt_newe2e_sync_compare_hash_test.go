package e2etest

import (
	"path/filepath"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// Scenario_CompareHashUnreadableSourceFile syncs with --compare-hash while one source file can't be read
// (locked by another process on Windows, no permissions elsewhere). The rest of the sync must still finish.
func (s *SyncTestSuite) Scenario_CompareHashUnreadableSourceFile(svm *ScenarioVariationManager) {
	const unreadable = "unreadable.txt"
	readable := ObjectResourceMappingFlat{
		"readable-1.txt":        ResourceDefinitionObject{Body: NewRandomObjectContentContainer(512)},
		"folder/readable-2.txt": ResourceDefinitionObject{Body: NewRandomObjectContentContainer(512)},
	}
	sourceObjects := ObjectResourceMappingFlat{unreadable: ResourceDefinitionObject{Body: NewRandomObjectContentContainer(512)}}
	for name, obj := range readable {
		sourceObjects[name] = obj
	}

	source := CreateResource[ContainerResourceManager](svm, NewLocalContainer(svm), ResourceDefinitionContainer{Objects: sourceObjects})
	dest := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})

	release := func() {}
	if !svm.Dryrun() {
		var ok bool
		release, ok = makeFileUnreadable(svm, filepath.Join(source.URI(), unreadable))
		svm.Cleanup(func(Asserter) { release() }) // runs if the scenario stops early, before its resources are deleted
		if !ok {
			svm.Skip("this account can read files without permissions, so the file can't be made unreadable")
		}
	}

	stdout, _ := RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbSync,
		Targets: []ResourceManager{source, dest},
		Flags: SyncFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive: pointerTo(true),
			},
			CompareHash:          PtrOf(common.ESyncHashType.MD5()),
			LocalHashStorageMode: PtrOf(common.EHashStorageMode.HiddenFiles()),
		},
		ShouldFail: true, // the unreadable file's transfer fails, so AzCopy exits with an error
	})
	release() // before cleanup, which deletes the source folder

	ValidateMessageOutput(svm, stdout, "Failed to hash one or more files", true)
	if !svm.Dryrun() {
		summary := stdout.(*AzCopyParsedCopySyncRemoveStdout).FinalStatus
		svm.Assert("job status", Equal{}, summary.JobStatus, common.EJobStatus.CompletedWithErrors())
		svm.Assert("failed transfers", Equal{}, summary.TransfersFailed, uint32(1))
	}

	expected := ObjectResourceMappingFlat{unreadable: ResourceDefinitionObject{ObjectShouldExist: pointerTo(false)}}
	for name, obj := range readable {
		expected[name] = obj
	}
	ValidateResource[ContainerResourceManager](svm, dest, ResourceDefinitionContainer{Objects: expected}, ValidateResourceOptions{
		validateObjectContent: true,
	})
}
