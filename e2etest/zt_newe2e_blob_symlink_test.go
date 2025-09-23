package e2etest

import (
	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"path"
	"runtime"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type BlobSymlinkSuite struct{}

func init() {
	if runtime.GOOS != "windows" || GlobalConfig.E2EFrameworkSpecialConfig.OverrideWindowsSymlinkSkip {
		// Windows symlinks are very funny. They need special privileges to create.
		suiteManager.RegisterSuite(&BlobSymlinkSuite{})
	}
}

func (s *BlobSymlinkSuite) Scenario_TestPreserveSymlinks_IndirectSource(svm *ScenarioVariationManager) {
	srcLoc := ResolveVariation(svm, []common.Location{
		common.ELocation.Local(),
		common.ELocation.Blob(),
		common.ELocation.BlobFS(),
	})
	dstLoc := ResolveVariation(svm, []common.Location{
		common.ELocation.Local(),
		common.ELocation.Blob(),
		common.ELocation.BlobFS(),
	})

	if srcLoc == common.ELocation.Local() && dstLoc == common.ELocation.Local() {
		svm.InvalidateScenario()
		return
	}

	srcDef := ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"foo": ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:        common.EEntityType.Symlink(),
					SymlinkedFileName: "bar",
				},
			},
			"bar": ResourceDefinitionObject{
				Body: NewZeroObjectContentContainer(1024),
			},
		},
	}

	source := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, srcLoc), srcDef)

	dest := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, dstLoc), ResourceDefinitionContainer{})

	_, _ = RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbCopy, // sync doesn't support symlinks at this time
			Targets: []ResourceManager{
				source, dest,
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
				},
				PreserveSymlinks: pointerTo(true),
				AsSubdir:         pointerTo(false),
			},
		})

	if source.Location() == common.ELocation.Local() { // recalculate the symlink
		srcDir := source.URI()
		srcDef = ResourceDefinitionContainer{
			Objects: ObjectResourceMappingFlat{
				"foo": ResourceDefinitionObject{
					ObjectProperties: ObjectProperties{
						EntityType:        common.EEntityType.Symlink(),
						SymlinkedFileName: path.Join(srcDir, "bar"),
					},
				},
				"bar": ResourceDefinitionObject{
					Body: NewZeroObjectContentContainer(1024),
				},
			},
		}
	}

	ValidateResource(svm, dest, srcDef, true)
}

func (s *BlobSymlinkSuite) Scenario_TestPreserveSymlinks_DirectSource(svm *ScenarioVariationManager) {
	srcLoc := ResolveVariation(svm, []common.Location{
		common.ELocation.Local(),
		common.ELocation.Blob(),
		common.ELocation.BlobFS(),
	})
	dstLoc := ResolveVariation(svm, []common.Location{
		common.ELocation.Local(),
		common.ELocation.Blob(),
		common.ELocation.BlobFS(),
	})

	if srcLoc == common.ELocation.Local() && dstLoc == common.ELocation.Local() {
		svm.InvalidateScenario()
		return
	}

	srcDef := ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"foo": ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:        common.EEntityType.Symlink(),
					SymlinkedFileName: "bar",
				},
			},
			"bar": ResourceDefinitionObject{
				Body: NewZeroObjectContentContainer(1024),
			},
		},
	}

	source := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, srcLoc), srcDef)

	dest := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, dstLoc), ResourceDefinitionContainer{})

	_, _ = RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbCopy, // sync doesn't support symlinks at this time
			Targets: []ResourceManager{
				source.GetObject(svm, "foo", common.EEntityType.Symlink()), dest,
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
				},
				PreserveSymlinks: pointerTo(true),
				AsSubdir:         pointerTo(false),
			},
		})

	if source.Location() == common.ELocation.Local() { // recalculate the symlink
		srcDir := source.URI()
		srcDef = ResourceDefinitionContainer{
			Objects: ObjectResourceMappingFlat{
				"foo": ResourceDefinitionObject{
					ObjectProperties: ObjectProperties{
						EntityType:        common.EEntityType.Symlink(),
						SymlinkedFileName: path.Join(srcDir, "bar"),
					},
				},
			},
		}
	} else {
		delete(srcDef.Objects.(ObjectResourceMappingFlat), "bar")
	}

	ValidateResource(svm, dest, srcDef, true)
}

func (s *BlobSymlinkSuite) Scenario_TestDirectSourceFollowing(svm *ScenarioVariationManager) {
	srcBody := NewRandomObjectContentContainer(1024)

	toFollow := NamedResolveVariation(svm, map[string]*bool{
		"Follow": PtrOf(true),
		"Skip":   nil,
	})

	source := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"foo": ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:        common.EEntityType.Symlink(),
					SymlinkedFileName: "bar",
				},
			},
			"bar": ResourceDefinitionObject{
				Body: srcBody,
			},
		},
	})

	dest := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})

	stdOut, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbCopy, // sync doesn't support symlinks at this time
			Targets: []ResourceManager{
				source.GetObject(svm, "foo", common.EEntityType.Symlink()), dest,
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
				},
				FollowSymlinks: toFollow,
				AsSubdir:       pointerTo(false),
			},

			ShouldFail: toFollow == nil,
		})

	if toFollow != nil && *toFollow {
		ValidateResource(svm, dest, ResourceDefinitionContainer{
			Objects: ObjectResourceMappingFlat{
				"foo": ResourceDefinitionObject{
					Body: srcBody,
				},
			},
		}, true)
	} else if toFollow == nil {
		ValidateContainsError(svm, stdOut, []string{
			cmd.ErrorLoneSymlinkSkipped.Error(),
		})
	}
}

func (s *BlobSymlinkSuite) Scenario_TestFollowLinks(svm *ScenarioVariationManager) {
	srcBody := NewRandomObjectContentContainer(1024)

	source := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"foo": ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType:        common.EEntityType.Symlink(),
					SymlinkedFileName: "bar",
				},
			},
			"bar": ResourceDefinitionObject{
				Body: srcBody,
			},
		},
	})

	dest := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})

	_, _ = RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbCopy, // sync doesn't support symlinks at this time
			Targets: []ResourceManager{
				source, dest,
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
				},
				FollowSymlinks: pointerTo(true),
				AsSubdir:       pointerTo(false),
			},
		})

	ValidateResource(svm, dest, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"foo": ResourceDefinitionObject{
				Body: srcBody,
			},
			"bar": ResourceDefinitionObject{
				Body: srcBody,
			},
		},
	}, true)
}
