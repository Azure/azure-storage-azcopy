package e2etest

import (
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

/*
FNSSuite exists to test oddities about virtual directory semantics on flat namespace blob.
*/
type FNSSuite struct{}

func init() {
	suiteManager.RegisterSuite(&FNSSuite{})
}

func (*FNSSuite) Scenario_CopyToOverlappableDirectoryMarker(a *ScenarioVariationManager) {
	DirMeta := ResolveVariation(a, []string{"", common.POSIXFolderMeta})
	tgtVerb := ResolveVariation(a, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync})

	// Target a fns account
	destRm := ObjectResourceMappingFlat{
		"foobar/": ResourceDefinitionObject{
			ObjectProperties: ObjectProperties{
				Metadata: common.Iff(DirMeta != "", common.Metadata{
					common.POSIXFolderMeta: pointerTo("true"),
				}, nil),
			},
			Body: NewZeroObjectContentContainer(0),
		},
	}

	if tgtVerb == AzCopyVerbSync {
		// Sync must have an existing destination, non-folder.
		destRm["foobar"] = ResourceDefinitionObject{
			Body: NewZeroObjectContentContainer(512),
		}
	}

	dest := CreateResource[ContainerResourceManager](a, GetRootResource(a, common.ELocation.Blob()),
		ResourceDefinitionContainer{
			Objects: destRm,
		},
	)

	if tgtVerb == AzCopyVerbSync && !a.Dryrun() {
		time.Sleep(time.Second * 5) // Ensure the source is newer
	}

	// Source must be newer than the destination
	source := CreateResource[ObjectResourceManager](a, GetRootResource(a, common.ELocation.Local()), ResourceDefinitionObject{
		Body: NewRandomObjectContentContainer(1024),
	})

	_, _ = RunAzCopy(a,
		AzCopyCommand{
			Verb: tgtVerb,
			Targets: []ResourceManager{
				source,
				dest.GetObject(a, "foobar", common.EEntityType.File()),
			},
			Flags: CopyFlags{
				AsSubdir: common.Iff(tgtVerb == AzCopyVerbCopy, pointerTo(false), nil),
			},
		},
	)

	ValidateResource(a, dest, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"foobar": ResourceDefinitionObject{
				ObjectShouldExist: pointerTo(true),
			},
			"foobar/": ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					Metadata: common.Iff(DirMeta != "", common.Metadata{
						common.POSIXFolderMeta: pointerTo("true"),
					}, nil),
				},
				ObjectShouldExist: pointerTo(true),
			},
		},
	}, ValidateResourceOptions{
		validateObjectContent: true,
	})
}

// Scenario_IncludeRootDirectoryStub tests that the root directory (and sub directories) appropriately get their files picked up.
func (*FNSSuite) Scenario_IncludeRootDirectoryStub(a *ScenarioVariationManager) {
	DirMeta := ResolveVariation(a, []string{"", common.POSIXFolderMeta})

	dst := CreateResource[ContainerResourceManager](a, GetRootResource(a, common.ELocation.Blob()), ResourceDefinitionContainer{})
	src := CreateResource[ContainerResourceManager](a, GetRootResource(a, common.ELocation.Blob()), ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"foobar": ResourceDefinitionObject{Body: NewRandomObjectContentContainer(512), ObjectProperties: ObjectProperties{Metadata: common.Metadata{"dontcopyme": pointerTo("")}}}, // Object w/ same name as root dir
			"foobar/": ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType: common.Iff(DirMeta != "", common.EEntityType.Folder(), common.EEntityType.File()),
					Metadata:   common.Metadata{"asdf": pointerTo("qwerty")},
				},
			}, // Folder w/ same name as object, add special prop to ensure
			"foobar/foo":           ResourceDefinitionObject{Body: NewZeroObjectContentContainer(0)},
			"foobar/bar":           ResourceDefinitionObject{Body: NewZeroObjectContentContainer(0)},
			"foobar/baz":           ResourceDefinitionObject{Body: NewZeroObjectContentContainer(0)},
			"foobar/folder/":       ResourceDefinitionObject{ObjectProperties: ObjectProperties{EntityType: common.Iff(DirMeta != "", common.EEntityType.Folder(), common.EEntityType.File())}},
			"foobar/folder/foobar": ResourceDefinitionObject{Body: NewZeroObjectContentContainer(0)},
		},
	})

	azcopyVerb := ResolveVariation(a, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync})
	RunAzCopy(a,
		AzCopyCommand{
			Verb: azcopyVerb,
			Targets: []ResourceManager{
				src.GetObject(a, "foobar/", common.EEntityType.Folder()),
				dst.GetObject(a, "foobar/", common.EEntityType.Folder()),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive:             pointerTo(true),
					IncludeDirectoryStubs: pointerTo(true),
				},
				AsSubdir: common.Iff(azcopyVerb == AzCopyVerbCopy, pointerTo(false), nil),
			},
		},
	)

	ValidateResource(a, dst, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"foobar": ResourceDefinitionObject{ObjectShouldExist: pointerTo(false)}, // We shouldn't have captured foobar, but foobar/ should exist as a directory.
			"foobar/": ResourceDefinitionObject{ObjectProperties: ObjectProperties{
				EntityType: common.Iff(DirMeta != "", common.EEntityType.Folder(), common.EEntityType.File()),
				Metadata: common.Metadata{
					"asdf": pointerTo("qwerty"),
				},
			},
			},
			"foobar/foo":           ResourceDefinitionObject{Body: NewZeroObjectContentContainer(0)},
			"foobar/bar":           ResourceDefinitionObject{Body: NewZeroObjectContentContainer(0)},
			"foobar/baz":           ResourceDefinitionObject{Body: NewZeroObjectContentContainer(0)},
			"foobar/folder/":       ResourceDefinitionObject{ObjectProperties: ObjectProperties{EntityType: common.Iff(DirMeta != "", common.EEntityType.Folder(), common.EEntityType.File())}},
			"foobar/folder/foobar": ResourceDefinitionObject{Body: NewZeroObjectContentContainer(0)},
		},
	}, ValidateResourceOptions{
		validateObjectContent: false,
	})
}

/*
Scenario_SyncTrailingSlashDeletion tests against a potential accidental deletion bug that could occur when `folder/` exists at the destination, but not the source
and `folder/` happens to have an overlapping file at `folder`.
*/
func (*FNSSuite) Scenario_SyncTrailingSlashDeletion(a *ScenarioVariationManager) {
	folderStyle := ResolveVariation(a, []common.EntityType{common.EEntityType.File(), common.EEntityType.Folder()})

	dest := CreateResource[ContainerResourceManager](a, GetRootResource(a, common.ELocation.Blob()), ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"foobar": ResourceDefinitionObject{
				Body: NewRandomObjectContentContainer(1024),
			},
			"foobar/": ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType: folderStyle,
				},
			},
			"foobar/bar/": ResourceDefinitionObject{
				Body: NewRandomObjectContentContainer(1024),
			},
		},
	})

	src := CreateResource[ContainerResourceManager](a, GetRootResource(a, common.ELocation.Blob()), ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"foobar": ResourceDefinitionObject{
				Body: NewRandomObjectContentContainer(1024),
			}, // We don't care about anything other than the overlap. We merely want to trigger a delete op against dest's folder/.
		},
	})

	RunAzCopy(a, AzCopyCommand{
		Verb: AzCopyVerbSync,
		Targets: []ResourceManager{
			src.GetObject(a, "foobar/", common.EEntityType.Folder()),
			dest.GetObject(a, "foobar/", common.EEntityType.Folder()),
		},
		Flags: SyncFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive: pointerTo(true),
				GlobalFlags: GlobalFlags{
					OutputType: pointerTo(cmd.EOutputFormat.Text()),
				},
				IncludeDirectoryStubs: pointerTo(true),
			},
			DeleteDestination: pointerTo(true),
		},
	})

	ValidateResource(a, dest, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"foobar": ResourceDefinitionObject{}, // We just care this guy exists
			"foobar/": ResourceDefinitionObject{ // and this guy doesn't.
				ObjectShouldExist: pointerTo(false),
			},
			"foobar/bar/": ResourceDefinitionObject{
				ObjectShouldExist: pointerTo(false),
			},
		},
	}, ValidateResourceOptions{
		validateObjectContent: false,
	})
}

func (*FNSSuite) Scenario_SyncOverlap(a *ScenarioVariationManager) {
	// Sync must be capable of mirroring a source resource type onto a blob destination.

	// Define our scenario up front; reduce complexity in debugging.
	srcLoc := ResolveVariation(a, []common.Location{common.ELocation.File(), common.ELocation.Blob(), common.ELocation.BlobFS(), common.ELocation.Local()})
	dstLoc := common.ELocation.Blob()
	a.InsertVariationSeparator("->Blob|Overwrite:")

	const (
		DestTypeStandardStub = iota
		DestTypeOverlapStup
	)

	dstType := NamedResolveVariation(a, map[string]uint{
		"StandardStub":      DestTypeStandardStub,
		"TrailingSlashStub": DestTypeOverlapStup,
	})

	// Set up the resource maps
	srcMap := ObjectResourceMappingFlat{
		"foo": ResourceDefinitionObject{},
	}

	dstMap := map[uint]ObjectResourceMappingFlat{
		DestTypeStandardStub: {
			"foo": ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType: common.EEntityType.Folder(),
				},
			},
		},
		DestTypeOverlapStup: {
			"foo/": ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType: common.EEntityType.Folder(),
				},
			},
		},
	}[dstType]

	// Spin up the resources
	src := CreateResource(a, GetRootResource(a, srcLoc), ResourceDefinitionContainer{
		Objects: srcMap,
	})

	dst := CreateResource(a, GetRootResource(a, dstLoc), ResourceDefinitionContainer{
		Objects: dstMap,
	})

	// Execute the test
	RunAzCopy(a, AzCopyCommand{
		Verb: AzCopyVerbSync,
		Targets: []ResourceManager{
			src.GetObject(a, "foo", common.EEntityType.File()),
			dst.GetObject(a, "foo", common.EEntityType.File()),
		},
		Flags: SyncFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive:             pointerTo(true),
				IncludeDirectoryStubs: pointerTo(true),
			},
		},
	})

	ValidateResource(a, dst, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat(JoinMap(dstMap, srcMap)),
	}, ValidateResourceOptions{
		validateObjectContent: false,
	})
}
