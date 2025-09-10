package e2etest

import (
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type DryrunSuite struct{}

func init() {
	suiteManager.RegisterSuite(&DryrunSuite{})
}

func (*DryrunSuite) Scenario_UploadSync_Encoded(a *ScenarioVariationManager) {
	dst := CreateResource[ContainerResourceManager](a, GetRootResource(a, ResolveVariation(a, []common.Location{common.ELocation.Blob(), common.ELocation.FileSMB(), common.ELocation.BlobFS()})), ResourceDefinitionContainer{})

	src := CreateResource[ContainerResourceManager](a, GetRootResource(a, common.ELocation.Local()), ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"foo%bar":  ResourceDefinitionObject{},
			"baz%bish": ResourceDefinitionObject{},
		},
	})

	stdout, _ := RunAzCopy(a, AzCopyCommand{
		Verb:    "sync",
		Targets: []ResourceManager{src, dst},
		Flags: SyncFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				DryRun: pointerTo(true),

				GlobalFlags: GlobalFlags{
					OutputType: pointerTo(ResolveVariation(a, []common.OutputFormat{common.EOutputFormat.Json(), common.EOutputFormat.Text()})),
				},
			},

			DeleteDestination: pointerTo(true),
		},
	})

	// we're looking to see foo%bar and bar%foo
	ValidateDryRunOutput(a, stdout, src, dst, map[string]DryrunOp{
		"foo%bar":  DryrunOpCopy,
		"baz%bish": DryrunOpCopy,
	})
}

func (*DryrunSuite) Scenario_DownloadSync_Encoded(a *ScenarioVariationManager) {
	src := CreateResource[ContainerResourceManager](a, GetRootResource(a, ResolveVariation(a, []common.Location{common.ELocation.Blob(), common.ELocation.FileSMB(), common.ELocation.BlobFS()})), ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"foo%bar":  ResourceDefinitionObject{},
			"baz%bish": ResourceDefinitionObject{},
		},
	})

	dst := CreateResource[ContainerResourceManager](a, GetRootResource(a, common.ELocation.Local()), ResourceDefinitionContainer{})

	stdout, _ := RunAzCopy(a, AzCopyCommand{
		Verb:    "sync",
		Targets: []ResourceManager{src, dst},
		Flags: SyncFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				DryRun: pointerTo(true),

				GlobalFlags: GlobalFlags{
					OutputType: pointerTo(ResolveVariation(a, []common.OutputFormat{common.EOutputFormat.Json(), common.EOutputFormat.Text()})),
				},
			},

			DeleteDestination: pointerTo(true),
		},
	})

	// we're looking to see foo%bar and bar%foo
	ValidateDryRunOutput(a, stdout, src, dst, map[string]DryrunOp{
		"foo%bar":  DryrunOpCopy,
		"baz%bish": DryrunOpCopy,
	})
}

func (*DryrunSuite) Scenario_ExtraProps(a *ScenarioVariationManager) {
	meta := common.Metadata{
		"foo": pointerTo("bar"),
	}
	tags := common.BlobTags{
		"foo": "bar",
	}

	body := NewRandomObjectContentContainer(common.MegaByte * 10)
	hash := body.MD5()
	srcContainer := CreateResource[ContainerResourceManager](a, GetRootResource(a, common.ELocation.Blob()), ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"foobar": ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					HTTPHeaders: contentHeaders{
						cacheControl:       pointerTo("asdf"),
						contentDisposition: pointerTo("asdf"),
						contentEncoding:    pointerTo("asdf"),
						contentLanguage:    pointerTo("asdf"),
						contentType:        pointerTo("asdf"),
						contentMD5:         hash[:],
					},
					Metadata: meta,
					BlobProperties: BlobProperties{
						Type:                pointerTo(blob.BlobTypeBlockBlob),
						Tags:                tags,
						BlockBlobAccessTier: pointerTo(blob.AccessTierCold),
					},
				},

				Body: body,
			},
		},
	})

	dstContainer := CreateResource[ContainerResourceManager](a, GetRootResource(a, common.ELocation.Blob()), ResourceDefinitionContainer{})

	stdout, _ := RunAzCopy(a, AzCopyCommand{
		Verb: ResolveVariation(a, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync}),
		Targets: []ResourceManager{
			AzCopyTarget{ResourceManager: srcContainer, AuthType: EExplicitCredentialType.OAuth()},
			AzCopyTarget{ResourceManager: dstContainer, AuthType: EExplicitCredentialType.OAuth()},
		},
		Flags: CopySyncCommonFlags{
			Recursive:             pointerTo(true),
			DryRun:                pointerTo(true),
			S2SPreserveAccessTier: pointerTo(true),
			S2SPreserveBlobTags:   pointerTo(true),
		},
	})

	if !a.Dryrun() {
		dryrun, ok := stdout.(*AzCopyParsedDryrunStdout)
		a.AssertNow("must be dryrun output", Equal{}, ok, true)
		a.AssertNow("must contain one entry", Equal{}, len(dryrun.Transfers), 1)

		tx := dryrun.Transfers[0]
		a.Assert("cache control", Equal{}, DerefOrZero(tx.HttpHeaders.BlobCacheControl), "asdf")
		a.Assert("content disposition", Equal{}, DerefOrZero(tx.HttpHeaders.BlobContentDisposition), "asdf")
		a.Assert("content encoding", Equal{}, DerefOrZero(tx.HttpHeaders.BlobContentEncoding), "asdf")
		a.Assert("content language", Equal{}, DerefOrZero(tx.HttpHeaders.BlobContentLanguage), "asdf")
		a.Assert("content type", Equal{}, DerefOrZero(tx.HttpHeaders.BlobContentType), "asdf")
		a.Assert("hash", Equal{Deep: true}, tx.HttpHeaders.BlobContentMD5, hash[:])
		a.Assert("meta", Equal{Deep: true}, tx.Metadata, meta)
		a.Assert("tags", Equal{Deep: true}, tx.BlobTags, tags)
		a.Assert("blobType", Equal{}, tx.BlobType, common.EBlobType.BlockBlob())
		a.Assert("access tier", Equal{}, DerefOrZero(tx.BlobTier), blob.AccessTierCold)
	}
}
