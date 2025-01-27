package e2etest

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type DryrunSuite struct{}

func init() {
	suiteManager.RegisterSuite(&DryrunSuite{})
}

func (*DryrunSuite) Scenario_UploadSync_Encoded(a *ScenarioVariationManager) {
	dst := CreateResource[ContainerResourceManager](a, GetRootResource(a, ResolveVariation(a, []common.Location{common.ELocation.Blob(), common.ELocation.File(), common.ELocation.BlobFS()})), ResourceDefinitionContainer{})

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
	src := CreateResource[ContainerResourceManager](a, GetRootResource(a, ResolveVariation(a, []common.Location{common.ELocation.Blob(), common.ELocation.File(), common.ELocation.BlobFS()})), ResourceDefinitionContainer{
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
