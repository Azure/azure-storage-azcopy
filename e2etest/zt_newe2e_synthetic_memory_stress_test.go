package e2etest

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type SyntheticMemoryStressTestSuite struct{}

/*
This test suite requires three things

1) The telemetry account is configured
2) Stress test data is generated and present in the expected containers (run the generators in stress_generators)
3) Stress testing is enabled-- this is not enabled for every run, and probably shouldn't be ran automatically.

Currently, this suite (and it's scenarios) are designed to target highly memory intensive operations. The extent of this is currently limited
but should be improved upon with time.
*/

func RegisterSyntheticStressTestHook(a Asserter) {
	// Why a hook instead of init? We want to check that conditions are set, which is reliant upon the config.
	if !GlobalConfig.TelemetryConfigured() {
		return // don't register if we don't have the account set up
	}

	if !GlobalConfig.TelemetryConfig.StressTestEnabled {
		return // don't register if we haven't enabled the stress tests.
	}

	// todo: validate stress test data is present

	suiteManager.RegisterSuite(&SyntheticMemoryStressTestSuite{})
}

func (s *SyntheticMemoryStressTestSuite) Scenario_CopyManyFiles(a *ScenarioVariationManager) {
	telemetryBlobService, err := GlobalConfig.GetTelemetryBlobService()
	a.NoError("Get telemetry blob service", err, true)
	destBlobService := GetRootResource(a, common.ELocation.Blob()).(ServiceResourceManager)

	telemServiceRm := &BlobServiceResourceManager{
		InternalClient: telemetryBlobService,
	}
	sourceCt := telemServiceRm.GetContainer(SyntheticContainerManyFilesSource)
	destCt := CreateResource(a, destBlobService, ResourceDefinitionContainer{})

	RunAzCopy(a, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{sourceCt, destCt},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive: PtrOf(true),
			},

			AsSubdir: PtrOf(false),
		},
		Stdout: &AzCopyDiscardStdout{},
	})
}

func (s *SyntheticMemoryStressTestSuite) Scenario_CopyFolders(a *ScenarioVariationManager) {
	telemetryFileService, err := GlobalConfig.GetTelemetryFileService()
	a.NoError("Get telemetry file service", err, true)
	destFileService := GetRootResource(a, common.ELocation.FileSMB()).(ServiceResourceManager)

	telemServiceRm := &FileServiceResourceManager{
		InternalClient: telemetryFileService,
	}

	sourceCt := telemServiceRm.GetContainer(SyntheticContainerManyFoldersSource)
	destCt := CreateResource(a, destFileService, ResourceDefinitionContainer{})

	RunAzCopy(a, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{sourceCt, destCt},
		Flags: CopyFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive: PtrOf(true),
			},

			AsSubdir: PtrOf(false),
		},
		Stdout: &AzCopyDiscardStdout{},
	})
}

func (s *SyntheticMemoryStressTestSuite) Scenario_SyncWorstCase(a *ScenarioVariationManager) {
	telemetryBlobService, err := GlobalConfig.GetTelemetryBlobService()
	a.NoError("Get telemetry blob service", err, true)

	telemServiceRm := &BlobServiceResourceManager{
		InternalClient: telemetryBlobService,
	}
	sourceCt := telemServiceRm.GetContainer(SyntheticContainerSyncWorstCaseSource)
	destCt := telemServiceRm.GetContainer(SyntheticContainerSyncWorstCaseDest)

	RunAzCopy(a, AzCopyCommand{
		Verb:    AzCopyVerbSync,
		Targets: []ResourceManager{sourceCt, destCt},
		Flags: SyncFlags{
			CopySyncCommonFlags: CopySyncCommonFlags{
				Recursive: PtrOf(true),
				// We don't want to modify the destination, this test is designed to target sync's algo
				DryRun: PtrOf(true),
			},

			// fully test sync; this will get caught by dryrun.
			DeleteDestination: pointerTo(true),
		},
		// we're not interested in knowing anything about this dryrun
		Stdout: &AzCopyDiscardStdout{},
	})
}
