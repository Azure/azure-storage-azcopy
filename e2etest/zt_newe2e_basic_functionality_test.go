package e2etest

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"time"
)

func init() {
	suiteManager.RegisterSuite(&BasicFunctionalitySuite{})
}

type BasicFunctionalitySuite struct{}

func (s *BasicFunctionalitySuite) Scenario_SingleFileUploadDownload(svm *ScenarioVariationManager) {
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync}) // Calculate verb early to create the destination object early
	// Scale up from service to object
	dstObj := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Local(), common.ELocation.Blob(), common.ELocation.File(), common.ELocation.BlobFS()})), ResourceDefinitionContainer{}).GetObject(svm, "test", common.EEntityType.File())
	// The object must exist already if we're syncing.
	if azCopyVerb == AzCopyVerbSync {
		dstObj.Create(svm, NewZeroObjectContentContainer(0), ObjectProperties{})

		if !svm.Dryrun() {
			// Make sure the LMT is in the past
			time.Sleep(time.Second * 10)
		}
	}

	body := NewRandomObjectContentContainer(svm, SizeFromString("10K"))
	// Scale up from service to object
	srcObj := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Local(), common.ELocation.Blob(), common.ELocation.File(), common.ELocation.BlobFS()})), ResourceDefinitionObject{
		ObjectName: pointerTo("test"),
		Body:       body,
	})

	// no s2s, no local->local
	if srcObj.Location().IsRemote() == dstObj.Location().IsRemote() {
		svm.InvalidateScenario()
		return
	}

	sasOpts := GenericAccountSignatureValues{}

	stdOut, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			// Sync is not included at this moment, because sync requires
			Verb: azCopyVerb,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcObj, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
				TryApplySpecificAuthType(dstObj, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
				},
			},
		})

	ValidateResource[ObjectResourceManager](svm, dstObj, ResourceDefinitionObject{
		Body: body,
	}, true)

	ValidatePlanFiles(svm, stdOut, ExpectedPlanFile{
		Objects: map[PlanFilePath]PlanFileObject{
			PlanFilePath{SrcPath: "", DstPath: ""}: {
				Properties: ObjectProperties{},
			},
		},
	})
}

func (s *BasicFunctionalitySuite) Scenario_MultiFileUploadDownload(svm *ScenarioVariationManager) {
	azCopyVerb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync}) // Calculate verb early to create the destination object early
	// Scale up from service to object
	dstContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Local(), common.ELocation.Blob(), common.ELocation.File(), common.ELocation.BlobFS()})), ResourceDefinitionContainer{})

	// Scale up from service to object
	srcDef := ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"abc":    ResourceDefinitionObject{Body: NewRandomObjectContentContainer(svm, SizeFromString("10K"))},
			"def":    ResourceDefinitionObject{Body: NewRandomObjectContentContainer(svm, SizeFromString("10K"))},
			"foobar": ResourceDefinitionObject{Body: NewRandomObjectContentContainer(svm, SizeFromString("10K"))},
		},
	}
	srcContainer := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Local(), common.ELocation.Blob(), common.ELocation.File(), common.ELocation.BlobFS()})), srcDef)

	// no s2s, no local->local
	if srcContainer.Location().IsRemote() == dstContainer.Location().IsRemote() {
		svm.InvalidateScenario()
		return
	}

	sasOpts := GenericAccountSignatureValues{}

	stdOut, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			// Sync is not included at this moment, because sync requires
			Verb: azCopyVerb,
			Targets: []ResourceManager{
				TryApplySpecificAuthType(srcContainer, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
				TryApplySpecificAuthType(dstContainer, EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: sasOpts,
				}),
			},
			Flags: CopyFlags{
				CopySyncCommonFlags: CopySyncCommonFlags{
					Recursive: pointerTo(true),
				},

				AsSubdir: common.Iff(azCopyVerb == AzCopyVerbCopy, PtrOf(true), nil), // defaults true
			},
		})

	ValidatePlanFiles(svm, stdOut, ExpectedPlanFile{
		// todo: service level resource to object mapping
		Objects: GeneratePlanFileObjectsFromMapping(srcDef.Objects, GeneratePlanFileObjectsOptions{DestPathProcessor: ParentDirDestPathProcessor(srcContainer.ContainerName())}),
	})

	ValidateResource[ContainerResourceManager](svm, dstContainer, srcDef, true)
}
