package e2etest

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func init() {
	suiteManager.RegisterSuite(&ExampleSuite{})
}

type ExampleSuite struct{}

func (s *ExampleSuite) SetupSuite(a Asserter) {
	//a.Log("Setup logging!")
}

func (s *ExampleSuite) TeardownSuite(a Asserter) {
	//a.Log("Teardown logging!")
	//a.Error("Oops!")
}

func (s *ExampleSuite) Scenario_SingleFileCopySyncS2S(svm *ScenarioVariationManager) {
	acct := GetAccount(svm, PrimaryStandardAcct)
	srcService := acct.GetService(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob()}))
	svm.InsertVariationSeparator("->")
	dstService := acct.GetService(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob()}))

	svm.InsertVariationSeparator(":")
	body := NewRandomObjectContentContainer(svm, SizeFromString("10K"))
	// Scale up from service to object
	srcObj := CreateResource(svm, srcService, ResourceDefinitionObject{
		Body: body,
	}).(ObjectResourceManager) // todo: generic CreateResource is something to pursue in another branch, but it's an interesting thought.
	// Scale up from service to container
	dstObj := CreateResource(svm, dstService, ResourceDefinitionContainer{}).(ContainerResourceManager).GetObject(svm, "foobar", common.EEntityType.File())

	_, _ = srcObj, dstObj
	RunAzCopy(
		svm,
		AzCopyCommand{
			Verb:    ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy}),
			Targets: []string{srcObj.URI(svm, true), dstObj.URI(svm, true)},
		})

	ValidateResource(svm, dstObj, ResourceDefinitionObject{
		Body: body,
	}, true)
}
