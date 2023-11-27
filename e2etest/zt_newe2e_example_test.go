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
	//svm.InsertVariationSeparator("->")
	//dstService := acct.GetService(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob()}))

	svm.InsertVariationSeparator(":")
	body := NewRandomObjectContentContainer(svm, SizeFromString("10K"))
	// Scale up from service to object
	srcObj := CreateResource[ObjectResourceManager](svm, srcService, ResourceDefinitionObject{
		Body: body,
	}) // todo: generic CreateResource is something to pursue in another branch, but it's an interesting thought.
	// Scale up from service to container
	//dstObj := CreateResource[ContainerResourceManager](svm, dstService, ResourceDefinitionContainer{}).GetObject(svm, "foobar", common.EEntityType.File())

	//_, _ = srcObj, dstObj
	RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: ResolveVariation(svm, []AzCopyVerb{AzCopyVerbRemove}),
			Targets: []ResourceManager{
				srcObj,
			},
		})

	ValidateResource[ObjectResourceManager](svm, srcObj, ResourceDefinitionObject{
		Body:              body,
		ObjectShouldExist: PtrOf(false),
	}, true)
}
