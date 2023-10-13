package e2etest

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"testing"
)

func init() {
	suiteManager.RegisterSuite(&ExampleSuite{})
}

type ExampleSuite struct{}

func (s *ExampleSuite) SetupSuite(a Asserter) {
	a.Log("Setup logging!")
}

func (s *ExampleSuite) TeardownSuite(a Asserter) {
	a.Log("Teardown logging!")
	//a.Error("Oops!")
}

func (s *ExampleSuite) Scenario_SingleFileCopySyncS2S(svm *ScenarioVariationManager) {
	acct := AccountRegistry[PrimaryStandardAcct]
	srcService := acct.GetService(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.File(), common.ELocation.BlobFS()}))
	svm.InsertVariationSeparator("->")
	dstService := acct.GetService(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.File(), common.ELocation.BlobFS()}))

	_, _ = srcService, dstService

	svm.InsertVariationSeparator(":")
	//body := NewRandomObjectContentContainer(svm, SizeFromString("10K"))

	//srcObj := CreateResource(svm, srcService, ResourceDefinitionObject{
	//	Name: PtrOf("foobar"),
	//	Body: body,
	//})
	//
	//dstObj := CreateResource(svm, dstService, ResourceDefinitionContainer{}).(ContainerResourceManager).GetObject(svm, "foobar", common.EEntityType.File())

	RunAzCopy(
		svm,
		ResolveVariation(svm, []string{"copy", "sync"}),
		[]ResourceManager{},
		nil,
		nil)

	//ValidateResource(svm, dstObj, ResourceDefinitionObject{
	//	Body: body,
	//})
}

func TestSingleFileCopyS2S(t *testing.T) {

}
