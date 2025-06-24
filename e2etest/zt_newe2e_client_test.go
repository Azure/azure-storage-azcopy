package e2etest

import (
	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"os"
)

func init() {
	suiteManager.RegisterSuite(&ClientTestSuite{})
}

// ClientTestSuite is a test suite for the azcopy client functionality.
type ClientTestSuite struct{}

func (s *ClientTestSuite) Scenario_LoginWorkload(svm *ScenarioVariationManager) {
	// Run only in environments that support and are set up for Workload Identity (ex: Azure Pipeline, Azure Kubernetes Service)
	if os.Getenv("NEW_E2E_ENVIRONMENT") != AzurePipeline {
		svm.Skip("Workload Identity is only supported in environments specifically set up for it.")
	}

	c := azcopy.Client{}

	err := c.Login(azcopy.LoginOptions{LoginType: common.EAutoLoginType.Workload()})

	svm.NoError("login with workload identity", err)
}
