package e2etest

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

var AccountRegistry = map[string]AccountResourceManager{
	PrimaryStandardAcct: &DummyAccountResourceManager{},
} // For re-using accounts across testing

const (
	PrimaryStandardAcct string = "PrimaryStandard"
	PrimaryHNSAcct      string = "PrimaryHNS"
)

func AccountRegistryInitHook() TieredError {
	if GlobalConfig.StaticResources() {
		AccountRegistry[PrimaryStandardAcct] = nil
		AccountRegistry[PrimaryHNSAcct] = nil

		return nil // Clean init, because we were supplied everything.
	} else {
		return nil
	}
}

func AccountRegistryCleanupHook() TieredError {
	return nil
}

type DummyAccountResourceManager struct{}

func (d DummyAccountResourceManager) AccountName() string {
	return "dummy"
}

func (d DummyAccountResourceManager) AccountType() AccountType {
	return EAccountType.Standard()
}

func (d DummyAccountResourceManager) AvailableServices() []common.Location {
	return []common.Location{common.ELocation.Blob(), common.ELocation.File(), common.ELocation.BlobFS()}
}

func (d DummyAccountResourceManager) GetService(a Asserter, location common.Location) ServiceResourceManager {
	return nil // not called right this minute(?)
}
