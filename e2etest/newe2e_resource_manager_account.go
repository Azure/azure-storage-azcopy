package e2etest

var AccountRegistry = map[string]AzureAccountResourceManager{} // For re-using accounts across testing

const (
	PrimaryStandardAcct string = "PrimaryStandard"
	PrimaryHNSAcct      string = "PrimaryHNS"
)

func CreateAzureStorageAccount(name string, opts interface{}) (AzureAccountResourceManager, error) {
	panic("not implemented")

}

func DestroyAzureStorageAccount(name string) (AzureAccountResourceManager, error) {
	panic("not implemented")
}

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
