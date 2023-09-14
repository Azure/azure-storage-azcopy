package e2etest

var AccountRegistry = map[string]AccountResourceManager{} // For re-using accounts across testing

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
