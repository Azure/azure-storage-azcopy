package e2etest

// AzCopyJobPlan todo probably load the job plan directly?
type AzCopyJobPlan struct{}

// RunAzCopy todo define more cleanly, implement
func RunAzCopy(sm *ScenarioVariationManager, verb string, params []ResourceManager, flags map[string]string, env map[string]string) (*AzCopyJobPlan, error) {
	return nil, nil
}
