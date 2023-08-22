package e2etest

type ResourceValidationScenarioStep struct {
	Target     ResourceManager
	Comparison ResourceSetupConfig
}

func (r ResourceValidationScenarioStep) MockVariations(mockState ScenarioState) (variations []MockedVariation) {
	return []MockedVariation{{StrVariation{"ValidateResource", false}, mockState}}
}

func (r ResourceValidationScenarioStep) Run(a TestingAsserter, state ScenarioState, variation ScenarioVariation) ScenarioState {
	return state
}
