package e2etest

type AzCopyJobPlan struct{} // nothing here yet

type AzCopyJobPlanValidationScenarioStep struct {
	TargetPlan       AzCopyJobPlan
	ExpectedEntities []string
}

func (s AzCopyJobPlanValidationScenarioStep) MockVariations(mockState ScenarioState) (variations []MockedVariation) {
	return []MockedVariation{{StrVariation{"JobPlanValidation", false}, mockState}}
}

func (s AzCopyJobPlanValidationScenarioStep) Run(a TestingAsserter, state ScenarioState, variation ScenarioVariation) ScenarioState {
	return state
}
