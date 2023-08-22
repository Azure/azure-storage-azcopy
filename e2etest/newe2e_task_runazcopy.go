package e2etest

type RunAzCopyScenarioStep struct {
	Verb          string
	Targets       []ResourceManager
	Flags         map[string]string
	PlanStateName string
}

func (r RunAzCopyScenarioStep) MockVariations(mockState ScenarioState) []MockedVariation {
	return []MockedVariation{{StrVariation{r.Verb, true}, mockState}}
}

func (r RunAzCopyScenarioStep) Run(a TestingAsserter, state ScenarioState, variation ScenarioVariation) ScenarioState {
	return state
}
