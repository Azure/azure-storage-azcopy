package e2etest

type CustomScenarioStep struct {
	Variations func(mockState ScenarioState) []MockedVariation
	Runner     func(a TestingAsserter, state ScenarioState, variation ScenarioVariation) ScenarioState
}

func (c CustomScenarioStep) MockVariations(mockState ScenarioState) (variations []MockedVariation) {
	return c.Variations(mockState)
}

func (c CustomScenarioStep) Run(a TestingAsserter, state ScenarioState, variation ScenarioVariation) ScenarioState {
	return c.Runner(a, state, variation)
}
