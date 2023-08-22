package e2etest

import "testing"

type ScenarioStepPreparer func(state ScenarioState) ScenarioStep

func RunScenarioPipeline(
	t *testing.T,
	preparers []ScenarioStepPreparer, // Careful! The more variations possible, the longer tests will take to run.
	opts *ScenarioPipelineOptions,
) {
	// First, we must acquire routes for the pipeline.
	variations := CalculateScenarioVariations(preparers)

	suiteName, testName := getTestName(t)

	isParallel := !isLaunchedByDebugger && opts.IsParallel()
	for _, v := range variations {
		t.Run(v.String(), func(t *testing.T) {
			if isParallel {
				t.Parallel()
			}

			a := TestingAsserter{t, suiteName + "-" + testName, v.String(), ""}

			defer func() {
				// TODO: Upload logs
				t.Log("Uploading logs... (if possible)")
			}()

			state := NewScenarioState(v.String())
			for idx := range preparers {
				a.Step = v[idx].ScenarioStepName()

				// run the step with the intended variation
				task := preparers[idx](state)
				state = task.Run(a, state, v[idx])
			}
		})
	}
}

func CalculateScenarioVariations(preparers []ScenarioStepPreparer) []ScenarioDescription {
	if len(preparers) == 0 {
		return []ScenarioDescription{}
	}

	type workItem struct {
		description ScenarioDescription
		mockState   ScenarioState
	}

	out := make([]ScenarioDescription, 0)
	queue := []workItem{{mockState: NewScenarioState("")}}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		preparer := preparers[len(current.description)]
		step := preparer(current.mockState)
		variations := step.MockVariations(current.mockState)

		for idx := range variations {
			newScenario := append(current.description, variations[idx].Variation)

			if len(newScenario) == len(preparers) {
				out = append(out, newScenario)
			} else {
				queue = append(queue, workItem{
					description: newScenario,
					mockState:   variations[idx].MockedState,
				})
			}
		}
	}

	return out
}
