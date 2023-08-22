package e2etest

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"testing"
)

func TestSingleFileCopyS2S(t *testing.T) {
	SourceRM := "Source"
	DestRM := "Dest"
	FileBody := []byte("hello, world!")

	RunScenarioPipeline(
		t,
		[]ScenarioStepPreparer{
			func(state ScenarioState) ScenarioStep { // Example of injecting a custom step
				return CustomScenarioStep{
					Variations: func(mockState ScenarioState) []MockedVariation {
						commands := []string{"copy", "sync"}
						out := make([]MockedVariation, len(commands))

						for k, v := range commands {
							newState := mockState.Clone()
							newState.CustomState["command"] = v

							out[k] = MockedVariation{StrVariation{v, false}, newState}
						}

						return out
					},
					Runner: func(a TestingAsserter, state ScenarioState, variation ScenarioVariation) ScenarioState {
						state.CustomState["command"] = variation.ScenarioStepName()
						return state
					},
				}
			},
			func(state ScenarioState) ScenarioStep { // Prepare a source
				return ResourceSetupScenarioStep{
					ResourceName: SourceRM,
					Locations:    []common.Location{common.ELocation.Blob(), common.ELocation.File(), common.ELocation.BlobFS()},
					Setup: ResourceSetupObject{
						Name: "src",
						Body: FileBody,
					},
				}
			},
			func(state ScenarioState) ScenarioStep { // Prepare a destination
				return ResourceSetupScenarioStep{
					ResourceName: DestRM,
					Locations:    []common.Location{common.ELocation.Blob(), common.ELocation.File(), common.ELocation.BlobFS()},
					Setup: ResourceSetupObject{
						Name: "dst",
					},
				}
			},
			func(state ScenarioState) ScenarioStep { // Run AzCopy
				return RunAzCopyScenarioStep{
					Verb:    state.CustomState["command"].(string),
					Targets: []ResourceManager{state.Resources[SourceRM], state.Resources[DestRM]},
					Flags: map[string]string{
						"as-subdir": "false",
					},
					PlanStateName: "plan",
				}
			},
			func(state ScenarioState) ScenarioStep { // Validate against the real resource (if necessary)
				return ResourceValidationScenarioStep{
					Target: state.Resources[DestRM],
					Comparison: ResourceSetupObject{
						Name: "dst",
						Body: FileBody,
					},
				}
			},
			func(state ScenarioState) ScenarioStep { // Validate the plan file (if necessary)
				return AzCopyJobPlanValidationScenarioStep{
					TargetPlan:       state.CustomState["plan"].(AzCopyJobPlan),
					ExpectedEntities: []string{""}, // todo: this will definitely change, likely to similar format as the resource validation step.
				}
			},
		},
		nil,
	)
}
