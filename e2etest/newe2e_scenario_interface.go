package e2etest

import (
	"strings"
)

//type AzCopyStatus struct {
//	stdOut   *bytes.Buffer
//	result   *CopyOrSyncCommandResult
//	exitCode int
//}

type ScenarioState struct {
	Name string
	// ResourceManager(s) in the raw; can be any variety, service, container, etc. GetTypeOrZero[T] can wrangle these.
	// RMs are assumed to carry no special actively modified state, and are thus OK to just borrow raw.
	Resources map[string]ResourceManager
	// CustomState is a map of variables, effectively.
	// Be careful with pointers in CustomState-- Clone is naive.
	CustomState map[string]any
}

func NewScenarioState(Name string) ScenarioState {
	return ScenarioState{
		Name:        Name,
		Resources:   map[string]ResourceManager{},
		CustomState: map[string]any{},
	}
}

func (s ScenarioState) Clone() ScenarioState {
	out := ScenarioState{
		Name: s.Name,
	}

	out.Resources = make(map[string]ResourceManager)
	for k, v := range s.Resources {
		out.Resources[k] = v
	}

	out.CustomState = make(map[string]any)
	for k, v := range s.CustomState {
		out.CustomState[k] = v
	}

	return out
}

type MockedVariation struct {
	Variation   ScenarioVariation
	MockedState ScenarioState
}

// ScenarioStep defines a process that can run as a step of a scenario (e.g. resource creation, copy/sync, resume, validation)
type ScenarioStep interface {
	// MockVariations does not actually run the step, but mocks the steps.
	MockVariations(mockState ScenarioState) (variations []MockedVariation)
	Run(a TestingAsserter, state ScenarioState, variation ScenarioVariation) ScenarioState
}

type ScenarioDescription []ScenarioVariation

func (s ScenarioDescription) String() string {
	out := make([]string, 0)

	for _, v := range s {
		if !v.Display() {
			continue
		}
		out = append(out, v.ScenarioStepName())
	}

	return strings.Join(out, "-")
}

// ScenarioVariation is the name for a particular variation of a scenario
type ScenarioVariation interface {
	ScenarioStepName() string
	Display() bool
}

type StrVariation struct {
	Name          string
	ShouldDisplay bool
}

func (s StrVariation) ScenarioStepName() string {
	return s.Name
}

func (s StrVariation) Display() bool {
	return s.ShouldDisplay
}
