package e2etest

import (
	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type LocationVariation common.Location

func (l LocationVariation) ScenarioStepName() string {
	return common.Location(l).String()
}

func (l LocationVariation) Display() bool {
	return true
}

type ResourceSetupScenarioStep struct {
	ResourceName string            // If specified, saves as a resource in the scenario state.
	Base         ResourceManager   // If none is specified, the primary account is used. The setup config cannot originate lower than the base.
	Locations    []common.Location // If base is above a service, locations is ignored.
	// Setup is an interface which can handle multiple levels of resources (e.g. ResourceSetupObject, ResourceSetupContainer, ResourceSetupService)
	// If no path from the base is specified, one is automagically created.
	// e.g. Acct base->service->container (guid name)->file
	Setup ResourceSetupConfig
}

func (r ResourceSetupScenarioStep) MockVariations(mockState ScenarioState) []MockedVariation {
	locations := r.Locations

	if r.Base != nil {
		baseLevel := r.Base.Level()

		if baseLevel > r.Setup.Level() {
			panic("invalid setup: base sits above target level")
		}

		if baseLevel >= cmd.ELocationLevel.Service() {
			locations = []common.Location{r.Base.Location()}
		}
	}

	out := make([]MockedVariation, len(locations))
	for idx, loc := range locations {
		newState := mockState.Clone()

		newState.Resources[r.ResourceName] = MockResourceManager{
			location: loc,
			level:    r.Setup.Level(),
		}

		out[idx] = MockedVariation{Variation: LocationVariation(loc), MockedState: newState}
	}

	return out
}

func (r ResourceSetupScenarioStep) Run(a TestingAsserter, state ScenarioState, variation ScenarioVariation) ScenarioState {
	return state
}

type ResourceSetupConfig interface {
	Level() cmd.LocationLevel
	Do(target ResourceManager, location common.Location) ResourceManager
	Validate(target ResourceManager, a TestingAsserter)
}

type resourceSetupStub struct{}

func (r resourceSetupStub) Do(target ResourceManager, location common.Location) ResourceManager {
	//TODO implement me
	panic("implement me")
}

func (r resourceSetupStub) Validate(target ResourceManager, a TestingAsserter) {
	panic("implement me")
}

type ResourceSetupObject struct {
	resourceSetupStub

	// Required parameters
	Name string // If not specified
	Body []byte // If not specified, does not actually create object, but still prepares the ResourceManager for the scenario state.

	// Optional parameters
	Type common.EntityType // assumed File unless otherwise specified
}

func (r ResourceSetupObject) Level() cmd.LocationLevel {
	return cmd.ELocationLevel.Object()
}

type ResourceSetupContainer struct {
	resourceSetupStub

	// Required Parameters
	Name string

	Objects []ResourceSetupObject
}

func (r ResourceSetupContainer) Level() cmd.LocationLevel {
	return cmd.ELocationLevel.Container()
}

type ResourceSetupService struct {
	resourceSetupStub

	Containers []ResourceSetupContainer
}

func (r ResourceSetupService) Level() cmd.LocationLevel {
	return cmd.ELocationLevel.Service()
}
