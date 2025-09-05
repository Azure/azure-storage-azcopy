package e2etest

import (
	"github.com/Azure/azure-storage-azcopy/v10/cmd"
)

// ResourceTracker tracks resources
type ResourceTracker interface {
	TrackCreatedResource(manager ResourceManager)
	TrackCreatedAccount(account AccountResourceManager)
}

func TrackResourceCreation(a Asserter, rm any) {
	a.HelperMarker().Helper()

	if t, ok := a.(ResourceTracker); ok {
		if arm, ok := rm.(AccountResourceManager); ok {
			t.TrackCreatedAccount(arm)
		} else if resMan, ok := rm.(ResourceManager); ok {
			t.TrackCreatedResource(resMan)
		}
	}
}

func CreateResource[T ResourceManager](a Asserter, base ResourceManager, def MatchedResourceDefinition[T]) T {
	definition := ResourceDefinition(def)

	a.AssertNow("Base resource and definition must not be null", Not{IsNil{}}, base, definition)
	a.AssertNow("Base resource must be at a equal or lower level than the resource definition", Equal{}, base.Level() <= definition.DefinitionTarget(), true)

	// Remember where we started so we can step up to there
	originalDefinition := definition
	_ = originalDefinition // use it so Go stops screaming

	// Get to the target level.
	for base.Level() < definition.DefinitionTarget() {
		/*
			Instead of scaling up to the definition, we'll scale the definition to the base.
			This means we don't have to keep tabs on the fact we'll need to create the container later,
			because the container creation is now an inherent part of the resource definition.
		*/
		definition = definition.GenerateAdoptiveParent(a)
	}

	// Create the resource(s)
	definition.ApplyDefinition(a, base, map[traverser.LocationLevel]func(Asserter, ResourceManager, ResourceDefinition){
		traverser.ELocationLevel.Container(): func(a Asserter, manager ResourceManager, definition ResourceDefinition) {
			manager.(ContainerResourceManager).Create(a, definition.(ResourceDefinitionContainer).Properties)
		},

		traverser.ELocationLevel.Object(): func(a Asserter, manager ResourceManager, definition ResourceDefinition) {
			objDef := definition.(ResourceDefinitionObject)

			if objDef.Body == nil {
				objDef.Body = NewZeroObjectContentContainer(0)
			}

			manager.(ObjectResourceManager).Create(a, objDef.Body, objDef.ObjectProperties)
		},
	})

	// Step up to where we need to be and return it
	matchingDef := definition
	matchingRes := base
	for matchingRes.Level() < originalDefinition.DefinitionTarget() {
		matchingRes, matchingDef = matchingDef.MatchAdoptiveChild(a, matchingRes)
	}

	return matchingRes.(T)
}
