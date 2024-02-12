package e2etest

import (
	"reflect"
	"strings"
	"sync"
	"testing"
)

type ScenarioManager struct {
	testingT *testing.T
	Func     reflect.Value

	suite    string
	scenario string

	runLock *sync.Mutex

	varStack []*ScenarioVariationManager
}

func NewScenarioManager(t *testing.T, targetFunc reflect.Value) *ScenarioManager {
	nameSplits := strings.Split(t.Name(), "/")
	nameSplits = nameSplits[1:] // Remove "Test"
	Suite := nameSplits[0]
	Scenario := nameSplits[1]

	return &ScenarioManager{
		testingT: t,
		Func:     targetFunc,
		suite:    Suite,
		scenario: Scenario,
		runLock:  &sync.Mutex{},
	}
}

func (sm *ScenarioManager) NewVariation(origin *ScenarioVariationManager, id string, setting []any) {
	if origin.isInvalid {
		return // isInvalid variations shouldn't spawn new variations
	}

	for i := len(setting) - 1; i >= 0; i-- { // Because the stack is FIFO, insert the first terms last to match expected variation ordering.
		v := setting[i]
		clone := &ScenarioVariationManager{
			VariationData: origin.VariationData.Insert(id, v),
			Parent:        sm,
			callcounts:    make(map[string]uint),
		}

		sm.varStack = append(sm.varStack, clone)
	}
}

func (sm *ScenarioManager) RunScenario() {
	sm.runLock.Lock()
	sm.testingT.Cleanup(func() { sm.runLock.Unlock() })

	/*
		When appending to the stack, the newest item is always from the closest ancestor of the tree.
		Thus, we can retain good (read: brain happy) test ordering by doing FIFO. Engineering at its finest.
	*/
	sm.varStack = []*ScenarioVariationManager{
		{Parent: sm, callcounts: make(map[string]uint)}, // Root svm, no variations, no nothing.
	}

	for len(sm.varStack) > 0 {
		svm := sm.varStack[len(sm.varStack)-1] // *pop*!
		sm.varStack = sm.varStack[:len(sm.varStack)-1]

		func() {
			defer func() {
				if err := recover(); err != nil {
					sm.testingT.Logf("Variation %s dryrun panicked: %v", svm.VariationName(), err)
					svm.InvalidateScenario()
				}
			}()

			sm.Func.Call([]reflect.Value{reflect.ValueOf(svm)}) // Test will push onto the stack a bunch
		}()

		if !svm.isInvalid { // If we made a real test
			sm.testingT.Run(svm.VariationName(), func(t *testing.T) {
				svm.t = t
				svm.callcounts = make(map[string]uint)

				t.Parallel()
				t.Cleanup(func() {
					svm.DeleteCreatedResources() // clean up after ourselves!
				})

				sm.Func.Call([]reflect.Value{reflect.ValueOf(svm)})
			})
		}
	}
}
