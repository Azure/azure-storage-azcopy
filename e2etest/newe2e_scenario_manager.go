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

	runLock                   *sync.Mutex
	waitGroup                 *sync.WaitGroup
	finalizedVariationChannel chan<- *ScenarioVariationManager
}

func NewScenarioManager(t *testing.T, targetFunc reflect.Value) *ScenarioManager {
	nameSplits := strings.Split(t.Name(), "/")
	nameSplits = nameSplits[1:] // Remove "Test"
	Suite := nameSplits[0]
	Scenario := nameSplits[1]

	return &ScenarioManager{
		testingT:  t,
		Func:      targetFunc,
		suite:     Suite,
		scenario:  Scenario,
		waitGroup: &sync.WaitGroup{},
		runLock:   &sync.Mutex{},
	}
}

func (sm *ScenarioManager) NewVariation(origin *ScenarioVariationManager, id string, setting []any) {
	if origin.Invalid {
		return // Invalid variations shouldn't spawn new variations
	}

	for _, v := range setting {
		clone := &ScenarioVariationManager{
			Asserter:      origin.Asserter,
			Invalid:       false,
			VariationData: origin.VariationData.Insert(id, v),
			Parent:        sm,
		}

		sm.RunVariation(clone, true)
	}
}

func (sm *ScenarioManager) RunVariation(svm *ScenarioVariationManager, dryrun bool) {
	sm.waitGroup.Add(1)
	svm.Dryrun = dryrun
	svm.Callcounts = make(map[string]uint) // Clear the callcount map to prevent generating new variations that don't make sense

	if dryrun {
		// Dry run in parallel. Dry runs run very quickly, because they're not realizing anything, just mapping.
		go func() {
			defer sm.waitGroup.Done()
			defer func() {
				// Catch panics, we can't have one bad test killing the entire framework.
				svm.AssertNow("Scenario dry run panicked (recovered)", NoError{stackTrace: true}, recover())
			}()

			sm.Func.Call([]reflect.Value{reflect.ValueOf(svm)})

			if svm.Invalid {
				return // Don't finalize this svm if it's not valid
			}

			svm.Dryrun = false                  // realize it
			sm.finalizedVariationChannel <- svm // submit it for running
		}()
	} else {
		// sanity check
		if svm.Invalid {
			sm.waitGroup.Done() // Clean up the waitgroup addition
			return
		}

		// Do the run for real.
		sm.testingT.Run(svm.VariationName(), func(t *testing.T) {
			defer sm.waitGroup.Done()
			defer func() {
				// Catch panics, we can't have one bad test killing the entire framework.
				svm.AssertNow("Scenario wet run panicked (recovered)", NoError{stackTrace: true}, recover())
			}()

			svm.Asserter = NewTestingAsserter(t)
			sm.Func.Call([]reflect.Value{reflect.ValueOf(svm)})
		})
	}
}

func (sm *ScenarioManager) RunScenario() {
	sm.runLock.Lock()
	defer sm.runLock.Unlock()

	ta := NewTestingAsserter(sm.testingT)

	ta.Log("Discovering variations...")
	//var finalizedVariations []*ScenarioVariationManager
	var finalizedVariationChannel = make(chan *ScenarioVariationManager) // todo processing message struct

	// set up the processor
	sm.finalizedVariationChannel = finalizedVariationChannel
	go func() {
		for {
			variation, ok := <-finalizedVariationChannel
			if !ok {
				return
			}

			sm.RunVariation(variation, false)
		}
	}()

	rootSvm := &ScenarioVariationManager{
		Asserter: ta,
		Dryrun:   true,
		Parent:   sm,
	}

	sm.RunVariation(rootSvm, true)
	sm.waitGroup.Wait()
	close(finalizedVariationChannel)
}
