package e2etest

import (
	"github.com/google/uuid"
	"reflect"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
)

type ScenarioManager struct {
	testingT *testing.T
	Func     reflect.Value

	// Skip the line, don't run parallel.
	runNow bool

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
		return // IsInvalid variations shouldn't spawn new variations
	}

	for i := len(setting) - 1; i >= 0; i-- { // Because the stack is FIFO, insert the first terms last to match expected variation ordering.
		v := setting[i]
		clone := &ScenarioVariationManager{
			VariationData: origin.VariationData.Insert(id, v),
			VariationUUID: uuid.New(),
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
		{Parent: sm, callcounts: make(map[string]uint), VariationUUID: uuid.New()}, // Root svm, no variations, no nothing.
	}

	for len(sm.varStack) > 0 {
		svm := sm.varStack[len(sm.varStack)-1] // *pop*!
		sm.varStack = sm.varStack[:len(sm.varStack)-1]
		panicked := false
		var panicError any
		var panicStack []byte

		func() {
			defer func() {
				if err := recover(); err != nil {
					panicError = err
					panicStack = debug.Stack()
					panicked = true
				}
			}()

			sm.Func.Call([]reflect.Value{reflect.ValueOf(svm)}) // Test will push onto the stack a bunch
		}()

		if !svm.isInvalid { // If we made a real test
			svm.runNow = sm.runNow
			sm.testingT.Run(svm.VariationName(), func(t *testing.T) {
				defer func() {
					if err := recover(); err != nil {
						stack := debug.Stack()
						t.Logf("scenario variation panicked: %v\n\n%s", err, string(stack))
						t.FailNow()
					}
				}()

				svm.t = t
				svm.callcounts = make(map[string]uint)

				if panicked {
					t.Logf("Variation %s dryrun panicked: %v;\n%v", svm.VariationName(), panicError, string(panicStack))
					t.FailNow()
				}

				if !svm.runNow {
					t.Parallel()
				}

				t.Cleanup(func() {
					c := ScenarioVariationManagerCleanupAsserter{svm: svm}

					// Reverted to LIFO
					for i := len(svm.CleanupFuncs) - 1; i >= 0; i-- {
						c.WrapCleanup(svm.CleanupFuncs[i])
					}

					svm.DeleteCreatedResources()
				})

				sm.Func.Call([]reflect.Value{reflect.ValueOf(svm)})

				if svm.isInvalid {
					t.Fail() // If FailNow hasn't already been called, we should fail.
				}
			})
		}
	}
}
