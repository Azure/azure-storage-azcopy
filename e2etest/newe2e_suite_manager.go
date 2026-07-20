package e2etest

import (
	"reflect"
	"strings"
	"testing"
)

type SuiteManager struct {
	testingT       *testing.T
	Suites         map[string]any
	EarlyRunSuites map[string]any
}

var suiteManager = &SuiteManager{Suites: make(map[string]any), EarlyRunSuites: make(map[string]any)}

func (sm *SuiteManager) RegisterSuite(Suite any) {
	suiteName := reflect.ValueOf(Suite).Elem().Type().Name()

	sm.Suites[suiteName] = Suite
}

// Early runs do not run in parallel, and run before anything else.
func (sm *SuiteManager) RegisterEarlyRunSuite(Suite any) {
	suiteName := reflect.ValueOf(Suite).Elem().Type().Name()

	sm.EarlyRunSuites[suiteName] = Suite
}

func (sm *SuiteManager) RunSuites(t *testing.T) {
	defer func() {
		NewFrameworkAsserter(t).AssertNow("Suite runner panicked (fatal)", IsNil{}, recover())
	}()

	sm.testingT = t

	tgt := sm.EarlyRunSuites
	early := true
runAllSuites:

	for sName, v := range tgt {
		sVal := reflect.ValueOf(v)
		sTyp := reflect.TypeOf(v)
		mCount := sVal.NumMethod()

		setupIdx := -1
		teardownIdx := -1
		testIdxs := make(map[string]int)
		for idx := 0; idx < mCount; idx++ {
			method := sTyp.Method(idx)
			mName := method.Name

			// in (self, asserter) out ()
			if method.Type.NumIn() != 2 || method.Type.NumOut() != 0 {
				continue
			}
			// check that the first input is actually an asserter
			inName := method.Type.In(1).String()
			if inName != "*e2etest.ScenarioVariationManager" && inName != "e2etest.Asserter" {
				continue
			}

			switch {
			case strings.EqualFold(mName, "SetupSuite"):
				setupIdx = idx
			case strings.EqualFold(mName, "TeardownSuite"):
				teardownIdx = idx
			case strings.HasPrefix(mName, "Scenario_"):
				testIdxs[mName] = idx
			}
		}

		t.Run(sName, func(t *testing.T) {
			// Register teardown before setup so it fires even if setup is skipped.
			if teardownIdx != -1 {
				t.Cleanup(func() {
					defer func() {
						NewFrameworkAsserter(t).AssertNow("Scenario teardown panicked (recovered; you probably need to manually clean up resources)", NoError{stackTrace: true}, recover())
					}()

					sVal.Method(teardownIdx).Call([]reflect.Value{reflect.ValueOf(&FrameworkAsserter{t: t, SuiteName: sName, ScenarioName: "Teardown"})})
				})
			}

			// Run SetupSuite before Parallel — calling t.Run on a parallel t deadlocks.
			if setupIdx != -1 {
				setupSVM := &ScenarioVariationManager{t: t}

				t.Cleanup(func() { // earmark a cleanup so that we can properly log a skip or failure
					if setupSVM.t != nil {
						switch {
						case setupSVM.t.Skipped():
							t.Skip("suite setup skipped")
						case setupSVM.t.Failed():
							t.Fail()
						}
					}
				})

				func() { // run inside a closure to catch the panic appropriately
					defer func() {
						NewFrameworkAsserter(t).AssertNow("Scenario setup panicked (recovered)", NoError{stackTrace: true}, recover())
					}()

					sVal.Method(setupIdx).Call([]reflect.Value{reflect.ValueOf(setupSVM)})
				}()
			}

			if !early { // Early runners must run now.
				t.Parallel() // todo: env var
			}

			if !t.Failed() && !t.Skipped() {
				for scenarioName, scenarioIdx := range testIdxs {
					scenarioName := scenarioName // create intermediate values.
					scenarioIdx := scenarioIdx   // This bug is technically fixed in newer versions of Go, but the fix must be manually applied.

					t.Run(scenarioName, func(t *testing.T) {
						defer func() {
							NewFrameworkAsserter(t).AssertNow("Scenario runner panicked (recovered)", NoError{stackTrace: true}, recover())
						}()

						if !early {
							t.Parallel()
						}

						sm := NewScenarioManager(t, sVal.Method(scenarioIdx))
						sm.runNow = early
						sm.RunScenario()

						_ = scenarioIdx
					})
				}
			} else {
				(&FrameworkAsserter{t: t, SuiteName: sName}).Log("Skipping tests, failed suite setup...")
			}
		})
	}

	if early { // Jump back and run the remaining suites.
		early = false
		tgt = sm.Suites
		goto runAllSuites
	}
}
