package e2etest

import (
	"reflect"
	"strings"
	"testing"
)

type SuiteManager struct {
	testingT         *testing.T
	Suites           map[string]any
	ScenarioManagers map[string]any
}

var suiteManager = &SuiteManager{Suites: make(map[string]any), ScenarioManagers: make(map[string]any)}

func (sm *SuiteManager) RegisterSuite(Suite any) {
	suiteName := reflect.ValueOf(Suite).Elem().Type().Name()

	sm.Suites[suiteName] = Suite
	sm.ScenarioManagers[suiteName] = nil // todo SuiteManager
}

func (sm *SuiteManager) RunSuites(t *testing.T) {
	defer func() {
		NewFrameworkAsserter(t).AssertNow("Suite runner panicked (fatal)", IsNil{}, recover())
	}()

	sm.testingT = t

	for sName, v := range sm.Suites {
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

			t.Parallel() // todo: env var

			if setupIdx != -1 {
				// todo: call setup with suite manager
				defer func() {
					NewFrameworkAsserter(t).AssertNow("Scenario setup panicked (recovered)", NoError{stackTrace: true}, recover())
				}()

				sVal.Method(setupIdx).Call([]reflect.Value{reflect.ValueOf(&ScenarioVariationManager{t: t})})
			}

			if !t.Failed() {
				for scenarioName, scenarioIdx := range testIdxs {
					scenarioName := scenarioName // create intermediate values.
					scenarioIdx := scenarioIdx   // This bug is technically fixed in newer versions of Go, but the fix must be manually applied.

					t.Run(scenarioName, func(t *testing.T) {
						defer func() {
							NewFrameworkAsserter(t).AssertNow("Scenario runner panicked (recovered)", NoError{stackTrace: true}, recover())
						}()

						t.Parallel()

						NewScenarioManager(t, sVal.Method(scenarioIdx)).RunScenario()

						_ = scenarioIdx
					})
				}
			} else {
				(&FrameworkAsserter{t: t, SuiteName: sName}).Log("Skipping tests, failed suite setup...")
			}

			if teardownIdx != -1 {
				t.Cleanup(func() {
					defer func() {
						NewFrameworkAsserter(t).AssertNow("Scenario teardown panicked (recovered; you probably need to manually clean up resources)", NoError{stackTrace: true}, recover())
					}()

					sVal.Method(teardownIdx).Call([]reflect.Value{reflect.ValueOf(&FrameworkAsserter{t: t, SuiteName: sName, ScenarioName: "Teardown"})})
				})
			}
		})
	}
}
