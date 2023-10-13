package e2etest

import (
	"fmt"
	"strings"
	"testing"
)

// ====== Asserter ======

type Asserter interface {
	NoError(comment string, err error)
	// Assert fails the test, but does not exit.
	Assert(comment string, assertion Assertion, items ...any)
	// AssertNow wraps Assert, and exits if failed.
	AssertNow(comment string, assertion Assertion, items ...any)
	// Error fails the test, exiting immediately.
	Error(reason string)
	// Skip skips the test, exiting immediately.
	Skip(reason string)
	// Log wraps t.Log with fmt.Sprintf
	Log(format string, a ...any)

	// Failed returns if the test has already failed.
	Failed() bool
}

// ====== Assertion ======

type Assertion interface {
	Name() string
	// MaxArgs must be >= 1; or 0 to indicate no maximum
	MaxArgs() int
	// MinArgs must be 0 or => MaxArgs
	MinArgs() int
	// Assert must operate over all provided items
	Assert(items ...any) bool
}

type FormattedAssertion interface {
	Assertion
	// Format must explain the reason for success or failure in a human-readable format.
	Format(items ...any) string
}

// ====== Implementation ======

type TestingAsserter struct {
	t             *testing.T
	SuiteName     string
	ScenarioName  string
	VariationName string // todo: do we just go through and use fmt.Sprint on all the objects in the variation in order?
}

func NewTestingAsserter(t *testing.T) Asserter {
	nameSplits := strings.Split(t.Name(), "/")
	nameSplits = nameSplits[1:]

	tryIndex := func(idx int) string {
		if len(nameSplits) > idx {
			return nameSplits[idx]
		}

		return ""
	}

	return &TestingAsserter{
		t:             t,
		SuiteName:     tryIndex(0),
		ScenarioName:  tryIndex(1),
		VariationName: tryIndex(2),
	}
}

func (ta *TestingAsserter) GetTestName() string {
	out := ""

	if ta.SuiteName != "" { // Follow the logical progression to produce "Suite/Scenario (Variation)" where available.
		out = ta.SuiteName

		if ta.ScenarioName != "" {
			out += "/" + ta.ScenarioName

			if ta.VariationName != "" {
				out += " (" + ta.VariationName + ")"
			}
		}
	} else {
		// Have a fallback for if a TestingAsserter exists without an associated Suite/Scenario/Variation
		// if the SuiteManager has something to say, it should still be able to, and it should still be clear from whence it came.
		out = "<FRAMEWORK>"
	}

	return out
}

func (ta *TestingAsserter) PrintFinalizingMessage(reasonFormat string, a ...any) {
	ta.t.Helper()
	ta.Log("========== %s ===========", ta.GetTestName())
	ta.Log(reasonFormat, a...)
}

func (ta *TestingAsserter) Log(format string, a ...any) {
	ta.t.Helper()
	ta.t.Log(fmt.Sprintf(format, a...))
}

func (ta *TestingAsserter) NoError(comment string, err error) {
	ta.t.Helper()
	ta.AssertNow(comment, IsNil{}, err)
}

func (ta *TestingAsserter) AssertNow(comment string, assertion Assertion, items ...any) {
	ta.t.Helper()
	ta.Assert(comment, assertion, items...)
	if ta.Failed() {
		ta.t.FailNow()
	}
}

func (ta *TestingAsserter) Assert(comment string, assertion Assertion, items ...any) {
	ta.t.Helper()
	if (assertion.MinArgs() > 0 && len(items) < assertion.MinArgs()) || (assertion.MaxArgs() > 0 && len(items) > assertion.MaxArgs()) {
		ta.PrintFinalizingMessage("Failed to assert: Assertion %s supports argument counts between %d and %d, but received %d args.", assertion.Name(), assertion.MinArgs(), assertion.MaxArgs(), len(items))
		ta.t.FailNow()
	}

	if !assertion.Assert(items...) {
		if fa, ok := assertion.(FormattedAssertion); ok {
			ta.PrintFinalizingMessage("Failed assertion %s: %s; %s", fa.Name(), fa.Format(items...), comment)
		} else {
			ta.PrintFinalizingMessage("Failed assertion %s with item(s): %v; %s", assertion.Name(), items, comment)
		}

		ta.t.Fail()
	}
}

func (ta *TestingAsserter) Error(reason string) {
	ta.t.Helper()
	ta.PrintFinalizingMessage("Test failed: %s", reason)
	ta.t.FailNow()
}

func (ta *TestingAsserter) Skip(reason string) {
	ta.t.Helper()
	ta.PrintFinalizingMessage("Test skipped: %s", reason)
	ta.t.SkipNow()
}

func (ta *TestingAsserter) Failed() bool {
	ta.t.Helper()
	return ta.t.Failed()
}
