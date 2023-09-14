package e2etest

import (
	"fmt"
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
	t            *testing.T
	TestName     string
	ScenarioName string
	Step         string
}

func (ta *TestingAsserter) PrintFinalizingMessage(reasonFormat string, a ...any) {
	ta.Log("========== %s/%s: at step %s ===========", ta.TestName, ta.ScenarioName, ta.Step)
	ta.Log(reasonFormat, a...)
	ta.Log("========== %s/%s: at step %s ===========", ta.TestName, ta.ScenarioName, ta.Step)
}

func (ta *TestingAsserter) Log(format string, a ...any) {
	ta.t.Log(fmt.Sprintf(format, a...))
}

func (ta *TestingAsserter) NoError(comment string, err error) {
	ta.AssertNow(comment, IsNil{}, err)
}

func (ta *TestingAsserter) AssertNow(comment string, assertion Assertion, items ...any) {
	ta.Assert(comment, assertion, items...)
	if ta.Failed() {
		ta.t.FailNow()
	}
}

func (ta *TestingAsserter) Assert(comment string, assertion Assertion, items ...any) {
	if (assertion.MinArgs() > 0 && len(items) < assertion.MinArgs()) || (assertion.MaxArgs() > 0 && len(items) > assertion.MaxArgs()) {
		ta.PrintFinalizingMessage("Failed to assert: Assertion %s supports argument counts between %d and %d, but received %d args.", assertion.Name(), assertion.MinArgs(), assertion.MaxArgs(), len(items))
		ta.t.FailNow()
	}

	if !assertion.Assert(items...) {
		if fa, ok := assertion.(FormattedAssertion); ok {
			ta.PrintFinalizingMessage("Failed assertion %s: %s", fa.Name(), fa.Format(items...))
		} else {
			ta.PrintFinalizingMessage("Failed assertion %s with item(s): %v", assertion.Name(), items)
		}

		ta.t.Fail()
	}
}

func (ta *TestingAsserter) Error(reason string) {
	ta.PrintFinalizingMessage("Test failed: %s", reason)
	ta.t.FailNow()
}

func (ta *TestingAsserter) Skip(reason string) {
	ta.PrintFinalizingMessage("Test skipped: %s", reason)
	ta.t.SkipNow()
}

func (ta *TestingAsserter) Failed() bool {
	return ta.t.Failed()
}
