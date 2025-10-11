package main

import (
	"errors"
	"fmt"

	"github.com/Azure/azure-storage-azcopy/v10/e2etest"
)

type DummyAsserter struct {
	CaughtError error
}

func (d *DummyAsserter) NoError(comment string, err error, failNow ...bool) {
	if err != nil {
		d.CaughtError = fmt.Errorf("%s: %w", comment, err)

		if e2etest.FirstOrZero(failNow) {
			panic(d.CaughtError)
		}
	}
}

func (d *DummyAsserter) Assert(comment string, assertion e2etest.Assertion, items ...any) {
	if len(items) < assertion.MinArgs() || len(items) > assertion.MaxArgs() {
		panic("assertion item count mismatch")
	}

	if !assertion.Assert(items...) {
		var err error
		if fa, ok := assertion.(e2etest.FormattedAssertion); ok {
			err = fmt.Errorf("assertion %s failed: %s (%s)", fa.Name(), fa.Format(items...), comment)
		} else {
			err = fmt.Errorf("assertion %s failed with items %v (%s)", assertion.Name(), items, comment)
		}

		d.CaughtError = err
	}
}

func (d *DummyAsserter) AssertNow(comment string, assertion e2etest.Assertion, items ...any) {
	if len(items) < assertion.MinArgs() || len(items) > assertion.MaxArgs() {
		panic("assertion item count mismatch")
	}

	if !assertion.Assert(items...) {
		var err error
		if fa, ok := assertion.(e2etest.FormattedAssertion); ok {
			err = fmt.Errorf("assertion %s failed: %s (%s)", fa.Name(), fa.Format(items...), comment)
		} else {
			err = fmt.Errorf("assertion %s failed with items %v (%s)", assertion.Name(), items, comment)
		}

		d.CaughtError = err
		panic(d.CaughtError)
	}
}

func (d *DummyAsserter) Error(reason string) {
	d.CaughtError = errors.New(reason)
}

func (d *DummyAsserter) Skip(reason string) {
	// no-op
}

func (d *DummyAsserter) Log(format string, a ...any) {
	fmt.Printf(format, a...)
}

func (d *DummyAsserter) Failed() bool {
	return d.CaughtError != nil
}

func (d *DummyAsserter) HelperMarker() e2etest.HelperMarker {
	return DummyHelper{}
}

func (d *DummyAsserter) GetTestName() string {
	return ""
}

type DummyHelper struct{}

func (d DummyHelper) Helper() {} // noop
