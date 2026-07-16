package common

import (
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

type fakeOncer struct {
	called bool
}

func (f *fakeOncer) Do(_ func()) {
	// intentionally, do not do the specified function, because it's the logging function.
	// we're not operating in parallel, so no concurrency security is needed.
	f.called = true
}

func TestEnvironmentLookup(t *testing.T) {
	a := assert.New(t)

	// only lookup and clear have fundamentally different functionality, because the other functions are breakouts for lookup...

	// we don't want log outputs for our fake environment var, and it's deprecated friends.
	newDone := func() *sync.Once {
		var out = &sync.Once{}
		out.Do(func() {}) // we are done
		return out
	}

	newEnv := EnvironmentVariable{
		Name:         "AZCOPY_NEW_ENV_VAR",
		DefaultValue: "",
		Replaces:     []string{"AZCOPY_DEPRECATED_A", "AZCOPY_DEPRECATED_B"},
	}
	deprecatedA := EnvironmentVariable{
		Name:       newEnv.Replaces[0],
		ReplacedBy: newEnv.Name,
	}
	deprecatedB := EnvironmentVariable{
		Name:         newEnv.Replaces[1],
		DefaultValue: "should-not-be-read",
		ReplacedBy:   newEnv.Name,
	}

	DeprecatedEnvVarsWarnOnce[deprecatedA.Name] = newDone()
	DeprecatedEnvVarsWarnOnce[deprecatedB.Name] = newDone()

	newVal, depAVal, depBVal := "new", "A", "B"

	a.NoError(os.Setenv(newEnv.Name, newVal))
	a.NoError(os.Setenv(deprecatedA.Name, depAVal))
	a.NoError(os.Setenv(deprecatedB.Name, depBVal))

	val, name, ok := newEnv.Lookup()
	// ensure each breakout function returns the expected value
	a.Equal(newVal, newEnv.Value())
	a.Equal(newEnv.Name, newEnv.LookupName())
	a.Equal(true, newEnv.IsSet())
	// ensure each breakout function matches lookup's functionality...
	// these should be the same as our original expected, because those would've failed if not.
	a.Equal(val, newEnv.Value())
	a.Equal(name, newEnv.LookupName())
	a.Equal(ok, newEnv.IsSet())

	// pop a fallback, re-check, it should be the same.
	a.NoError(os.Unsetenv(deprecatedA.Name))

	val, name, ok = newEnv.Lookup()
	// ensure each breakout function returns the expected value
	a.Equal(newVal, newEnv.Value())
	a.Equal(newEnv.Name, newEnv.LookupName())
	a.Equal(true, newEnv.IsSet())
	// ensure each breakout function matches lookup's functionality...
	// these should be the same as our original expected, because those would've failed if not.
	a.Equal(val, newEnv.Value())
	a.Equal(name, newEnv.LookupName())
	a.Equal(ok, newEnv.IsSet())

	// then, pop the newest, and check that it falls back.
	a.NoError(os.Unsetenv(newEnv.Name))

	val, name, ok = newEnv.Lookup()
	// ensure each breakout function returns the expected value
	a.Equal(depBVal, newEnv.Value())
	a.Equal(deprecatedB.Name, newEnv.LookupName())
	a.Equal(true, newEnv.IsSet())
	// ensure each breakout function matches lookup's functionality...
	// these should be the same as our original expected, because those would've failed if not.
	a.Equal(val, newEnv.Value())
	a.Equal(name, newEnv.LookupName())
	a.Equal(ok, newEnv.IsSet())

	// then, make sure that clearing the newest version clears everything.
	newEnv.Clear()
	a.Equal(false, newEnv.IsSet())
	a.Equal(false, deprecatedB.IsSet(true))

	// validate that checking a deprecated var without intentionally doing so panics
	panicked := false
	func() {
		defer func() {
			if err := recover(); err != nil {
				panicked = true
			}
		}()

		_, _, _ = deprecatedB.Lookup()
	}()
	a.Equal(true, panicked)

	// since we cleared everything, reset b.
	a.NoError(os.Setenv(deprecatedB.Name, depBVal))

	// finally, validate that we log when finding that we're on a deprecated variable.
	fakeOnce := &fakeOncer{}
	DeprecatedEnvVarsWarnOnce[deprecatedB.Name] = fakeOnce
	newEnv.Lookup()
	a.Equal(true, fakeOnce.called)

	// look up via the breakout funcs to ensure this is maintained
	fakeOnce.called = false
	newEnv.Value()
	a.Equal(true, fakeOnce.called)

	fakeOnce.called = false
	newEnv.IsSet()
	a.Equal(true, fakeOnce.called)

	fakeOnce.called = false
	newEnv.LookupName()
	a.Equal(true, fakeOnce.called)
}
