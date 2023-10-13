package e2etest

import (
	"fmt"
	"runtime/debug"
	"strings"
)

// ====== NoError ======

// NoError works like IsNil but only asserts when there is an error, and formats errors
type NoError struct {
	// set stackTrace to true to get the panic stack trace
	stackTrace bool
}

func (n NoError) Name() string {
	return "NoError"
}

func (n NoError) MaxArgs() int {
	return 0
}

func (n NoError) MinArgs() int {
	return 0
}

func (n NoError) Assert(items ...any) bool {
	for _, v := range items {
		if _, ok := v.(error); ok {
			return false
		}
	}

	return true
}

func (n NoError) Format(items ...any) string {
	out := ""
	for _, v := range items {
		if err, ok := v.(error); ok {
			out += err.Error() + "\n\n"
		} else if v != nil {
			out += fmt.Sprintf("item %s was not an error, but also not nil\n\n", v)
		}
	}

	if n.stackTrace {
		stack := debug.Stack()
		out += string(stack)
	}

	strings.TrimSuffix(out, "\n\n")

	return out
}

// ====== IsNil ======

// IsNil checks that all parameters are nil.
type IsNil struct{}

func (i IsNil) Name() string {
	return "IsNil"
}

func (i IsNil) MaxArgs() int {
	return 0
}

func (i IsNil) MinArgs() int {
	return 0
}

func (i IsNil) Assert(items ...any) bool {
	for _, v := range items {
		if v != nil {
			return false
		}
	}

	return true
}

// ====== Not ======

// Not inverts the contained Assertion.
type Not struct {
	a Assertion
}

func (n Not) Name() string {
	return "Not(" + n.a.Name() + ")"
}

func (n Not) MaxArgs() int {
	return n.a.MaxArgs()
}

func (n Not) MinArgs() int {
	return n.a.MinArgs()
}

func (n Not) Assert(items ...any) bool {
	return !n.a.Assert(items...)
}

// ====== Equal =======

// Equal checks that all parameters are equal.
type Equal struct{}

func (e Equal) Name() string {
	return "Equal"
}

func (e Equal) MaxArgs() int {
	return 0
}

func (e Equal) MinArgs() int {
	return 0
}

func (e Equal) Assert(items ...any) bool {
	if len(items) == 0 {
		return true
	}

	left := items[0]
	for _, right := range items[1:] {
		if left != right {
			return false
		}
	}

	return true
}

// ====== Contains ======

// Contains checks that all parameters are included within the array (or map's keys)
