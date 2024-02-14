package e2etest

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"reflect"
	"runtime/debug"
	"strings"
)

// ====== NoError ======

// NoError works like IsNil but only asserts when there is an error, and formats errors
type NoError struct {
	// set stackTrace to true to get the panic stack trace
	stackTrace bool

	// todo: traced error decorations
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
		if v != nil {
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

// ====== Empty =======

// Empty checks that all parameters are equivalent to their zero-values
type Empty struct {
	// Invert is distinctly different from Not{Empty{}}, in that Not{Empty{}} states "if *any* object is not empty", but Empty{Invert: true} specifies that ALL objects are nonzero
	Invert bool
}

func (e Empty) Name() string {
	return "IsEmpty"
}

func (e Empty) MaxArgs() int {
	return 0
}

func (e Empty) MinArgs() int {
	return 0
}

func (e Empty) Assert(items ...any) bool {
	for _, v := range items {
		item := reflect.ValueOf(v)
		// false (all objects are zero) == false (the object is not zero); failure
		// true (all objects are nonzero) == true (the object is zero); failure
		if e.Invert == item.IsZero() {
			return false
		}
	}

	return true
}

func (e Empty) Format(items ...any) string {
	failed := make([]uint, 0)

	for idx, v := range items {
		item := reflect.ValueOf(v)
		// false (all objects are zero) == false (the object is not zero); failure
		// true (all objects are nonzero) == true (the object is zero); failure
		if e.Invert == item.IsZero() {
			failed = append(failed, uint(idx))
		}
	}

	if len(failed) == 0 {
		return "all items were " + common.Iff(e.Invert, "nonzero", "zero")
	}

	trait := common.Iff(e.Invert, "zero, expected nonzero values", "nonzero, expected zero values")

	return fmt.Sprintf("items %v were %s", failed, trait)
}

// ====== Equal =======

// Equal checks that all parameters are equal.
type Equal struct {
	Deep bool
}

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
		if !e.Deep {
			if left != right {
				return false
			}
		} else {
			if !reflect.DeepEqual(left, right) {
				return false
			}
		}
	}

	return true
}

// ====== Contains ======

// Contains checks that all parameters are included within the array (or map's keys)
