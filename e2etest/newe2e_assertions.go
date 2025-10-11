package e2etest

import (
	"fmt"
	"reflect"
	"runtime/debug"
	"strings"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

var _ Assertion = NoError{}
var _ Assertion = IsNil{}
var _ Assertion = Not{}
var _ Assertion = Empty{}
var _ Assertion = Equal{}
var _ Assertion = MapContains[bool, bool]{}
var _ Assertion = Always{}

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

// MapContains takes in a TargetMap, and multiple KVPair objects, and checks if the map contains all of them.
type MapContains[K comparable, V any] struct {
	TargetMap     map[K]V
	ValueToKVPair func(V) KVPair[K, V]
}

type KVPair[K comparable, V any] struct {
	Key   K
	Value V
}

func (m MapContains[K, V]) Name() string {
	return "MapContains"
}

func (m MapContains[K, V]) MaxArgs() int {
	return 0
}

func (m MapContains[K, V]) MinArgs() int {
	return 0
}

func (m MapContains[K, V]) Assert(items ...any) bool {
	if (m.TargetMap == nil || len(m.TargetMap) == 0) && len(items) > 0 {
		return false // Map is nil, so, can't contain anything!
	}

	for len(items) > 0 {
		v := items[0]
		items = items[1:]
		kvPair, ok := v.(KVPair[K, V])

		if !ok {
			if val, ok := v.(V); ok {
				kvPair = m.ValueToKVPair(val)
			} else if dict, ok := v.(map[K]V); ok {
				for key, value := range dict {
					items = append(items, KVPair[K, V]{key, value})
				}
				continue
			} else {
				panic("MapContains only accepts KVPair[K,V], map[K]V, or V as items")
			}
		}

		val, ok := m.TargetMap[kvPair.Key]
		if !ok {
			return false // map must contain the key
		}

		if !reflect.DeepEqual(val, kvPair.Value) {
			return false
		}
	}

	return true
}

// ====== Always ======

// Always asserts. It's designed to throw quick errors.
type Always struct{}

func (a Always) Name() string {
	return ""
}

func (a Always) MaxArgs() int {
	return 0
}

func (a Always) MinArgs() int {
	return 0
}

func (a Always) Assert(items ...any) bool {
	return false
}

func (a Always) Format(items ...any) string {
	return ""
}
