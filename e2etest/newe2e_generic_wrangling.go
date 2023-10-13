package e2etest

import (
	"fmt"
	"reflect"
)

func GetTypeOrZero[T any](in any) (out T) {
	if out, ok := in.(T); ok {
		return out
	}

	return
}

func GetTypeOrAssert[T any](a Asserter, in any) (out T) {
	if out, ok := in.(T); ok {
		return out
	}

	if in != nil {
		inType := reflect.ValueOf(in).Elem().Type().String()
		outType := reflect.ValueOf(out).Type().String()
		a.Error(fmt.Sprintf("in type (%s) is not compatible with out type (%s)", inType, outType))
	}

	return
}

func DerefOrZero[T any](in *T) (out T) {
	if in != nil {
		out = *in
	}

	return
}

func DerefOrDefault[T any](in *T, def T) T {
	if in != nil {
		return *in
	}

	return def
}

func PtrOf[T any](in T) (out *T) {
	return &in
}

func IsZero[T comparable](in T) bool {
	var zero T
	return in == zero
}

func ListOfAny[T any](in []T) []any {
	out := make([]any, len(in))

	for k, v := range in {
		out[k] = v
	}

	return out
}

func CloneMap[K comparable, V any](in map[K]V) map[K]V {
	out := make(map[K]V)

	for k, v := range in {
		out[k] = v
	}

	return out
}
