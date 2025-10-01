package e2etest

import (
	"fmt"
	"reflect"
	"sync"
)

func FirstOrZero[T any](list []T) T {
	if len(list) != 0 {
		return list[0]
	}

	var zero T
	return zero
}

func FirstOrNil[T any](list []T) *T {
	if len(list) != 0 {
		return &list[0]
	}

	return nil
}

func SetIfZero[T comparable](target *T, result T) {
	var zero T
	if target == nil || *target != zero {
		return
	}

	*target = result
}

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

func Keys[K comparable, V any](in map[K]V) []K {
	out := make([]K, 0, len(in))
	for k, _ := range in {
		out = append(out, k)
	}
	return out
}

func AnyKeys[K comparable, V any](in map[K]V) []any {
	out := make([]any, 0, len(in))
	for k, _ := range in {
		out = append(out, k)
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

func CloneMapWithRule[K comparable, V any](in map[K]V, rule func(K, V) (key K, value V, include bool)) map[K]V {
	out := make(map[K]V)

	for k, v := range in {
		var include bool
		k, v, include = rule(k, v)

		if !include {
			continue
		}

		out[k] = v
	}

	return out
}

func ListContains[I comparable](item I, in []I) bool {
	for _, v := range in {
		if item == v {
			return true
		}
	}

	return false
}

func Any[I any](items []I, f func(I) bool) bool {
	for _, v := range items {
		if f(v) {
			return true
		}
	}

	return false
}

func ClonePointer[T any](in *T) *T {
	if in == nil {
		return nil
	}

	out := *in

	return &out
}

func JoinMap[K comparable, V any](in ...map[K]V) map[K]V {
	out := map[K]V{}

	for _, dict := range in {
		for k, v := range dict {
			out[k] = v
		}
	}

	return out
}

type RWMutexResource[T any] struct {
	res  T
	rwmu *sync.RWMutex
}

func NewRWMutexResource[T any](res T) *RWMutexResource[T] {
	return &RWMutexResource[T]{
		res:  res,
		rwmu: &sync.RWMutex{},
	}
}

func (r *RWMutexResource[T]) DoRead(f func(res T)) {
	r.rwmu.RLock()
	defer r.rwmu.RUnlock()
	f(r.res)
}

func (r *RWMutexResource[T]) DoWrite(f func(res T)) {
	r.rwmu.Lock()
	defer r.rwmu.Unlock()
	f(r.res)
}
