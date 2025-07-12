package common

import (
	"golang.org/x/exp/constraints"
)

type Atomic[T any] interface {
	Store(x T)
	Load() T
	CompareAndSwap(old T, new T) (swapped bool)
}

type AtomicNumeric[T constraints.Integer] interface {
	Atomic[T]
	Add(n T) T
	And(n T) T
	Or(n T) T
}

func AtomicSubtract[T constraints.Integer](left AtomicNumeric[T], right T) T {
	return AtomicMorph(left, func(startVal T) (val T, res T) {
		out := startVal - right
		return out, out
	})
}
