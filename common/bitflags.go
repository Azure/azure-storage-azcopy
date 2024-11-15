package common

import (
	"golang.org/x/exp/constraints"
)

func BitflagsContainAll[T constraints.Unsigned](flags, test T) bool {
	return flags&test == test
}

func BitflagsContainAny[T constraints.Unsigned](flags, test T) bool {
	return flags&test != 0
}

func BitflagsAdd[T constraints.Unsigned](flags, add T) T {
	return flags | add
}

func BitflagsRemove[T constraints.Unsigned](flags, remove T) T {
	return flags & (^remove)
}
