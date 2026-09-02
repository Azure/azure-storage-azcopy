package data_structures

import "iter"

type Set[T comparable] interface {
	// package-restrict the interface.
	setImpl()

	Add(value T)
	Remove(value T)
	Contains(value T) bool
	Len() int
	// Values returns all elements in no guaranteed order.
	Values() iter.Seq[T]
}

type set[T comparable] struct {
	data map[T]struct{}
}

func NewSet[T comparable](values ...T) Set[T] {
	s := &set[T]{data: make(map[T]struct{})}
	for _, v := range values {
		s.data[v] = struct{}{}
	}
	return s
}

func (s *set[T]) setImpl() {}

func (s *set[T]) Add(value T) {
	s.data[value] = struct{}{}
}

func (s *set[T]) Remove(value T) {
	delete(s.data, value)
}

func (s *set[T]) Contains(value T) bool {
	_, ok := s.data[value]
	return ok
}

func (s *set[T]) Len() int {
	return len(s.data)
}

func (s *set[T]) Values() iter.Seq[T] {
	return func(yield func(T) bool) {
		for k := range s.data {
			if !yield(k) {
				return
			}
		}
	}
}
