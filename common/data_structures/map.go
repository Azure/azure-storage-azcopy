package data_structures

import "sync"

type SyncMap[K comparable, V any] interface {
	// package-restrict the interface.
	syncMapImpl()

	Load(key K) (val V, ok bool)
	Store(key K, value V)
	LoadOrStore(key K, value V) (result V, loaded bool)
	LoadAndDelete(key K) (value V, loaded bool)
	Delete(key K)
	Swap(key K, value V) (previous V, loaded bool)
	CompareAndSwap(key K, new, old V) (swapped bool)
	CompareAndDelete(key K, old V) (deleted bool)
	Range(f func(key, value any) bool)
}

type syncMap[K comparable, V any] struct {
	m sync.Map
}

func NewSyncMap[K comparable, V any]() SyncMap[K, V] {
	return &syncMap[K, V]{
		m: sync.Map{},
	}
}

func (v *syncMap[K, V]) Load(key K) (val V, ok bool) {
	out, ok := v.m.Load(key)
	val, _ = out.(V)
	return
}

func (v *syncMap[K, V]) Store(key K, value V) {
	v.m.Store(key, value)
}

func (v *syncMap[K, V]) LoadOrStore(key K, value V) (result V, loaded bool) {
	out, loaded := v.m.LoadOrStore(key, value)
	result, _ = out.(V)
	return
}

func (v *syncMap[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
	out, loaded := v.m.LoadAndDelete(key)
	value, _ = out.(V)
	return
}

func (v *syncMap[K, V]) Delete(key K) {
	v.m.Delete(key)
}

func (v *syncMap[K, V]) Swap(key K, value V) (previous V, loaded bool) {
	out, loaded := v.m.Swap(key, value)
	previous = out.(V)
	return
}

func (v *syncMap[K, V]) CompareAndSwap(key K, new, old V) (swapped bool) {
	return v.m.CompareAndSwap(key, new, old)
}

func (v *syncMap[K, V]) CompareAndDelete(key K, old V) (deleted bool) {
	return v.m.CompareAndDelete(key, old)
}

func (v *syncMap[K, V]) Range(f func(key any, value any) bool) {
	v.m.Range(func(key, value any) bool {
		return f(key.(K), value.(V))
	})
}

func (*syncMap[K, V]) syncMapImpl() {}
