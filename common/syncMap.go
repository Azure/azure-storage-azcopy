package common

import "sync"

type SyncMap struct {
	lock sync.RWMutex
	m    map[string]string
}

func (sm *SyncMap) Set(key string, value string) {
	sm.lock.Lock()
	sm.m[key] = value
	sm.lock.Unlock()
}
func (sm *SyncMap) Get(key string) (value string, ok bool) {
	sm.lock.RLock()
	value, ok = sm.m[key]
	sm.lock.RUnlock()
	return
}
func (sm *SyncMap) Delete(key string) {
	sm.lock.Lock()
	delete(sm.m, key)
	sm.lock.Unlock()
}

func (sm *SyncMap) Iterate(readonly bool, f func(k string, v string)) {
	locker := sync.Locker(&sm.lock)
	if !readonly {
		locker = sm.lock.RLocker()
	}
	locker.Lock()
	for k, v := range sm.m {
		f(k, v)
	}
	locker.Unlock()
}
