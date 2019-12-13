package common

import (
	"sync"
)

// LFUCache implements a thread-safe least frequently used map cache.
type LFUCache struct {
	internalMap *sync.Map

	// freqList is managed as the highest frequency at 0, and the lowest frequency at capacity.
	// If the list is full, the last element will be popped off the end, and a new one will take it's place.
	freqMutex *sync.Mutex
	freqList  []lfuFreqWatch
	freqMap   *sync.Map
	capacity  int
}

type lfuFreqWatch struct {
	usage  int64
	objKey interface{}
}

func NewLFUCache(capacity int) LFUCache {
	cache := LFUCache{
		capacity: capacity,
	}

	cache.internalMap = &sync.Map{}
	cache.freqMap = &sync.Map{}
	cache.freqMutex = &sync.Mutex{}
	cache.freqList = make([]lfuFreqWatch, 0)

	return cache
}

func (c *LFUCache) Set(key, value interface{}) {
	// First, determine whether we need to add the object, or if we're just modifying it.
	if _, ok := c.internalMap.Load(key); ok {
		// If we're modifying it, add to the usage frequency.
		c.incrementFreqWatch(key)
	} else {
		// If we're creating a new object, add a new frequency watch.
		c.newFreqWatch(key)
	}

	c.internalMap.Store(key, value)
}

func (c *LFUCache) Get(key interface{}) (obj interface{}, ok bool) {
	obj, ok = c.internalMap.Load(key)

	// If we actually got the object, we now need to increment it's usage frequency.
	if ok {
		c.incrementFreqWatch(key)
	}

	return
}

func (c *LFUCache) Delete(key interface{}) {
	c.freqMutex.Lock()
	defer c.freqMutex.Unlock()
	// Delete the item from the map first
	c.internalMap.Delete(key)
	tmpIdx, ok := c.freqMap.Load(key)

	if ok {
		idx := tmpIdx.(int)

		c.freqList = append(c.freqList[:idx], c.freqList[idx+1:]...)
	}
}

func (c *LFUCache) Range(mapFunc func(key, value interface{}) bool) {
	// Coat the user's func in a func that handles freq incrementing
	c.internalMap.Range(func(key, value interface{}) bool {
		// Make sure we increment the key.
		c.incrementFreqWatch(key)
		// Then perform the map function.
		return mapFunc(key, value)
	})
}

func (c *LFUCache) newFreqWatch(key interface{}) {
	// First, lock the list.
	c.freqMutex.Lock()
	defer c.freqMutex.Unlock()

	// Determine if we can just add the object or if we need to delete another to make this one happen.
	if len(c.freqList) == c.capacity {
		// Delete the original object
		c.internalMap.Delete(c.freqList[c.capacity-1].objKey)
		// Create a new frequency watcher
		c.freqList[c.capacity-1] = lfuFreqWatch{objKey: key}
		// Inform the freqMap of it.
		c.freqMap.Store(key, c.capacity-1)
	} else {
		// Just append it.
		c.freqList = append(c.freqList, lfuFreqWatch{objKey: key})
		// Inform the freqmap of it.
		c.freqMap.Store(key, len(c.freqList)-1)
	}
}

func (c *LFUCache) incrementFreqWatch(key interface{}) {
	// First, lock the list.
	c.freqMutex.Lock()
	defer c.freqMutex.Unlock()

	// First let's locate the index.
	idx := -1
	if tmpIdx, ok := c.freqMap.Load(key); ok {
		idx = tmpIdx.(int)
	} else {
		panic("key doesn't exist in freqmap")
	}

	// "hidden" functions
	// toContinue: As long as there is a next index to check, and its usage is LESS than the current object's usage, this returns true.
	toContinue := func(cIdx int) bool {
		if cIdx-1 < 0 {
			return false
		} else if c.freqList[idx-1].usage > c.freqList[idx].usage {
			return false
		}

		return true
	}

	// swap: Swaps two items on the freqList.
	swap := func(i, j int) {
		c.freqList[i], c.freqList[j] = c.freqList[j], c.freqList[i]
	}

	// Start by incrementing usage
	c.freqList[idx].usage++

	// Then, traverse down the list until we hit an end condition defined above.
	for toContinue(idx) {
		swap(idx, idx-1)
		idx--
	}

	// Finally, let's update the index.
	c.freqMap.Store(key, idx)
}
