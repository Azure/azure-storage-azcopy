package common_test

import (
	chk "gopkg.in/check.v1"

	"github.com/Azure/azure-storage-azcopy/common"
)

type LFUCacheTestSuite struct{}

var _ = chk.Suite(&LFUCacheTestSuite{})

func (s *LFUCacheTestSuite) TestBasicStoreGet(c *chk.C) {
	cache := common.NewLFUCache(4)

	fakeKey := "nonexistent"
	realKey := "test"

	keys := map[string]int{
		realKey:             5,
		"many key such wow": 3,
		"sudo is just a polite way to say please": 25,
		"key": 2,
	}

	// Set the map's values.
	for k, v := range keys {
		cache.Set(k, v)
	}

	// Ensure we can't get an unknown key.
	val, ok := cache.Get(fakeKey)
	c.Assert(ok, chk.Equals, false)
	c.Assert(val, chk.IsNil)

	// Ensure we can get a currently existing key.
	val, ok = cache.Get(realKey)
	c.Assert(ok, chk.Equals, true)
	c.Assert(val.(int), chk.Equals, keys[realKey])

	// Now that the map is full, let's try to put a new key and check that only one of the other keys has disappeared.
	cache.Set("newkey", 70531)
	val, ok = cache.Get("newkey")
	c.Assert(ok, chk.Equals, true)
	c.Assert(val.(int), chk.Equals, 70531)

	gone := 0
	for k := range keys {
		_, ok = cache.Get(k)
		if !ok {
			gone++
		}
	}

	c.Assert(gone, chk.Equals, 1)

	// This proves 3 things about our cache:
	// 1) Our cache can add keys
	// 2) Our cache can get values
	// 3) Our cache drops a value when needed.
}

func (s *LFUCacheTestSuite) TestDropOrder(c *chk.C) {
	cache := common.NewLFUCache(4)

	// The index indicates how many times we'll get an object to ensure it doesn't disappear.
	keysToStay := map[string]int{
		"last but not least":   4,
		"third down the drain": 3,
		"aoeuidhtns":           2,
	}

	for k, v := range keysToStay {
		cache.Set(k, v)

		for i := v; i > 0; i-- {
			cache.Get(k)
		}
	}

	// set a dummy value that'll be wiped out by the next one.
	cache.Set("xyz", 8)
	cache.Set("abc", 30)
	val, ok := cache.Get("xyz")
	c.Assert(ok, chk.Equals, false)
	c.Assert(val, chk.Equals, nil)
	val, ok = cache.Get("abc")
	c.Assert(ok, chk.Equals, true)
	c.Assert(val.(int), chk.Equals, 30)

	// make that dummy king and ensure that aoeuidhtns goes next
	for i := 30; i > 0; i-- {
		cache.Get("abc")
	}

	// Set a new value so aoeuidhtns disappears
	cache.Set("new dummy value", 2389)

	// Ensure abc stuck around, since it's now king.
	val, ok = cache.Get("abc")
	c.Assert(ok, chk.Equals, true)
	c.Assert(val.(int), chk.Equals, 30)

	// Ensure that aoeuidhtns went away
	val, ok = cache.Get("aoeuidhtns")
	c.Assert(ok, chk.Equals, false)
	c.Assert(val, chk.IsNil)

	// What this test verifies:
	// 1) Values can usurp others in terms of usage
	// 2) The least used values get deleted
}
