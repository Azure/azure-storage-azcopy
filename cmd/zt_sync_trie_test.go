package cmd

import (
	chk "gopkg.in/check.v1"
)

type trieSuite struct{}

var _ = chk.Suite(&trieSuite{})

func (*trieSuite) TestPutGet(c *chk.C) {
	o := newObjectTrie(nil)
	o.PutObject("abc", "aoeu")
	o.PutObject("abd", "asdf")

	// test items were added properly
	c.Assert(len(o.items), chk.Equals, 2)

	// test paths didn't overwrite eachother
	data, returned := o.GetObject("abc")
	c.Assert(returned, chk.Equals, true)
	c.Assert(data, chk.Equals, "aoeu")

	data, returned = o.GetObject("abd")
	c.Assert(returned, chk.Equals, true)
	c.Assert(data, chk.Equals, "asdf")

	// test we're properly handling non-entries
	data, returned = o.GetObject("adb")
	c.Assert(returned, chk.Equals, false)
	c.Assert(data, chk.Equals, nil)
}

func (*trieSuite) TestPutDeleteGet(c *chk.C) {
	o := newObjectTrie(nil)
	o.PutObject("abcd", "aoeu")
	o.PutObject("abdc", "asdf")
	o.PutObject("abed", "test")
	o.PutObject("ab", nil) // let's try nil data!

	// Delete the abcd path, leaving abdc and abed
	o.DeleteRecursive("abcd")

	// Check abdc, ab and abed are still in-tact
	data, returned := o.GetObject("abdc")
	c.Assert(returned, chk.Equals, true)
	c.Assert(data, chk.Equals, "asdf")

	data, returned = o.GetObject("abed")
	c.Assert(returned, chk.Equals, true)
	c.Assert(data, chk.Equals, "test")

	data, returned = o.GetObject("ab")
	c.Assert(returned, chk.Equals, true)
	c.Assert(data, chk.Equals, nil)

	// Check abcd is gone
	data, returned = o.GetObject("abcd")
	c.Assert(returned, chk.Equals, false)
	c.Assert(data, chk.Equals, nil)

	// Delete ab, check the other two are in tact
	o.DeleteRecursive("ab")

	// Check abdc, ab and abed are still in-tact
	data, returned = o.GetObject("abdc")
	c.Assert(returned, chk.Equals, true)
	c.Assert(data, chk.Equals, "asdf")

	data, returned = o.GetObject("abed")
	c.Assert(returned, chk.Equals, true)
	c.Assert(data, chk.Equals, "test")

	// ab has nothing?
	data, returned = o.GetObject("ab")
	c.Assert(returned, chk.Equals, false)
	c.Assert(data, chk.Equals, nil)
}

func (*trieSuite) TestGetName(c *chk.C) {
	o := newObjectTrie(nil)
	o.PutObject("abcd", nil)

	c.Assert(o.leaves['a'].leaves['b'].leaves['c'].leaves['d'].GetName(), chk.Equals, "abcd")
}

func (*trieSuite) TestGetIndexes(c *chk.C) {
	o := newObjectTrie(nil)
	indexes := map[string]bool{
		"asdf":     true,
		"aoeu":     true,
		"asong":    true,
		"aoedipus": true,
	}

	for k, v := range indexes {
		o.PutObject(k, v)
	}

	c.Assert(len(o.items), chk.Equals, len(indexes))
	for v := range o.GetIndexes() {
		c.Assert(indexes[v.path], chk.Equals, true)
	}
	for k := range indexes {
		_, present := o.GetObject(k)
		c.Assert(present, chk.Equals, true)
	}
}
