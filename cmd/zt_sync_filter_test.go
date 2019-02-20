// Copyright © 2017 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cmd

import (
	chk "gopkg.in/check.v1"
	"time"
)

type syncFilterSuite struct{}

var _ = chk.Suite(&syncFilterSuite{})

func (s *syncFilterSuite) TestSyncSourceFilter(c *chk.C) {
	// set up the indexer as well as the source filter
	indexer := newObjectIndexer()
	sourceFilter := newSyncSourceFilter(indexer)

	// create a sample destination object
	sampleDestinationObject := storedObject{name: "test", relativePath: "/usr/test", lastModifiedTime: time.Now()}

	// test the filter in case a given source object is not present at the destination
	// meaning no entry in the index, so the filter should pass the given object to schedule a transfer
	passed := sourceFilter.doesPass(storedObject{name: "only_at_source", relativePath: "only_at_source", lastModifiedTime: time.Now()})
	c.Assert(passed, chk.Equals, true)

	// test the filter in case a given source object is present at the destination
	// and it has a later modified time, so the filter should pass the give object to schedule a transfer
	err := indexer.store(sampleDestinationObject)
	c.Assert(err, chk.IsNil)
	passed = sourceFilter.doesPass(storedObject{name: "test", relativePath: "/usr/test", lastModifiedTime: time.Now()})
	c.Assert(passed, chk.Equals, true)

	// test the filter in case a given source object is present at the destination
	// but is has an earlier modified time compared to the one at the destination
	// meaning that the source object is considered stale, so no transfer should be scheduled
	err = indexer.store(sampleDestinationObject)
	c.Assert(err, chk.IsNil)
	passed = sourceFilter.doesPass(storedObject{name: "test", relativePath: "/usr/test", lastModifiedTime: time.Now().Add(-time.Hour)})
	c.Assert(passed, chk.Equals, false)
}

func (s *syncFilterSuite) TestSyncDestinationFilter(c *chk.C) {
	// set up the indexer as well as the destination filter
	indexer := newObjectIndexer()
	dummyProcessor := dummyProcessor{}
	destinationFilter := newSyncDestinationFilter(indexer, dummyProcessor.process)

	// create a sample source object
	sampleSourceObject := storedObject{name: "test", relativePath: "/usr/test", lastModifiedTime: time.Now()}

	// test the filter in case a given destination object is not present at the source
	// meaning it is an extra file that needs to be deleted, so the filter should pass the given object to the destinationCleaner
	passed := destinationFilter.doesPass(storedObject{name: "only_at_source", relativePath: "only_at_source", lastModifiedTime: time.Now()})
	c.Assert(passed, chk.Equals, false)
	c.Assert(len(dummyProcessor.record), chk.Equals, 1)
	c.Assert(dummyProcessor.record[0].name, chk.Equals, "only_at_source")

	// reset dummy processor
	dummyProcessor.record = make([]storedObject, 0)

	// test the filter in case a given destination object is present at the source
	// and it has a later modified time, since the source data is stale,
	// the filter should pass not the give object to schedule a transfer
	err := indexer.store(sampleSourceObject)
	c.Assert(err, chk.IsNil)
	passed = destinationFilter.doesPass(storedObject{name: "test", relativePath: "/usr/test", lastModifiedTime: time.Now()})
	c.Assert(passed, chk.Equals, false)
	c.Assert(len(dummyProcessor.record), chk.Equals, 0)

	// test the filter in case a given destination object is present at the source
	// but is has an earlier modified time compared to the one at the source
	// meaning that the source object should be transferred since the destination object is stale
	err = indexer.store(sampleSourceObject)
	c.Assert(err, chk.IsNil)
	passed = destinationFilter.doesPass(storedObject{name: "test", relativePath: "/usr/test", lastModifiedTime: time.Now().Add(-time.Hour)})
	c.Assert(passed, chk.Equals, true)
	c.Assert(len(dummyProcessor.record), chk.Equals, 0)
}
