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

type syncComparatorSuite struct{}

var _ = chk.Suite(&syncComparatorSuite{})

func (s *syncComparatorSuite) SetUpTest(c *chk.C) {
	c.Log("Still running tests... ", c.TestName())
}

func (s *syncComparatorSuite) TestSyncSourceComparator(c *chk.C) {
	dummyCopyScheduler := dummyProcessor{}
	srcMD5 := []byte{'s'}
	destMD5 := []byte{'d'}

	// set up the indexer as well as the source comparator
	indexer := newObjectIndexer()
	sourceComparator := newSyncSourceComparator(indexer, dummyCopyScheduler.process)

	// create a sample destination object
	sampleDestinationObject := storedObject{name: "test", relativePath: "/usr/test", lastModifiedTime: time.Now(), md5: destMD5}

	// test the comparator in case a given source object is not present at the destination
	// meaning no entry in the index, so the comparator should pass the given object to schedule a transfer
	compareErr := sourceComparator.processIfNecessary(storedObject{name: "only_at_source", relativePath: "only_at_source", lastModifiedTime: time.Now(), md5: srcMD5})
	c.Assert(compareErr, chk.Equals, nil)

	// check the source object was indeed scheduled
	c.Assert(len(dummyCopyScheduler.record), chk.Equals, 1)
	c.Assert(dummyCopyScheduler.record[0].md5, chk.DeepEquals, srcMD5)

	// reset the processor so that it's empty
	dummyCopyScheduler = dummyProcessor{}

	// test the comparator in case a given source object is present at the destination
	// and it has a later modified time, so the comparator should pass the give object to schedule a transfer
	err := indexer.store(sampleDestinationObject)
	c.Assert(err, chk.IsNil)
	compareErr = sourceComparator.processIfNecessary(storedObject{name: "test", relativePath: "/usr/test", lastModifiedTime: time.Now().Add(time.Hour), md5: srcMD5})
	c.Assert(compareErr, chk.Equals, nil)

	// check the source object was indeed scheduled
	c.Assert(len(dummyCopyScheduler.record), chk.Equals, 1)
	c.Assert(dummyCopyScheduler.record[0].md5, chk.DeepEquals, srcMD5)
	c.Assert(len(indexer.indexMap), chk.Equals, 0)

	// reset the processor so that it's empty
	dummyCopyScheduler = dummyProcessor{}

	// test the comparator in case a given source object is present at the destination
	// but is has an earlier modified time compared to the one at the destination
	// meaning that the source object is considered stale, so no transfer should be scheduled
	err = indexer.store(sampleDestinationObject)
	c.Assert(err, chk.IsNil)
	compareErr = sourceComparator.processIfNecessary(storedObject{name: "test", relativePath: "/usr/test", lastModifiedTime: time.Now().Add(-time.Hour), md5: srcMD5})
	c.Assert(compareErr, chk.Equals, nil)

	// check no source object was scheduled
	c.Assert(len(dummyCopyScheduler.record), chk.Equals, 0)
	c.Assert(len(indexer.indexMap), chk.Equals, 0)
}

func (s *syncComparatorSuite) TestSyncDestinationComparator(c *chk.C) {
	dummyCopyScheduler := dummyProcessor{}
	dummyCleaner := dummyProcessor{}
	srcMD5 := []byte{'s'}
	destMD5 := []byte{'d'}

	// set up the indexer as well as the destination comparator
	indexer := newObjectIndexer()
	destinationComparator := newSyncDestinationComparator(indexer, dummyCopyScheduler.process, dummyCleaner.process)

	// create a sample source object
	sampleSourceObject := storedObject{name: "test", relativePath: "/usr/test", lastModifiedTime: time.Now(), md5: srcMD5}

	// test the comparator in case a given destination object is not present at the source
	// meaning it is an extra file that needs to be deleted, so the comparator should pass the given object to the destinationCleaner
	compareErr := destinationComparator.processIfNecessary(storedObject{name: "only_at_dst", relativePath: "only_at_dst", lastModifiedTime: time.Now(), md5: destMD5})
	c.Assert(compareErr, chk.Equals, nil)

	// verify that destination object is being deleted
	c.Assert(len(dummyCopyScheduler.record), chk.Equals, 0)
	c.Assert(len(dummyCleaner.record), chk.Equals, 1)
	c.Assert(dummyCleaner.record[0].md5, chk.DeepEquals, destMD5)

	// reset dummy processors
	dummyCopyScheduler = dummyProcessor{}
	dummyCleaner = dummyProcessor{}

	// test the comparator in case a given destination object is present at the source
	// and it has a later modified time, since the source data is stale,
	// no transfer happens
	err := indexer.store(sampleSourceObject)
	c.Assert(err, chk.IsNil)
	compareErr = destinationComparator.processIfNecessary(storedObject{name: "test", relativePath: "/usr/test", lastModifiedTime: time.Now().Add(time.Hour), md5: destMD5})
	c.Assert(compareErr, chk.Equals, nil)

	// verify that the source object is scheduled for transfer
	c.Assert(len(dummyCopyScheduler.record), chk.Equals, 0)
	c.Assert(len(dummyCleaner.record), chk.Equals, 0)

	// reset dummy processors
	dummyCopyScheduler = dummyProcessor{}
	dummyCleaner = dummyProcessor{}

	// test the comparator in case a given destination object is present at the source
	// but is has an earlier modified time compared to the one at the source
	// meaning that the source object should be transferred since the destination object is stale
	err = indexer.store(sampleSourceObject)
	c.Assert(err, chk.IsNil)
	compareErr = destinationComparator.processIfNecessary(storedObject{name: "test", relativePath: "/usr/test", lastModifiedTime: time.Now().Add(-time.Hour), md5: destMD5})
	c.Assert(compareErr, chk.Equals, nil)

	// verify that there's no transfer & no deletes
	c.Assert(len(dummyCopyScheduler.record), chk.Equals, 1)
	c.Assert(dummyCopyScheduler.record[0].md5, chk.DeepEquals, srcMD5)
	c.Assert(len(dummyCleaner.record), chk.Equals, 0)
}
