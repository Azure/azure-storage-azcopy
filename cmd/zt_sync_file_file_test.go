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
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
	"github.com/stretchr/testify/assert"
)

func TestSyncSourceComparator(t *testing.T) {
	a := assert.New(t)
	dummyCopyScheduler := dummyProcessor{}
	srcMD5 := []byte{'s'}
	destMD5 := []byte{'d'}

	// set up the indexer as well as the source comparator
	indexer := traverser.NewObjectIndexer()
	sourceComparator := newSyncSourceComparator(indexer, dummyCopyScheduler.process, common.ESyncHashType.None(), false, false)

	// create a sample destination object
	sampleDestinationObject := traverser.StoredObject{Name: "test", RelativePath: "/usr/test", LastModifiedTime: time.Now(), Md5: destMD5}

	// test the comparator in case a given source object is not present at the destination
	// meaning no entry in the index, so the comparator should pass the given object to schedule a transfer
	compareErr := sourceComparator.processIfNecessary(traverser.StoredObject{Name: "only_at_source", RelativePath: "only_at_source", LastModifiedTime: time.Now(), Md5: srcMD5})
	a.Nil(compareErr)

	// check the source object was indeed scheduled
	a.Equal(1, len(dummyCopyScheduler.record))

	a.Equal(srcMD5, dummyCopyScheduler.record[0].Md5)

	// reset the processor so that it's empty
	dummyCopyScheduler = dummyProcessor{}

	// test the comparator in case a given source object is present at the destination
	// and it has a later modified time, so the comparator should pass the give object to schedule a transfer
	err := indexer.Store(sampleDestinationObject)
	a.Nil(err)
	compareErr = sourceComparator.processIfNecessary(traverser.StoredObject{Name: "test", RelativePath: "/usr/test", LastModifiedTime: time.Now().Add(time.Hour), Md5: srcMD5})
	a.Nil(compareErr)

	// check the source object was indeed scheduled
	a.Equal(1, len(dummyCopyScheduler.record))
	a.Equal(srcMD5, dummyCopyScheduler.record[0].Md5)
	a.Zero(len(indexer.IndexMap))

	// reset the processor so that it's empty
	dummyCopyScheduler = dummyProcessor{}

	// test the comparator in case a given source object is present at the destination
	// but is has an earlier modified time compared to the one at the destination
	// meaning that the source object is considered stale, so no transfer should be scheduled
	err = indexer.Store(sampleDestinationObject)
	a.Nil(err)
	compareErr = sourceComparator.processIfNecessary(traverser.StoredObject{Name: "test", RelativePath: "/usr/test", LastModifiedTime: time.Now().Add(-time.Hour), Md5: srcMD5})
	a.Nil(compareErr)

	// check no source object was scheduled
	a.Zero(len(dummyCopyScheduler.record))
	a.Zero(len(indexer.IndexMap))
}

func TestSyncSrcCompDisableComparator(t *testing.T) {
	a := assert.New(t)
	dummyCopyScheduler := dummyProcessor{}
	srcMD5 := []byte{'s'}
	destMD5 := []byte{'d'}

	// set up the indexer as well as the source comparator
	indexer := traverser.NewObjectIndexer()
	sourceComparator := newSyncSourceComparator(indexer, dummyCopyScheduler.process, common.ESyncHashType.None(), false, true)

	// test the comparator in case a given source object is not present at the destination
	// meaning no entry in the index, so the comparator should pass the given object to schedule a transfer
	compareErr := sourceComparator.processIfNecessary(traverser.StoredObject{Name: "only_at_source", RelativePath: "only_at_source", LastModifiedTime: time.Now(), Md5: srcMD5})
	a.Nil(compareErr)

	// check the source object was indeed scheduled
	a.Equal(1, len(dummyCopyScheduler.record))
	a.Equal(srcMD5, dummyCopyScheduler.record[0].Md5)

	// reset the processor so that it's empty
	dummyCopyScheduler = dummyProcessor{}

	// create a sample source object
	currTime := time.Now()
	destinationStoredObjects := []traverser.StoredObject{
		// file whose last modified time is greater than that of source
		{Name: "test1", RelativePath: "/usr/test1", LastModifiedTime: currTime, Md5: destMD5},
		// file whose last modified time is less than that of source
		{Name: "test2", RelativePath: "/usr/test2", LastModifiedTime: currTime, Md5: destMD5},
	}

	sourceStoredObjects := []traverser.StoredObject{
		{Name: "test1", RelativePath: "/usr/test1", LastModifiedTime: currTime.Add(time.Hour), Md5: srcMD5},
		{Name: "test2", RelativePath: "/usr/test2", LastModifiedTime: currTime.Add(-time.Hour), Md5: srcMD5},
	}

	// test the comparator in case a given source object is present at the destination
	// but is has an earlier modified time compared to the one at the destination
	// meaning that the source object is considered stale, so no transfer should be scheduled
	for key, dstStoredObject := range destinationStoredObjects {
		err := indexer.Store(dstStoredObject)
		a.Nil(err)
		compareErr = sourceComparator.processIfNecessary(sourceStoredObjects[key])
		a.Nil(compareErr)
		a.Equal(key+1, len(dummyCopyScheduler.record))
		a.Zero(len(indexer.IndexMap))
	}
}

func TestSyncDestinationComparator(t *testing.T) {
	a := assert.New(t)
	dummyCopyScheduler := dummyProcessor{}
	dummyCleaner := dummyProcessor{}
	srcMD5 := []byte{'s'}
	destMD5 := []byte{'d'}

	// set up the indexer as well as the destination comparator
	indexer := traverser.NewObjectIndexer()
	destinationComparator := newSyncDestinationComparator(indexer, dummyCopyScheduler.process, dummyCleaner.process, common.ESyncHashType.None(), false, false)

	// create a sample source object
	sampleSourceObject := traverser.StoredObject{Name: "test", RelativePath: "/usr/test", LastModifiedTime: time.Now(), Md5: srcMD5}

	// test the comparator in case a given destination object is not present at the source
	// meaning it is an extra file that needs to be deleted, so the comparator should pass the given object to the destinationCleaner
	compareErr := destinationComparator.processIfNecessary(traverser.StoredObject{Name: "only_at_dst", RelativePath: "only_at_dst", LastModifiedTime: time.Now(), Md5: destMD5})
	a.Nil(compareErr)

	// verify that destination object is being deleted
	a.Zero(len(dummyCopyScheduler.record))
	a.Equal(1, len(dummyCleaner.record))
	a.Equal(destMD5, dummyCleaner.record[0].Md5)

	// reset dummy processors
	dummyCopyScheduler = dummyProcessor{}
	dummyCleaner = dummyProcessor{}

	// test the comparator in case a given destination object is present at the source
	// and it has a later modified time, since the source data is stale,
	// no transfer happens
	err := indexer.Store(sampleSourceObject)
	a.Nil(err)
	compareErr = destinationComparator.processIfNecessary(traverser.StoredObject{Name: "test", RelativePath: "/usr/test", LastModifiedTime: time.Now().Add(time.Hour), Md5: destMD5})
	a.Nil(compareErr)

	// verify that the source object is scheduled for transfer
	a.Zero(len(dummyCopyScheduler.record))
	a.Zero(len(dummyCleaner.record))

	// reset dummy processors
	dummyCopyScheduler = dummyProcessor{}
	dummyCleaner = dummyProcessor{}

	// test the comparator in case a given destination object is present at the source
	// but is has an earlier modified time compared to the one at the source
	// meaning that the source object should be transferred since the destination object is stale
	err = indexer.Store(sampleSourceObject)
	a.Nil(err)
	compareErr = destinationComparator.processIfNecessary(traverser.StoredObject{Name: "test", RelativePath: "/usr/test", LastModifiedTime: time.Now().Add(-time.Hour), Md5: destMD5})
	a.Nil(compareErr)

	// verify that there's no transfer & no deletes
	a.Equal(1, len(dummyCopyScheduler.record))
	a.Equal(srcMD5, dummyCopyScheduler.record[0].Md5)
	a.Zero(len(dummyCleaner.record))
}

func TestSyncDestCompDisableComparison(t *testing.T) {
	a := assert.New(t)
	dummyCopyScheduler := dummyProcessor{}
	dummyCleaner := dummyProcessor{}
	srcMD5 := []byte{'s'}
	destMD5 := []byte{'d'}

	// set up the indexer as well as the destination comparator
	indexer := traverser.NewObjectIndexer()
	destinationComparator := newSyncDestinationComparator(indexer, dummyCopyScheduler.process, dummyCleaner.process, common.ESyncHashType.None(), false, true)

	// create a sample source object
	currTime := time.Now()
	sourceStoredObjects := []traverser.StoredObject{
		{Name: "test1", RelativePath: "/usr/test1", LastModifiedTime: currTime, Md5: srcMD5},
		{Name: "test2", RelativePath: "/usr/test2", LastModifiedTime: currTime, Md5: srcMD5},
	}

	// onlyAtSrc := StoredObject{Name: "only_at_src", RelativePath: "/usr/only_at_src", LastModifiedTime: currTime, Md5: destMD5}

	destinationStoredObjects := []traverser.StoredObject{
		// file whose last modified time is greater than that of source
		{Name: "test1", RelativePath: "/usr/test1", LastModifiedTime: time.Now().Add(time.Hour), Md5: destMD5},
		// file whose last modified time is less than that of source
		{Name: "test2", RelativePath: "/usr/test2", LastModifiedTime: time.Now().Add(-time.Hour), Md5: destMD5},
	}

	// test the comparator in case a given destination object is not present at the source
	// meaning it is an extra file that needs to be deleted, so the comparator should pass the given object to the destinationCleaner
	compareErr := destinationComparator.processIfNecessary(traverser.StoredObject{Name: "only_at_dst", RelativePath: "only_at_dst", LastModifiedTime: currTime, Md5: destMD5})
	a.Nil(compareErr)

	// verify that destination object is being deleted
	a.Zero(len(dummyCopyScheduler.record))
	a.Equal(1, len(dummyCleaner.record))
	a.Equal(destMD5, dummyCleaner.record[0].Md5)

	// reset dummy processors
	dummyCopyScheduler = dummyProcessor{}
	dummyCleaner = dummyProcessor{}

	// test the comparator in case a given destination object is present at the source
	// and it has a later modified time, since the source data is stale,
	// no transfer happens
	for key, srcStoredObject := range sourceStoredObjects {
		err := indexer.Store(srcStoredObject)
		a.Nil(err)
		compareErr = destinationComparator.processIfNecessary(destinationStoredObjects[key])
		a.Nil(compareErr)
		a.Equal(key+1, len(dummyCopyScheduler.record))
	}
}
