// Copyright © Microsoft <wastore@microsoft.com>
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

package parallel

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type crawler struct {
	output      chan CrawlResult
	workerBody  EnumerateOneDirFunc
	parallelism int
	cond        *sync.Cond
	// the following are protected by cond (and must only be accessed when cond.L is held)
	unstartedDirs      []Directory // not a channel, because channels have length limits, and those get in our way
	dirInProgressCount int64
	lastAutoShutdown   time.Time
	root               Directory

	// Fields applicable only to sync operation.
	isSync                  bool
	getObjectIndexerMapSize func() int64

	// Its communication channel not exactly queue, as soon as we get something on this channel we pop it and append to
	// unstartedDirs (that's target queue).
	tqueue chan interface{}

	isSource                 bool
	maxObjectIndexerSizeInGB uint32
	mayHaveMoreDirs          bool
}

type Directory interface{}
type DirectoryEntry interface{}

type CrawlResult struct {
	item DirectoryEntry
	err  error
	//
	// This is special case where an 'item' (which is a directory in this case), needs to be added to tqueue.
	// This is applicable only in case of sync and source traverser. This special CrawlResult will be generated
	// by the source traverser, after it completes enumeration of a directory.
	// Reason for this adding here is, source traverser should tell target traverser about directory only when
	// it completes enumeration and stores it's entries in ObjectIndexerMap.
	//
	enqueueToTqueue bool
}

func (r CrawlResult) EnqueueToTqueue() bool {
	return r.enqueueToTqueue
}

func (r CrawlResult) Item() (interface{}, error) {
	return r.item, r.err
}

// must be safe to be simultaneously called by multiple go-routines, each with a different dir
type EnumerateOneDirFunc func(dir Directory, enqueueDir func(Directory), enqueueOutput func(DirectoryEntry, error)) error

// Crawl crawls an abstract directory tree, using the supplied enumeration function.  May be use for whatever
// that function can enumerate (i.e. not necessarily a local file system, just anything tree-structured)
// getObjectIndexerMapSize func returns in-memory map size, tqueue is channel between source and target enumeration.
// isSource tells whether its source or target traverser.
// isSync flag tells whether its sync or copy operation.
// maxObjectIndexerSizeInGB is configurable value tells how much maximum memory ObjectIndexerMap can occupy.
// We choose tqueue large enough to not become the bottleneck and hence maxObjectIndexerSizeInGB should be the only
// one controlling the source traverser speed.
func Crawl(ctx context.Context, root Directory, worker EnumerateOneDirFunc, parallelism int, getObjectIndexerMapSize func() int64, tqueue chan interface{}, isSource bool, isSync bool, maxObjectIndexerSizeInGB uint32) <-chan CrawlResult {
	c := &crawler{
		unstartedDirs: make([]Directory, 0, 1024),
		output:        make(chan CrawlResult, 1000),
		workerBody:    worker,
		parallelism:   parallelism,
		cond:          sync.NewCond(&sync.Mutex{}),
		root:          root,

		// Sync related parameters.
		isSync:                   isSync,
		getObjectIndexerMapSize:  getObjectIndexerMapSize,
		tqueue:                   tqueue,
		isSource:                 isSource,
		maxObjectIndexerSizeInGB: maxObjectIndexerSizeInGB,
	}

	if isSync {
		if tqueue == nil {
			// Both source and target traversers need tqueue, source traverser writes to it and target traverser reads from it.
			panic("Source/Destination traverser has nil tqueue!")
		}
	}

	go c.start(ctx, root)
	return c.output
}

func (c *crawler) start(ctx context.Context, root Directory) {
	done := make(chan struct{})
	heartbeat := func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(10 * time.Second):
				c.cond.Broadcast() // prevent things waiting for ever, even after cancellation has happened
			}
		}
	}
	go heartbeat()

	if c.isSync && !c.isSource {
		// Target traverser follow source traverser, so it needs to wait for directories completed by source traverser.
		// Target traverse can't come out until source traverser will tell no more entries left. It tells by closing the source and
		// target communication channel tqueue.
		c.mayHaveMoreDirs = true

		// Go routine receives dir entries on tqueue, which till now processed by source traverser.
		go c.readTqueue()
	} else {
		c.unstartedDirs = append(c.unstartedDirs, root)
	}

	c.runWorkersToCompletion(ctx)

	close(c.output)
	close(done)
}

func (c *crawler) runWorkersToCompletion(ctx context.Context) {
	wg := &sync.WaitGroup{}
	for i := 0; i < c.parallelism; i++ {
		wg.Add(1)
		go c.workerLoop(ctx, wg, i)
	}
	wg.Wait()
}

func (c *crawler) workerLoop(ctx context.Context, wg *sync.WaitGroup, workerIndex int) {
	defer wg.Done()

	var err error
	mayHaveMore := true
	for mayHaveMore && ctx.Err() == nil {
		mayHaveMore, err = c.processOneDirectoryWithAutoPacer(ctx, workerIndex)
		if err != nil {
			c.output <- CrawlResult{err: err}
			// output the error, but we don't necessarily stop the enumeration (e.g. it might be one unreadable dir)
		}
	}
}

const maxQueueDirectories = 1000 * 1000

//
// readTqueue() dequeues directory names added by the source traverser and appends them to unstartedDirs.
// In order to keep the memory requirement in check for the cases when source traverser is going very fast
// and maxObjectIndexerSizeInGB is not able to clamp it (because there are lot of small directories), we
// put a limit of 1 Million outstanding directories. After that readTqueue() will not read any more
// directories from tqueue which will put a back pressure on the source traverser.
//
func (c *crawler) readTqueue() {

	for tDir := range c.tqueue {
		c.cond.L.Lock()
		//
		// Lets put backpressure on source to slow down, otherwise c.unstartedDirs will keep growing and and cause memory pressure on system.
		// Once unstartedDirs crosses the limit we let it drain to 80% of the limit to avoid pointelessely sleeping on every other iteration.
		// 20% of maxQueueDirectories is 200K directories. Even the fastest target will take multiple seconds to process these many directories,
		// so we can safely add a largish sleep.
		//
		if len(c.unstartedDirs) > maxQueueDirectories {
			for len(c.unstartedDirs) > maxQueueDirectories*0.8 {
				fmt.Printf("Number of directories in unstartedDirs queue reached high water mark (%v): %v", maxQueueDirectories*0.8, len(c.unstartedDirs))
				c.cond.L.Unlock()
				time.Sleep(1000 * time.Millisecond)
				c.cond.L.Lock()
			}
		}
		if tDir != nil {
			c.unstartedDirs = append(c.unstartedDirs, tDir)
		}
		c.cond.L.Unlock()
		c.cond.Broadcast()
	}

	//
	// Let processOneDirectoryWithAutoPacer() (called by target traverser) know that we have flushed tqueue and no new
	// directories are expected. Once it processes everything queued to unstartedDirs it may plan to exit.
	//
	c.cond.L.Lock()
	c.mayHaveMoreDirs = false
	c.cond.L.Unlock()
	c.cond.Broadcast()
}

// autoPacerWait do the job of pacemaker between source and target traverser.
func (c *crawler) autoPacerWait(ctx context.Context) {
	const bytesInGB = 1024 * 1024 * 1024
	//
	// Consider 80% full as low-water-mark. Anything less than 80% full is fair game and we let the source traverser
	// run full throttle. Once it exceeds low-water-mark we slowly start taking the foot off the pedal and every thread/goroutine
	// needs to wait for a small time before proceeding. Once it touches/crosses high-water-mark no scanner thread/goroutine will
	// be allowed to proceed. For reasonable sized directories this should ensure that we remain in MaxObjectIndexerSizeInBytes limit
	// for most practical scenarios. For large/huge directories we might exceed MaxObjectIndexerSizeInBytes by the size of the largest directory.
	//
	MaxObjectIndexerSizeInBytes := int64(c.maxObjectIndexerSizeInGB) * int64(bytesInGB)

	lowWaterMark := (MaxObjectIndexerSizeInBytes * 8) / 10

	highWaterMark := MaxObjectIndexerSizeInBytes

	mapSizeInBytes := c.getObjectIndexerMapSize()

	// Nice sunny morning, press that pedal more.
	if mapSizeInBytes < lowWaterMark {
		return
	}

	//
	// We are nearing danger zone, start slowing down.
	//
	// TODO: The sleep duration may be decided based on how fast the
	//       objectIndexerMap is seen to be growing.
	//
	if mapSizeInBytes < highWaterMark && ctx.Err() == nil {
		// TODO: As of now crawler dont have knowledge about logger, so that it can log some metrics.
		// We need to add support of logger in crawler.
		fmt.Printf("\n ObjectIndexerMapSize[%v] is between lowerWaterMark[%v] and highWaterMark[%v]", mapSizeInBytes, lowWaterMark, highWaterMark)

		time.Sleep(1 * time.Second)
		return
	}

	// In danger zone. Don't proceed any further without dropping the objectIndexerMap size below lowWaterMark.
	for mapSizeInBytes > lowWaterMark && ctx.Err() == nil {
		fmt.Printf("\n [DANGER] ObjectIndexerMapSize[%v] lowerWaterMark[%v] and highWaterMark[%v]", mapSizeInBytes, lowWaterMark, highWaterMark)
		time.Sleep(1 * time.Second)
		mapSizeInBytes = c.getObjectIndexerMapSize()
	}

	return
}

func (c *crawler) processOneDirectoryWithAutoPacer(ctx context.Context, workerIndex int) (bool, error) {
	const maxQueueDirsForBreadthFirst = 100 * 1000 // figure is somewhat arbitrary.  Want it big, but not huge

	var toExamine Directory
	stop := false

	// Before picking a new directory to enumerate, call autoPacerWait() to check if we need to
	// slow down as the objectIndexer might be getting "too full". If Target Traverser is running
	// slow causing objectIndexer map to get full, we induce wait to let Target Traverser catch up.
	// Only source traverser of the sync process should call auto-pacer to slow itself down.
	if c.isSync && c.isSource {
		c.autoPacerWait(ctx)
	}

	// Acquire a directory to work on
	// Note that we need explicit locking because there are two
	// mutable things involved in our decision making, not one. (The two being c.unstartedDirs and c.dirInProgressCount)
	c.cond.L.Lock()
	{
		if c.isSync && !c.isSource {

			//
			// This is the target traverser of the sync process. It is handled separately since it depends on source
			// traverser to provide it the directories to process. Others do the normal breadth-first enumeration by
			// processing directories they have discovered. So, for the target traverser case, we have the following wait criteria:
			// 1. unstartedDirs is empty. This is where it will get the next directory to process.
			// 2. mayHaveMoreDirs is true, which means we are not yet done reading from tqueue and hence more new directories may be added by source traverser.
			//
			for len(c.unstartedDirs) == 0 && c.mayHaveMoreDirs && ctx.Err() == nil {
				c.cond.Wait()
			}
		} else {
			// wait while there's nothing to do, and another thread might be going to add something.
			for len(c.unstartedDirs) == 0 && c.dirInProgressCount > 0 && ctx.Err() == nil {
				c.cond.Wait()
			}
		}

		// if we have something to do now, grab it. Else we must be all finished with nothing more to do (ever)
		stop = ctx.Err() != nil
		if !stop {
			if len(c.unstartedDirs) > 0 {
				if len(c.unstartedDirs) < maxQueueDirsForBreadthFirst {
					// pop from start of list. This gives a breadth-first flavour to the search.
					// (Breadth-first is useful for distributing small-file workloads over the full keyspace, which
					// is can help performance when uploading small files to Azure Blob Storage)
					toExamine = c.unstartedDirs[0]
					c.unstartedDirs = c.unstartedDirs[1:]
				} else {
					// Fall back to popping from end of list if list is already pretty big.
					// This gives more of a depth-first flavour to our processing,
					// which (we think) will prevent c.unstartedDirs getting really large and using too much RAM.
					// (Since we think that depth first tends to hit leaf nodes relatively quickly, so total number of
					// unstarted dirs should tend to grow less in a depth first mode)
					lastIndex := len(c.unstartedDirs) - 1
					toExamine = c.unstartedDirs[lastIndex]
					c.unstartedDirs = c.unstartedDirs[:lastIndex]
				}

				c.dirInProgressCount++ // record that we are working on something
				c.cond.Broadcast()     // and let other threads know of that fact
			} else {
				if c.dirInProgressCount > 0 {
					// something has gone wrong in the design of this algorithm, because we should only get here if all done now
					panic("assertion failure: should be no more dirs in progress here")
				}
				stop = true
			}
		}
	}
	c.cond.L.Unlock()
	if stop {
		return false, nil
	}

	// find dir's immediate children (outside the lock, because this could be slow)
	var foundDirectories = make([]Directory, 0, 16)
	addDir := func(d Directory) {
		foundDirectories = append(foundDirectories, d)
	}

	addOutput := func(de DirectoryEntry, er error) {
		select {
		case c.output <- CrawlResult{item: de, err: er}:
		case <-ctx.Done(): // don't block on full channel if cancelled
		}
	}

	bodyErr := c.workerBody(toExamine, addDir, addOutput) // this is the worker body supplied by our caller

	// finally, update shared state (inside the lock)
	c.cond.L.Lock()
	defer c.cond.L.Unlock()

	//
	// Source traverser MUST add all completed directories to tqueue, for target traverser to process.
	// Note that c.workerBody() above would have completed enumeration of 'toExamine' dir and hence this is
	// the right time to add it to tqueue.
	//
	if c.isSync && c.isSource {
		if _, ok := toExamine.(string); ok {
			if _, ok := c.root.(string); ok {
				//
				// Add a special CrawlResult telling caller to enqueue this directory to tqueue for processing by the target traverser.
				// Note that we don't enqueue it here but instead ask the caller to enqueue, to ensure that we add a directory to tqueue
				// only after all the directory children have been added to the objectIndexer map.
				//
				c.output <- CrawlResult{
					item:            common.RelativePath(toExamine.(string), c.root.(string)),
					enqueueToTqueue: true,
				}
			}
		} else {
			panic("toExamine not string type")
		}

		c.unstartedDirs = append(c.unstartedDirs, foundDirectories...)
	} else if !c.isSync {
		// This is the regular copy case.
		c.unstartedDirs = append(c.unstartedDirs, foundDirectories...)
	}

	c.dirInProgressCount-- // we were doing something, and now we have finished it
	c.cond.Broadcast()     // let other workers know that the state has changed

	// If our queue of unstarted stuff is getting really huge,
	// reduce our parallelism in the hope of preventing further excessive RAM growth.
	// (It's impossible to know exactly what to do here, because we don't know whether more workers would _clear_
	// the queue more quickly; or _add to_ the queue more quickly.  It depends on whether the directories we process
	// next contain mostly child directories or if they are "leaf" directories containing mostly just files.  But,
	// if we slowly reduce parallelism the end state is closer to a single-threaded depth-first traversal, which
	// is generally fine in terms of memory usage on most folder structures)

	shouldShutSelfDown := len(c.unstartedDirs) > maxQueueDirectories && // we are getting way too much stuff queued up
		workerIndex > (c.parallelism/4) && // never shut down the last ones, since we need something left to clear the queue
		time.Since(c.lastAutoShutdown) > time.Second // adjust somewhat gradually
	if shouldShutSelfDown {
		c.lastAutoShutdown = time.Now()
		return false, bodyErr
	}

	return true, bodyErr // true because, as far as we know, the work is not finished. And err because it was the err (if any) from THIS dir
}
