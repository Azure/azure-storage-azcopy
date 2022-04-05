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
)

type crawler struct {
	output      chan CrawlResult
	workerBody  EnumerateOneDirFunc
	parallelism int
	cond        *sync.Cond
	// the following are protected by cond (and must only be accessed when cond.L is held)
	unstartedDirs       []Directory // not a channel, because channels have length limits, and those get in our way
	dirInProgressCount  int64
	lastAutoShutdown    time.Time
	desiredNumOfthreads int
	currentNumOfthreads int
	desiredTuneCond     *sync.Cond
	desiredTuneCh       chan int
	done                bool
}

type Directory interface{}
type DirectoryEntry interface{}

type CrawlResult struct {
	item DirectoryEntry
	err  error
}

func (r CrawlResult) Item() (interface{}, error) {
	return r.item, r.err
}

// must be safe to be simultaneously called by multiple go-routines, each with a different dir
type EnumerateOneDirFunc func(dir Directory, enqueueDir func(Directory), enqueueOutput func(DirectoryEntry, error)) error

// Crawl crawls an abstract directory tree, using the supplied enumeration function.  May be use for whatever
// that function can enumerate (i.e. not necessarily a local file system, just anything tree-structured)
func Crawl(ctx context.Context, root Directory, worker EnumerateOneDirFunc, parallelism int, ch chan int) <-chan CrawlResult {
	c := &crawler{
		unstartedDirs:       make([]Directory, 0, 1024),
		output:              make(chan CrawlResult, 1000),
		workerBody:          worker,
		parallelism:         parallelism,
		desiredNumOfthreads: parallelism,
		currentNumOfthreads: 0,
		desiredTuneCh:       ch,
		cond:                sync.NewCond(&sync.Mutex{}),
		desiredTuneCond:     sync.NewCond(&sync.Mutex{}),
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
	if c.desiredTuneCh != nil {
		go c.adjustNumberofthreads()
	}

	go c.adjustNumberofthreads()

	c.unstartedDirs = append(c.unstartedDirs, root)
	c.runWorkersToCompletion(ctx)

	if c.desiredTuneCh != nil {
		close(c.desiredTuneCh)
	}
	c.done = true
	close(c.output)
	close(done)
}

func (c *crawler) runWorkersToCompletion(ctx context.Context) {

	for {
		c.desiredTuneCond.L.Lock()
		if c.desiredNumOfthreads == 0 {
			fmt.Printf("CurrentNumOfthreads: %d, desiredNumOfThreds: %d", c.currentNumOfthreads, c.desiredNumOfthreads)
			fmt.Printf("Return")
			c.desiredTuneCond.L.Unlock()
			return
		}

		if c.desiredNumOfthreads > c.currentNumOfthreads {
			fmt.Printf("Increasing currentNumOfthreads: %d, desiredNumOfThreds: %d", c.currentNumOfthreads, c.desiredNumOfthreads)
			go c.workerLoop(ctx, c.currentNumOfthreads)
			c.currentNumOfthreads++
		} else if c.desiredNumOfthreads < c.currentNumOfthreads {
			fmt.Printf("Wait for decrease currentNumOfthreads: %d, desiredNumOfThreds: %d", c.currentNumOfthreads, c.desiredNumOfthreads)
			if c.desiredNumOfthreads != c.currentNumOfthreads {
				c.desiredTuneCond.Wait()
			}

		} else {

			if c.desiredNumOfthreads == c.currentNumOfthreads && c.desiredNumOfthreads != 0 {
				c.desiredTuneCond.Wait()
			}

		}
		c.desiredTuneCond.L.Unlock()
	}
}

func (c *crawler) workerLoop(ctx context.Context, workerIndex int) {
	//defer wg.Done()

	var err error
	mayHaveMore := true
	nice := false

	for mayHaveMore && ctx.Err() == nil {
		mayHaveMore, err = c.processOneDirectory(ctx, workerIndex)
		if err != nil {
			c.output <- CrawlResult{err: err}
			// output the error, but we don't necessarily stop the enumeration (e.g. it might be one unreadable dir)
		}

		c.desiredTuneCond.L.Lock()
		{
			if c.desiredNumOfthreads < c.currentNumOfthreads {
				c.currentNumOfthreads--
				nice = true
				mayHaveMore = false
			}

			if !mayHaveMore && !nice {
				c.desiredNumOfthreads--
				c.currentNumOfthreads--
			}
			c.desiredTuneCond.Signal()
		}
		c.desiredTuneCond.L.Unlock()
	}
}

func (c *crawler) adjustNumberofthreads() {
	for {
		var desire int
		if c.done {
			return
		}

		if c.currentNumOfthreads > 5 {
			desire = -1
		} else {
			desire = 1
		}
		c.desiredTuneCond.L.Lock()
		c.desiredNumOfthreads += desire
		c.desiredTuneCond.Signal()
		c.desiredTuneCond.L.Unlock()
		time.Sleep(10 * time.Second)
	}
}

// func (c *crawler) adjustNumberofthreads() {
// 	for desire := range c.desiredTuneCh {
// 		c.desiredTuneCond.L.Lock()
// 		c.desiredNumOfthreads += desire
// 		c.desiredTuneCond.Signal()
// 		c.desiredTuneCond.L.Unlock()
// 	}
// }

func (c *crawler) processOneDirectory(ctx context.Context, workerIndex int) (bool, error) {
	const maxQueueDirectories = 1000 * 1000
	const maxQueueDirsForBreadthFirst = 100 * 1000 // figure is somewhat arbitrary.  Want it big, but not huge

	var toExamine Directory
	stop := false

	// Acquire a directory to work on
	// Note that we need explicit locking because there are two
	// mutable things involved in our decision making, not one. (The two being c.unstartedDirs and c.dirInProgressCount)
	c.cond.L.Lock()
	{
		// wait while there's nothing to do, and another thread might be going to add something
		for len(c.unstartedDirs) == 0 && c.dirInProgressCount > 0 && ctx.Err() == nil {
			c.cond.Wait() // temporarily relinquish the lock (just on this line only) while we wait for a Signal/Broadcast
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

	c.unstartedDirs = append(c.unstartedDirs, foundDirectories...) // do NOT try to wait here if unstartedDirs is getting big. May cause deadlocks, due to all workers waiting and none processing the queue
	c.dirInProgressCount--                                         // we were doing something, and now we have finished it
	c.cond.Broadcast()                                             // let other workers know that the state has changed

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
