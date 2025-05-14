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
	"sync"
)

type InputObject interface{}
type OutputObject interface{}

type transformer struct {
	input       <-chan CrawlResult // TODO: would have liked this to be of InputObject, but it made our usage messy.  Not sure of right solution to that yet. For now, using CrawlResult ties us to transforming result of crawl
	output      chan TransformResult
	workerBody  TransformFunc
	parallelism int
}

type TransformResult struct {
	item OutputObject
	err  error
}

func (r TransformResult) Item() (interface{}, error) {
	return r.item, r.err
}

// must be safe to be simultaneously called by multiple go-routines
type TransformFunc func(input InputObject) (OutputObject, error)

// transformation will stop when input is closed
func Transform(ctx context.Context, input <-chan CrawlResult, worker TransformFunc, parallelism int) <-chan TransformResult {
	t := &transformer{
		input:       input,
		output:      make(chan TransformResult, 1000),
		workerBody:  worker,
		parallelism: parallelism,
	}
	go t.runWorkersToCompletion(ctx)
	return t.output
}

func (t *transformer) runWorkersToCompletion(ctx context.Context) {
	wg := &sync.WaitGroup{}
	for i := 0; i < t.parallelism; i++ {
		wg.Add(1)
		go t.workerLoop(ctx, wg)
	}
	wg.Wait()
	close(t.output)
}

func (t *transformer) workerLoop(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	for t.processOneObject(ctx) {
	}
}

func (t *transformer) processOneObject(ctx context.Context) bool {

	select {
	case rawObject, ok := <-t.input:
		if ok {
			in, err := rawObject.Item() // unpack it
			if err != nil {
				t.output <- TransformResult{err: err} // propagate the error
				return true
			}
			out, err := t.workerBody(in)
			if out != nil { // In the case of an NFS symbolic link, we skip processing the file
				t.output <- TransformResult{item: out, err: err}
			}
		}
		return ok // exit this worker loop when input is closed and empty
	case <-ctx.Done():
		return false
	}
}
