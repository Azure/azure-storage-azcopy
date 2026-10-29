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

package cmd

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// slowEntryTraverser yields one object per entry after a delay, tracking how
// many entries are in flight at once.
type slowEntryTraverser struct {
	name     string
	delay    time.Duration
	inFlight *int64
	maxSeen  *int64
	err      error
	empty    bool
}

func (s slowEntryTraverser) IsDirectory(bool) (bool, error) { return false, nil }
func (s slowEntryTraverser) Traverse(_ objectMorpher, processor objectProcessor, _ []ObjectFilter) error {
	n := atomic.AddInt64(s.inFlight, 1)
	defer atomic.AddInt64(s.inFlight, -1)
	for {
		m := atomic.LoadInt64(s.maxSeen)
		if n <= m || atomic.CompareAndSwapInt64(s.maxSeen, m, n) {
			break
		}
	}
	time.Sleep(s.delay)
	if s.err != nil {
		return s.err
	}
	if s.empty {
		return nil
	}
	return processor(StoredObject{name: s.name})
}

func TestListTraverserParallel(t *testing.T) {
	a := assert.New(t)
	const entries = 200
	var names []string
	for i := 0; i < entries; i++ {
		names = append(names, fmt.Sprintf("f%03d", i))
	}
	files := append([]string(nil), names...)
	names = append(names, "denied", "gone")

	var inFlight, maxSeen int64
	lookupErr := errors.New("403 AuthorizationPermissionMismatch")
	gen := func(childPath string) (ResourceTraverser, error) {
		return slowEntryTraverser{name: childPath, delay: 5 * time.Millisecond, inFlight: &inFlight, maxSeen: &maxSeen,
			err: map[bool]error{true: lookupErr}[childPath == "denied"], empty: childPath == "gone"}, nil
	}
	warnings := captureListEntryWarnings(t)
	var warnMu sync.Mutex
	orig := listEntryWarn
	listEntryWarn = func(msg string) { warnMu.Lock(); defer warnMu.Unlock(); orig(msg) }

	errCh := make(chan TraverserErrorItemInfo, 10)
	l := newTestListTraverser(names, gen, errCh)
	l.parallelism = 16

	// The processor is not synchronized: the traverser must serialize it.
	var got []string
	var inProcessor int32
	err := l.Traverse(nil, func(o StoredObject) error {
		if atomic.AddInt32(&inProcessor, 1) != 1 {
			a.Fail("processor called concurrently")
		}
		got = append(got, o.name)
		atomic.AddInt32(&inProcessor, -1)
		return nil
	}, nil)
	a.NoError(err)
	close(errCh)

	sort.Strings(got)
	a.Equal(files, got, "want every listed file once")
	a.GreaterOrEqual(maxSeen, int64(2), "max entries in flight")
	a.LessOrEqual(maxSeen, int64(16), "max entries in flight")

	var reported []string
	for item := range errCh {
		reported = append(reported, item.FullPath())
		a.ErrorIs(item.ErrorMessage(), lookupErr)
	}
	a.Equal([]string{"denied"}, reported)
	a.Len(*warnings, 2, "want one for gone and a total")
}

func TestListTraverserParallelStopsOnCancel(t *testing.T) {
	a := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	// genMu makes the 20th entry's count and cancel one step, so each other
	// worker can start at most one entry after the cancel.
	var genMu sync.Mutex
	var visited int64
	gen := func(childPath string) (ResourceTraverser, error) {
		genMu.Lock()
		defer genMu.Unlock()
		if atomic.AddInt64(&visited, 1) == 20 {
			cancel()
			return fakeEntryTraverser{traverseErr: context.Canceled}, nil
		}
		return fakeEntryTraverser{}, nil
	}
	var names []string
	for i := 0; i < 1000; i++ {
		names = append(names, fmt.Sprintf("f%04d", i))
	}
	// Unbuffered: a report attempted after cancel would block forever.
	errCh := make(chan TraverserErrorItemInfo)
	l := newTestListTraverser(names, gen, errCh)
	l.ctx = ctx
	l.parallelism = 8

	var mu sync.Mutex
	a.ErrorIs(l.Traverse(nil, func(StoredObject) error { mu.Lock(); defer mu.Unlock(); return nil }, nil), context.Canceled)
	a.LessOrEqual(atomic.LoadInt64(&visited), int64(20+8), "want the traversal to stop soon after the cancel")
}
