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
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
)

type fakeEntryTraverser struct {
	traverseErr error
	empty       bool
}

func (f fakeEntryTraverser) IsDirectory(bool) (bool, error) { return false, nil }
func (f fakeEntryTraverser) Traverse(_ objectMorpher, processor objectProcessor, _ []ObjectFilter) error {
	if f.traverseErr != nil {
		return f.traverseErr
	}
	if f.empty {
		return nil
	}
	return processor(StoredObject{name: "obj"})
}

// captureListEntryWarnings records listEntryWarn messages for the test.
func captureListEntryWarnings(t *testing.T) *[]string {
	var got []string
	orig := listEntryWarn
	listEntryWarn = func(msg string) { got = append(got, msg) }
	t.Cleanup(func() { listEntryWarn = orig })
	return &got
}

func newTestListTraverser(entries []string, gen childTraverserGenerator, errCh chan<- TraverserErrorItemInfo) *listTraverser {
	list := make(chan string, len(entries))
	for _, e := range entries {
		list <- e
	}
	close(list)
	return &listTraverser{
		listReader:              list,
		childTraverserGenerator: gen,
		entryErrorChannel:       errCh,
		location:                common.ELocation.Blob(),
		ctx:                     context.Background(),
	}
}

func TestListTraverserReportsEntryErrors(t *testing.T) {
	a := assert.New(t)
	lookupErr := errors.New("403 AuthorizationPermissionMismatch")
	genErr := errors.New("bad entry")
	gen := func(childPath string) (ResourceTraverser, error) {
		switch childPath {
		case "denied":
			return fakeEntryTraverser{traverseErr: lookupErr}, nil
		case "unparsable":
			return nil, genErr
		default:
			return fakeEntryTraverser{}, nil
		}
	}

	errCh := make(chan TraverserErrorItemInfo, 10)
	l := newTestListTraverser([]string{"ok", "denied", "unparsable"}, gen, errCh)
	a.NoError(l.Traverse(nil, func(StoredObject) error { return nil }, nil))
	close(errCh)

	got := map[string]error{}
	for item := range errCh {
		entry, ok := item.(ListEntryErrorInfo)
		if !a.True(ok, "got %T, want ListEntryErrorInfo", item) {
			continue
		}
		got[entry.FullPath()] = entry.ErrorMessage()
	}
	a.Len(got, 2)
	a.ErrorIs(got["denied"], lookupErr)
	a.ErrorIs(got["unparsable"], genErr)
}

func TestListTraverserSkipsEntryErrorsWithoutChannel(t *testing.T) {
	a := assert.New(t)
	gen := func(string) (ResourceTraverser, error) {
		return fakeEntryTraverser{traverseErr: errors.New("boom")}, nil
	}
	l := newTestListTraverser([]string{"a"}, gen, nil)
	a.NoError(l.Traverse(nil, func(StoredObject) error { return nil }, nil))
}

func TestListTraverserStopsOnCancel(t *testing.T) {
	a := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	var visited []string
	gen := func(childPath string) (ResourceTraverser, error) {
		visited = append(visited, childPath)
		if childPath == "b" {
			cancel()
			return fakeEntryTraverser{traverseErr: context.Canceled}, nil
		}
		return fakeEntryTraverser{}, nil
	}

	// Unbuffered: a report attempted after cancel would block forever.
	errCh := make(chan TraverserErrorItemInfo)
	l := newTestListTraverser([]string{"a", "b", "c", "d"}, gen, errCh)
	l.ctx = ctx

	a.ErrorIs(l.Traverse(nil, func(StoredObject) error { return nil }, nil), context.Canceled)
	a.Equal([]string{"a", "b"}, visited, "entries visited after cancel")
}

func TestListTraverserReturnsWhenCanceledBeforeStart(t *testing.T) {
	a := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	gen := func(string) (ResourceTraverser, error) {
		a.Fail("entry enumerated after cancel")
		return nil, errors.New("entry enumerated after cancel")
	}
	l := newTestListTraverser([]string{"a"}, gen, nil)
	l.ctx = ctx
	a.ErrorIs(l.Traverse(nil, func(StoredObject) error { return nil }, nil), context.Canceled)
}

func TestListTraverserLogsEntriesThatMatchNothing(t *testing.T) {
	a := assert.New(t)
	warnings := captureListEntryWarnings(t)
	gen := func(childPath string) (ResourceTraverser, error) {
		return fakeEntryTraverser{empty: childPath == "gone"}, nil
	}

	errCh := make(chan TraverserErrorItemInfo, 10)
	l := newTestListTraverser([]string{"ok", "gone"}, gen, errCh)
	a.NoError(l.Traverse(nil, func(StoredObject) error { return nil }, nil))
	if a.Len(*warnings, 2, "want one for gone and a total") {
		a.Contains((*warnings)[0], "gone")
		a.Contains((*warnings)[1], "1 list entries")
	}
	a.Empty(errCh, "an entry that matched nothing was reported as an error")

	// Without entry error reporting (plain azcopy), nothing is logged.
	*warnings = nil
	l = newTestListTraverser([]string{"gone"}, gen, nil)
	a.NoError(l.Traverse(nil, func(StoredObject) error { return nil }, nil))
	a.Empty(*warnings)
}
