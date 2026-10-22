package cmd

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
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
	if err := l.Traverse(nil, func(StoredObject) error { return nil }, nil); err != nil {
		t.Fatalf("Traverse() = %v", err)
	}
	close(errCh)

	got := map[string]error{}
	for item := range errCh {
		entry, ok := item.(ListEntryErrorInfo)
		if !ok {
			t.Fatalf("got %T, want ListEntryErrorInfo", item)
		}
		got[entry.FullPath()] = entry.ErrorMessage()
	}
	if len(got) != 2 || !errors.Is(got["denied"], lookupErr) || !errors.Is(got["unparsable"], genErr) {
		t.Fatalf("reported entries = %v, want denied and unparsable with their errors", got)
	}
}

func TestListTraverserSkipsEntryErrorsWithoutChannel(t *testing.T) {
	gen := func(string) (ResourceTraverser, error) {
		return fakeEntryTraverser{traverseErr: errors.New("boom")}, nil
	}
	l := newTestListTraverser([]string{"a"}, gen, nil)
	if err := l.Traverse(nil, func(StoredObject) error { return nil }, nil); err != nil {
		t.Fatalf("Traverse() = %v", err)
	}
}

func TestNewListTraverserReportingIsOptIn(t *testing.T) {
	errCh := make(chan TraverserErrorItemInfo, 1)
	list := make(chan string)
	close(list)

	off := newListTraverser(common.ResourceString{Value: "https://a.blob.core.windows.net/c"}, common.ELocation.Blob(), context.Background(),
		InitResourceTraverserOptions{ListOfFiles: list, ErrorChannel: errCh}).(*listTraverser)
	if off.entryErrorChannel != nil {
		t.Fatal("entry errors reported without ReportListOfFilesEntryErrors")
	}

	on := newListTraverser(common.ResourceString{Value: "https://a.blob.core.windows.net/c"}, common.ELocation.Blob(), context.Background(),
		InitResourceTraverserOptions{ListOfFiles: list, ErrorChannel: errCh, ReportListOfFilesEntryErrors: true}).(*listTraverser)
	if on.entryErrorChannel == nil {
		t.Fatal("entry errors not reported with ReportListOfFilesEntryErrors")
	}
}

func TestListTraverserStopsOnCancel(t *testing.T) {
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

	if err := l.Traverse(nil, func(StoredObject) error { return nil }, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("Traverse() = %v, want context.Canceled", err)
	}
	if len(visited) != 2 {
		t.Fatalf("visited %v after cancel, want [a b]", visited)
	}
}

func TestListTraverserReturnsWhenCanceledBeforeStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	gen := func(string) (ResourceTraverser, error) {
		t.Fatal("entry enumerated after cancel")
		return nil, nil
	}
	l := newTestListTraverser([]string{"a"}, gen, nil)
	l.ctx = ctx
	if err := l.Traverse(nil, func(StoredObject) error { return nil }, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("Traverse() = %v, want context.Canceled", err)
	}
}

func TestListTraverserLogsEntriesThatMatchNothing(t *testing.T) {
	warnings := captureListEntryWarnings(t)
	gen := func(childPath string) (ResourceTraverser, error) {
		return fakeEntryTraverser{empty: childPath == "gone"}, nil
	}

	errCh := make(chan TraverserErrorItemInfo, 10)
	l := newTestListTraverser([]string{"ok", "gone"}, gen, errCh)
	if err := l.Traverse(nil, func(StoredObject) error { return nil }, nil); err != nil {
		t.Fatalf("Traverse() = %v", err)
	}
	if len(*warnings) != 2 || !strings.Contains((*warnings)[0], "gone") || !strings.Contains((*warnings)[1], "1 list entries") {
		t.Fatalf("warnings = %q, want one for gone and a total of 1", *warnings)
	}
	if len(errCh) != 0 {
		t.Fatalf("an entry that matched nothing was reported as an error")
	}

	// Without entry error reporting (plain azcopy), nothing is logged.
	*warnings = nil
	l = newTestListTraverser([]string{"gone"}, gen, nil)
	if err := l.Traverse(nil, func(StoredObject) error { return nil }, nil); err != nil {
		t.Fatalf("Traverse() = %v", err)
	}
	if len(*warnings) != 0 {
		t.Fatalf("warnings = %q without reporting", *warnings)
	}
}
