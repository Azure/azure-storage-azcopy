package cmd

import (
	"context"
	"errors"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type fakeEntryTraverser struct {
	traverseErr error
}

func (f fakeEntryTraverser) IsDirectory(bool) (bool, error) { return false, nil }
func (f fakeEntryTraverser) Traverse(objectMorpher, objectProcessor, []ObjectFilter) error {
	return f.traverseErr
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
