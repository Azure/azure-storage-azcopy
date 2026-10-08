package cmd

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// fakeDirTraverser models a directory entry: Traverse yields the directory
// and its contents, TraverseSelf only the directory.
type fakeDirTraverser struct{}

func (fakeDirTraverser) IsDirectory(bool) (bool, error) { return true, nil }
func (fakeDirTraverser) Traverse(pre objectMorpher, processor objectProcessor, _ []ObjectFilter) error {
	for _, rel := range []string{"", "child.txt"} {
		obj := StoredObject{name: "x", relativePath: rel, entityType: common.EEntityType.Folder()}
		if rel != "" {
			obj.entityType = common.EEntityType.File()
		}
		if pre != nil {
			pre(&obj)
		}
		if err := processor(obj); err != nil {
			return err
		}
	}
	return nil
}
func (f fakeDirTraverser) TraverseSelf(pre objectMorpher, processor objectProcessor, _ []ObjectFilter) error {
	obj := StoredObject{name: "x", entityType: common.EEntityType.Folder()}
	if pre != nil {
		pre(&obj)
	}
	return processor(obj)
}

// fakeDirNoSelf is a directory on a source that cannot enumerate it alone.
type fakeDirNoSelf struct{ traversed *bool }

func (fakeDirNoSelf) IsDirectory(bool) (bool, error) { return true, nil }
func (f fakeDirNoSelf) Traverse(objectMorpher, objectProcessor, []ObjectFilter) error {
	*f.traversed = true
	return nil
}

// fakeFileTraverser is a file entry.
type fakeFileTraverser struct{}

func (fakeFileTraverser) IsDirectory(bool) (bool, error) { return false, nil }
func (fakeFileTraverser) Traverse(pre objectMorpher, processor objectProcessor, _ []ObjectFilter) error {
	obj := StoredObject{name: "f", entityType: common.EEntityType.File()}
	if pre != nil {
		pre(&obj)
	}
	return processor(obj)
}

// fakeEmptyDir is a directory that yields nothing on its own, like a
// flat-namespace stub when the transfer does not carry folders.
type fakeEmptyDir struct{ fakeDirTraverser }

func (fakeEmptyDir) TraverseSelf(objectMorpher, objectProcessor, []ObjectFilter) error { return nil }

func collectRelPaths(t *testing.T, l *listTraverser) []string {
	t.Helper()
	var got []string
	if err := l.Traverse(nil, func(o StoredObject) error {
		got = append(got, o.relativePath)
		return nil
	}, nil); err != nil {
		t.Fatalf("Traverse() = %v", err)
	}
	return got
}

func TestListTraverserDirectoriesSelfOnly(t *testing.T) {
	gen := func(childPath string) (ResourceTraverser, error) {
		if strings.HasPrefix(childPath, "dir") {
			return fakeDirTraverser{}, nil
		}
		return fakeFileTraverser{}, nil
	}

	expanded := newTestListTraverser([]string{"dir", "file.txt"}, gen, nil)
	expanded.recursive = true
	if got := collectRelPaths(t, expanded); strings.Join(got, ",") != "dir,dir/child.txt,file.txt" {
		t.Fatalf("default mode yielded %q, want the directory expanded", got)
	}

	selfOnly := newTestListTraverser([]string{"dir", "file.txt"}, gen, nil)
	selfOnly.recursive = true
	selfOnly.directoriesSelfOnly = true
	if got := collectRelPaths(t, selfOnly); strings.Join(got, ",") != "dir,file.txt" {
		t.Fatalf("self-only mode yielded %q, want the directory itself and the file", got)
	}
}

func TestListTraverserSelfOnlyReportsDirectoriesItCannotNarrow(t *testing.T) {
	traversed := false
	gen := func(string) (ResourceTraverser, error) { return fakeDirNoSelf{traversed: &traversed}, nil }

	errCh := make(chan TraverserErrorItemInfo, 1)
	l := newTestListTraverser([]string{"dir"}, gen, errCh)
	l.recursive = true
	l.directoriesSelfOnly = true
	if got := collectRelPaths(t, l); len(got) != 0 {
		t.Fatalf("yielded %q, want nothing", got)
	}
	if traversed {
		t.Fatal("directory was expanded")
	}
	close(errCh)
	item, ok := <-errCh
	if !ok || !errors.Is(item.ErrorMessage(), errDirectoryNotSelfTraversable) || item.FullPath() != "dir" {
		t.Fatalf("reported %v, want dir with errDirectoryNotSelfTraversable", item)
	}
}

func TestListTraverserSelfOnlyEmptyDirectoryIsNotWarned(t *testing.T) {
	warnings := captureListEntryWarnings(t)
	gen := func(string) (ResourceTraverser, error) { return fakeEmptyDir{}, nil }

	errCh := make(chan TraverserErrorItemInfo, 1)
	l := newTestListTraverser([]string{"stub"}, gen, errCh)
	l.recursive = true
	l.directoriesSelfOnly = true
	collectRelPaths(t, l)
	if len(*warnings) != 0 || len(errCh) != 0 {
		t.Fatalf("warnings=%q errors=%d, want neither for a directory that yields nothing by design", *warnings, len(errCh))
	}
}

func TestNewListTraverserDirectoriesSelfOnlyIsOptIn(t *testing.T) {
	list := make(chan string)
	close(list)
	src := common.ResourceString{Value: "https://a.blob.core.windows.net/c"}

	off := newListTraverser(src, common.ELocation.Blob(), context.Background(), InitResourceTraverserOptions{ListOfFiles: list}).(*listTraverser)
	on := newListTraverser(src, common.ELocation.Blob(), context.Background(),
		InitResourceTraverserOptions{ListOfFiles: list, ListOfFilesDirectoriesSelfOnly: true}).(*listTraverser)
	if off.directoriesSelfOnly || !on.directoriesSelfOnly {
		t.Fatalf("directoriesSelfOnly off=%v on=%v, want false/true", off.directoriesSelfOnly, on.directoriesSelfOnly)
	}
}

// hnsDirTransport serves a container holding an HNS-style directory "dir"
// (a blob with hdi_isfolder metadata) with one file under it, and records
// every request.
type hnsDirTransport struct {
	mu       sync.Mutex
	requests []string
}

func (tr *hnsDirTransport) Do(req *http.Request) (*http.Response, error) {
	tr.mu.Lock()
	tr.requests = append(tr.requests, req.Method+" "+req.URL.Path+"?"+req.URL.RawQuery)
	tr.mu.Unlock()

	resp := &http.Response{Request: req, StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(""))}
	if req.Method == http.MethodHead {
		resp.Header.Set("Last-Modified", "Wed, 07 Oct 2026 10:00:00 GMT")
		resp.Header.Set("Content-Length", "0")
		resp.Header.Set("x-ms-blob-type", "BlockBlob")
		switch strings.TrimPrefix(req.URL.Path, "/c/") {
		case "dir":
			resp.Header.Set("x-ms-meta-hdi_isfolder", "true")
		case "dir/a.txt", "file.txt":
		default:
			resp.StatusCode = http.StatusNotFound
			resp.Header.Set("x-ms-error-code", "BlobNotFound")
		}
		return resp, nil
	}
	resp.Header.Set("Content-Type", "application/xml")
	resp.Body = io.NopCloser(strings.NewReader(`<?xml version="1.0" encoding="utf-8"?>` +
		`<EnumerationResults ServiceEndpoint="https://acct.blob.core.windows.net/" ContainerName="c"><Prefix>dir/</Prefix><Delimiter>/</Delimiter><Blobs>` +
		`<Blob><Name>dir/a.txt</Name><Properties><Last-Modified>Wed, 07 Oct 2026 10:00:00 GMT</Last-Modified><Etag>0x1</Etag>` +
		`<Content-Length>5</Content-Length><BlobType>BlockBlob</BlobType></Properties><Metadata /></Blob>` +
		`</Blobs><NextMarker /></EnumerationResults>`))
	return resp, nil
}

func (tr *hnsDirTransport) listed() bool {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	for _, r := range tr.requests {
		if strings.Contains(r, "comp=list") {
			return true
		}
	}
	return false
}

// newHnsDirListTraverser is a self-only list traverser over real blob
// traversers on a fake service, configured as XDM configures an HNS source
// (recursive, directory stubs included).
func newHnsDirListTraverser(t *testing.T, entries []string, includeDirStubs, selfOnly bool) (*listTraverser, *hnsDirTransport) {
	t.Helper()
	tr := &hnsDirTransport{}
	sc, err := service.NewClientWithNoCredential("https://acct.blob.core.windows.net/", &service.ClientOptions{
		ClientOptions: azcore.ClientOptions{Transport: tr, Retry: policy.RetryOptions{MaxRetries: -1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	gen := func(childPath string) (ResourceTraverser, error) {
		return newBlobTraverser("https://acct.blob.core.windows.net/c/"+childPath, sc, context.Background(), InitResourceTraverserOptions{
			Recursive:             true,
			IncludeDirectoryStubs: includeDirStubs,
			IncrementEnumeration:  enumerationCounterFuncNoop,
		}), nil
	}
	l := newTestListTraverser(entries, gen, nil)
	l.recursive = true
	l.directoriesSelfOnly = selfOnly
	return l, tr
}

func collectObjects(t *testing.T, l *listTraverser) map[string]common.EntityType {
	t.Helper()
	got := map[string]common.EntityType{}
	if err := l.Traverse(nil, func(o StoredObject) error {
		got[o.relativePath] = o.entityType
		return nil
	}, nil); err != nil {
		t.Fatalf("Traverse() = %v", err)
	}
	return got
}

func TestBlobListEntryDirectorySelfOnly(t *testing.T) {
	// Baseline: without self-only, the directory entry expands to its file.
	l, tr := newHnsDirListTraverser(t, []string{"dir"}, true, false)
	if got := collectObjects(t, l); got["dir"] != common.EEntityType.Folder() || got["dir/a.txt"] != common.EEntityType.File() {
		t.Fatalf("default mode yielded %v, want dir (folder) and dir/a.txt", got)
	}
	if !tr.listed() {
		t.Fatal("default mode did not list the directory; the fake does not model expansion")
	}

	// Self-only: the directory as a folder, nothing under it, no listing.
	l, tr = newHnsDirListTraverser(t, []string{"dir", "file.txt"}, true, true)
	got := collectObjects(t, l)
	if len(got) != 2 || got["dir"] != common.EEntityType.Folder() || got["file.txt"] != common.EEntityType.File() {
		t.Fatalf("self-only mode yielded %v, want exactly dir (folder) and file.txt (file)", got)
	}
	if tr.listed() {
		t.Fatalf("self-only mode listed a directory: %q", tr.requests)
	}
}

func TestBlobListEntryDirectoryStubSelfOnlyWithoutFolders(t *testing.T) {
	// A transfer that does not carry folders (flat namespace to flat
	// namespace) yields nothing for a directory stub, and does not expand it.
	l, tr := newHnsDirListTraverser(t, []string{"dir"}, false, true)
	if got := collectObjects(t, l); len(got) != 0 {
		t.Fatalf("yielded %v, want nothing", got)
	}
	if tr.listed() {
		t.Fatalf("listed a directory: %q", tr.requests)
	}
}
