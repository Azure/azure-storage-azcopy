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
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
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

func collectRelPaths(a *assert.Assertions, l *listTraverser) []string {
	var got []string
	a.NoError(l.Traverse(nil, func(o StoredObject) error {
		got = append(got, o.relativePath)
		return nil
	}, nil))
	return got
}

func TestListTraverserDirectoriesSelfOnly(t *testing.T) {
	a := assert.New(t)
	gen := func(childPath string) (ResourceTraverser, error) {
		if strings.HasPrefix(childPath, "dir") {
			return fakeDirTraverser{}, nil
		}
		return fakeFileTraverser{}, nil
	}

	expanded := newTestListTraverser([]string{"dir", "file.txt"}, gen, nil)
	expanded.recursive = true
	a.Equal([]string{"dir", "dir/child.txt", "file.txt"}, collectRelPaths(a, expanded), "default mode expands the directory")

	selfOnly := newTestListTraverser([]string{"dir", "file.txt"}, gen, nil)
	selfOnly.recursive = true
	selfOnly.directoriesSelfOnly = true
	a.Equal([]string{"dir", "file.txt"}, collectRelPaths(a, selfOnly), "self-only mode yields the directory itself and the file")
}

func TestListTraverserSelfOnlyReportsDirectoriesItCannotNarrow(t *testing.T) {
	a := assert.New(t)
	traversed := false
	gen := func(string) (ResourceTraverser, error) { return fakeDirNoSelf{traversed: &traversed}, nil }

	errCh := make(chan TraverserErrorItemInfo, 1)
	l := newTestListTraverser([]string{"dir"}, gen, errCh)
	l.recursive = true
	l.directoriesSelfOnly = true
	a.Empty(collectRelPaths(a, l))
	a.False(traversed, "directory was expanded")

	close(errCh)
	item, ok := <-errCh
	if a.True(ok, "nothing reported") {
		a.ErrorIs(item.ErrorMessage(), errDirectoryNotSelfTraversable)
		a.Equal("dir", item.FullPath())
	}
}

func TestListTraverserSelfOnlyEmptyDirectoryIsNotWarned(t *testing.T) {
	a := assert.New(t)
	warnings := captureListEntryWarnings(t)
	gen := func(string) (ResourceTraverser, error) { return fakeEmptyDir{}, nil }

	errCh := make(chan TraverserErrorItemInfo, 1)
	l := newTestListTraverser([]string{"stub"}, gen, errCh)
	l.recursive = true
	l.directoriesSelfOnly = true
	collectRelPaths(a, l)
	a.Empty(*warnings, "a directory that yields nothing by design is not warned about")
	a.Empty(errCh, "a directory that yields nothing by design is not an error")
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

// heads counts the HEAD (properties) requests the transport saw for path.
func (tr *hnsDirTransport) heads(path string) int {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	n := 0
	for _, r := range tr.requests {
		if strings.HasPrefix(r, "HEAD /c/"+path+"?") {
			n++
		}
	}
	return n
}

// newHnsDirListTraverser is a self-only list traverser over real blob
// traversers on a fake service, configured as XDM configures an HNS source
// (recursive, directory stubs included).
func newHnsDirListTraverser(a *assert.Assertions, entries []string, includeDirStubs, selfOnly bool) (*listTraverser, *hnsDirTransport) {
	tr := &hnsDirTransport{}
	sc, err := service.NewClientWithNoCredential("https://acct.blob.core.windows.net/", &service.ClientOptions{
		ClientOptions: azcore.ClientOptions{Transport: tr, Retry: policy.RetryOptions{MaxRetries: -1}},
	})
	a.NoError(err)
	gen := func(childPath string) (ResourceTraverser, error) {
		return newBlobTraverser("https://acct.blob.core.windows.net/c/"+childPath, sc, context.Background(), InitResourceTraverserOptions{
			Recursive:             true,
			IncludeDirectoryStubs: includeDirStubs,
			IncrementEnumeration:  enumerationCounterFuncNoop,
			// As the list traverser configures its entries.
			cacheSingleBlobLookup: true,
		}), nil
	}
	l := newTestListTraverser(entries, gen, nil)
	l.recursive = true
	l.directoriesSelfOnly = selfOnly
	return l, tr
}

func collectObjects(a *assert.Assertions, l *listTraverser) map[string]common.EntityType {
	got := map[string]common.EntityType{}
	a.NoError(l.Traverse(nil, func(o StoredObject) error {
		got[o.relativePath] = o.entityType
		return nil
	}, nil))
	return got
}

func TestBlobListEntryDirectorySelfOnly(t *testing.T) {
	a := assert.New(t)

	// Baseline: without self-only, the directory entry expands to its file.
	l, tr := newHnsDirListTraverser(a, []string{"dir"}, true, false)
	got := collectObjects(a, l)
	a.Equal(common.EEntityType.Folder(), got["dir"])
	a.Equal(common.EEntityType.File(), got["dir/a.txt"])
	a.True(tr.listed(), "default mode did not list the directory; the fake does not model expansion")

	// Self-only: the directory as a folder, nothing under it, no listing.
	l, tr = newHnsDirListTraverser(a, []string{"dir", "file.txt"}, true, true)
	a.Equal(map[string]common.EntityType{
		"dir":      common.EEntityType.Folder(),
		"file.txt": common.EEntityType.File(),
	}, collectObjects(a, l))
	a.False(tr.listed(), "self-only mode listed a directory: %q", tr.requests)
}

func TestBlobListEntryDirectoryStubSelfOnlyWithoutFolders(t *testing.T) {
	a := assert.New(t)
	// A transfer that does not carry folders (flat namespace to flat
	// namespace) yields nothing for a directory stub, and does not expand it.
	l, tr := newHnsDirListTraverser(a, []string{"dir"}, false, true)
	a.Empty(collectObjects(a, l))
	a.False(tr.listed(), "listed a directory: %q", tr.requests)
}
