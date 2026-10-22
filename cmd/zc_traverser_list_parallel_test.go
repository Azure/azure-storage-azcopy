package cmd

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/azure-storage-azcopy/v10/common"
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
	const entries = 200
	var names []string
	for i := 0; i < entries; i++ {
		names = append(names, fmt.Sprintf("f%03d", i))
	}
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
	start := time.Now()
	err := l.Traverse(nil, func(o StoredObject) error {
		if atomic.AddInt32(&inProcessor, 1) != 1 {
			t.Error("processor called concurrently")
		}
		got = append(got, o.name)
		atomic.AddInt32(&inProcessor, -1)
		return nil
	}, nil)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Traverse() = %v", err)
	}
	close(errCh)

	sort.Strings(got)
	if len(got) != entries || got[0] != "f000" || got[entries-1] != fmt.Sprintf("f%03d", entries-1) {
		t.Fatalf("processed %d objects (%v...), want every listed file once", len(got), got[:min(3, len(got))])
	}
	if maxSeen < 2 || maxSeen > 16 {
		t.Errorf("max entries in flight = %d, want 2..16", maxSeen)
	}
	if elapsed > time.Duration(entries)*5*time.Millisecond/4 {
		t.Errorf("took %s, want well under the sequential %s", elapsed, time.Duration(entries)*5*time.Millisecond)
	}
	var reported []string
	for item := range errCh {
		reported = append(reported, item.FullPath())
		if !errors.Is(item.ErrorMessage(), lookupErr) {
			t.Errorf("reported %q with %v", item.FullPath(), item.ErrorMessage())
		}
	}
	if len(reported) != 1 || reported[0] != "denied" {
		t.Errorf("reported %v, want [denied]", reported)
	}
	if len(*warnings) != 2 {
		t.Errorf("warnings %q, want one for gone and a total", *warnings)
	}
}

func TestListTraverserParallelStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var visited int64
	gen := func(childPath string) (ResourceTraverser, error) {
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
	if err := l.Traverse(nil, func(StoredObject) error { mu.Lock(); defer mu.Unlock(); return nil }, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("Traverse() = %v, want context.Canceled", err)
	}
	if v := atomic.LoadInt64(&visited); v > 20+8 {
		t.Fatalf("visited %d entries, want the traversal to stop soon after the cancel", v)
	}
}

func TestNewListTraverserParallelism(t *testing.T) {
	list := make(chan string)
	close(list)
	l := newListTraverser(common.ResourceString{Value: "https://a.blob.core.windows.net/c"}, common.ELocation.Blob(), context.Background(),
		InitResourceTraverserOptions{ListOfFiles: list, ListOfFilesParallelism: 32}).(*listTraverser)
	if l.parallelism != 32 {
		t.Fatalf("parallelism = %d, want 32", l.parallelism)
	}
	if err := l.Traverse(nil, func(StoredObject) error { return nil }, nil); err != nil {
		t.Fatalf("Traverse() over an empty list = %v", err)
	}
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

func TestBlobTraverserCachesSingleBlobLookup(t *testing.T) {
	for _, cache := range []bool{false, true} {
		tr := &hnsDirTransport{}
		sc, err := service.NewClientWithNoCredential("https://acct.blob.core.windows.net/", &service.ClientOptions{
			ClientOptions: azcore.ClientOptions{Transport: tr, Retry: policy.RetryOptions{MaxRetries: -1}},
		})
		if err != nil {
			t.Fatal(err)
		}
		bt := newBlobTraverser("https://acct.blob.core.windows.net/c/file.txt", sc, context.Background(), InitResourceTraverserOptions{
			Recursive:             true,
			IncrementEnumeration:  enumerationCounterFuncNoop,
			cacheSingleBlobLookup: cache,
		})
		if isDir, err := bt.IsDirectory(true); err != nil || isDir {
			t.Fatalf("IsDirectory() = %v, %v", isDir, err)
		}
		var got []string
		if err := bt.Traverse(nil, func(o StoredObject) error { got = append(got, o.name); return nil }, nil); err != nil {
			t.Fatalf("Traverse() = %v", err)
		}
		want := 2
		if cache {
			want = 1
		}
		if n := tr.heads("file.txt"); n != want || len(got) != 1 {
			t.Errorf("cache=%v: %d properties requests and %d objects, want %d and 1", cache, n, len(got), want)
		}
	}

}
