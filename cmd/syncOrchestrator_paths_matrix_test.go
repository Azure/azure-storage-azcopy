//go:build smslidingwindow

// Copyright Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package cmd

import (
	"context"
	"fmt"
	"html"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/buildmode"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every source/destination pair runSyncOrchestrator accepts (see validate()).
// TestSyncPathMatrixCoversEverySupportedDirection keeps this list honest.
var orchestratorDirections = []common.FromTo{
	common.EFromTo.LocalBlob(),
	common.EFromTo.LocalBlobFS(),
	common.EFromTo.LocalFile(),
	common.EFromTo.LocalFileNFS(),
	common.EFromTo.S3Blob(),
	common.EFromTo.BlobBlob(),
	common.EFromTo.BlobBlobFS(),
	common.EFromTo.BlobFSBlob(),
	common.EFromTo.BlobFSBlobFS(),
	common.EFromTo.FileFile(),
}

// asciiSpecialChars returns every printable ASCII character except letters,
// digits and the path separator: space ! " # $ % & ' ( ) * + , - . : ; < = > ? @ [ \ ] ^ _ ` { | } ~
func asciiSpecialChars() []string {
	var out []string
	for c := rune(0x20); c <= 0x7e; c++ {
		if c == '/' || unicode.IsLetter(c) || unicode.IsDigit(c) {
			continue
		}
		out = append(out, string(c))
	}
	return out
}

var (
	unicodeSpecialNames = []string{
		"\u00e9",       // é, precomposed (NFC)
		"e\u0301",      // é, decomposed (NFD) - must not be normalized
		"\u00f1\u00df", // ñß
		"\u0433.",      // Cyrillic + trailing dot (Run1 folder shape)
		"\u65e5\u672c", // CJK
		"\U0001f600",   // emoji outside the BMP (UTF-16 surrogate pair)
		"\U0001f468\u200d\U0001f469\u200d\U0001f467", // ZWJ emoji sequence
		"\u200b",                   // zero-width space
		"\ufeff",                   // byte-order mark
		"\u202e",                   // right-to-left override
		"\u2028",                   // line separator
		"\u00a0",                   // no-break space
		"\uff03\uff05\uff1f\uff0f", // full-width # % ? / (look-alikes, not delimiters)
	}
	controlChars = []string{"\t", "\x01", "\x1f", "\x7f"}
)

// Characters Azure Files (SMB) rejects in names, in addition to control characters.
const fileInvalidNameChars = `"\/:|<>*?`

// nameForms places c at every position that matters to a URL parser: alone, at
// either end, in the middle, doubled, next to escape-looking text ("%41" would
// decode to "A"), and next to the fragment/query delimiters.
func nameForms(c string) []string {
	return []string{c, "a" + c, c + "b", "a" + c + "b", c + c, c + "%41", "%" + c, "#" + c, c + "#", c + "?x"}
}

// legalName reports whether a single path segment can exist at loc.
func legalName(name string, loc common.Location) bool {
	if name == "" || name == "." || name == ".." || strings.Contains(name, "/") || strings.ContainsRune(name, 0) {
		return false
	}
	switch loc {
	case common.ELocation.File(), common.ELocation.FileNFS():
		if strings.ContainsAny(name, fileInvalidNameChars) || strings.HasSuffix(name, " ") {
			return false
		}
		for _, r := range name {
			if r < 0x20 || r == 0x7f {
				return false
			}
		}
		return utf8.ValidString(name)
	case common.ELocation.Local():
		// Windows local paths are rewritten to '\' separators, so a literal '\' is ambiguous there.
		return !(runtime.GOOS == "windows" && strings.Contains(name, `\`))
	default: // Blob, BlobFS, S3: any non-empty key segment
		return true
	}
}

// syncPathMatrixNames is the full name inventory: every special character in
// every form, plus every ordered pair of ASCII specials (with and without a
// separator character), plus one name containing all ASCII specials at once.
func syncPathMatrixNames() []string {
	seen := map[string]bool{}
	var names []string
	add := func(n string) {
		if !seen[n] {
			seen[n] = true
			names = append(names, n)
		}
	}
	ascii := asciiSpecialChars()
	for _, group := range [][]string{ascii, unicodeSpecialNames, controlChars} {
		for _, c := range group {
			for _, n := range nameForms(c) {
				add(n)
			}
		}
	}
	for _, a := range ascii {
		for _, b := range ascii {
			add(a + b)
			add(a + "x" + b)
		}
	}
	add(strings.Join(ascii, ""))
	add("a" + strings.Join(ascii, "") + "z")
	return names
}

// syncPathShapes turns a name into the relative directory paths the
// orchestrator hands to syncDirectoryResources: top level, nested in itself
// (the Run2 runaway-recursion shape), and between ordinary siblings.
func syncPathShapes(name string) []string {
	return []string{
		"/" + name + "/",
		"/" + name + "/" + name + "/",
		"/p q/" + name + "/r/",
	}
}

type syncPathFixture struct {
	resource common.ResourceString
	rootPath string // decoded path of resource within its container/share/bucket
	parse    func(full string) (string, error)
}

const syncMatrixRootEscaped = "root%20%23%25" // "root #%": the root itself is already escaped

func syncPathFixtureFor(loc common.Location, isSource bool) syncPathFixture {
	parseContainer := func(container string, rewriteDFS bool) func(string) (string, error) {
		return func(full string) (string, error) {
			if rewriteDFS {
				full = strings.Replace(full, ".dfs.", ".blob.", 1)
			}
			p, err := blob.ParseURL(full)
			if err != nil {
				return "", err
			}
			if p.ContainerName != container {
				return "", fmt.Errorf("container %q, want %q", p.ContainerName, container)
			}
			return p.BlobName, nil
		}
	}
	switch loc {
	case common.ELocation.Blob():
		return syncPathFixture{
			resource: common.ResourceString{Value: "https://acct.blob.core.windows.net/cont/" + syncMatrixRootEscaped, SAS: "sv=2025-05-05&sig=a%2Bb%3D"},
			rootPath: "root #%",
			parse:    parseContainer("cont", false),
		}
	case common.ELocation.BlobFS():
		return syncPathFixture{
			resource: common.ResourceString{Value: "https://acct.dfs.core.windows.net/fs/" + syncMatrixRootEscaped, SAS: "sv=2025-05-05&sig=a%2Bb%3D"},
			rootPath: "root #%",
			parse:    parseContainer("fs", true),
		}
	case common.ELocation.File(), common.ELocation.FileNFS():
		f := syncPathFixture{
			resource: common.ResourceString{Value: "https://acct.file.core.windows.net/share/" + syncMatrixRootEscaped, SAS: "sv=2025-05-05&sig=a%2Bb%3D"},
			rootPath: "root #%",
		}
		wantSnapshot := ""
		if isSource {
			f.resource.ExtraQuery = "sharesnapshot=" + fileSyncTestSnapshot
			wantSnapshot = fileSyncTestSnapshot
		}
		f.parse = func(full string) (string, error) {
			p, err := file.ParseURL(full)
			if err != nil {
				return "", err
			}
			if p.ShareName != "share" || p.ShareSnapshot != wantSnapshot {
				return "", fmt.Errorf("share %q snapshot %q", p.ShareName, p.ShareSnapshot)
			}
			return p.DirectoryOrFilePath, nil
		}
		return f
	case common.ELocation.S3():
		return syncPathFixture{
			resource: common.ResourceString{Value: "https://bucket.s3.us-west-2.amazonaws.com/" + syncMatrixRootEscaped},
			rootPath: "root #%",
			parse: func(full string) (string, error) {
				u, err := url.Parse(full)
				if err != nil {
					return "", err
				}
				p, err := common.NewS3URLParts(*u)
				if err != nil {
					return "", err
				}
				if p.BucketName != "bucket" {
					return "", fmt.Errorf("bucket %q", p.BucketName)
				}
				return p.ObjectKey, nil
			},
		}
	case common.ELocation.Local():
		return syncPathFixture{
			resource: common.ResourceString{Value: filepath.FromSlash("/local/root #%")},
			rootPath: "/local/root #%",
			parse:    func(full string) (string, error) { return filepath.ToSlash(full), nil },
		}
	}
	panic("no fixture for " + loc.String())
}

// expectedDestinationName is the documented naming contract for destination
// paths (pathEncodeRules, source=false): Azure Files (SMB) targets store
// characters SMB rejects as literal %XX text; Windows-local sources decode that
// text back. Everything else is stored verbatim.
func expectedDestinationName(name string, fromTo common.FromTo) string {
	encode := fromTo.To() == common.ELocation.File() ||
		(fromTo.To() == common.ELocation.FileNFS() && !buildmode.IsMover)
	if encode {
		for c, enc := range encodedInvalidCharacters {
			name = strings.ReplaceAll(name, string(c), enc)
		}
		return name
	}
	if (fromTo.From() == common.ELocation.Local() && runtime.GOOS == "windows") || fromTo.From().IsFile() {
		for enc, c := range reverseEncodedChars {
			name = strings.ReplaceAll(name, enc, string(c))
		}
	}
	return name
}

func trimSlash(s string) string { return strings.TrimSuffix(s, "/") }

// checkSyncDirectoryResources validates one relative directory path for one
// direction and returns a description of every violated invariant.
func checkSyncDirectoryResources(fromTo common.FromTo, relativePath string) []string {
	srcFix := syncPathFixtureFor(fromTo.From(), true)
	dstFix := syncPathFixtureFor(fromTo.To(), false)
	cca := cookedSyncCmdArgs{fromTo: fromTo, source: srcFix.resource, destination: dstFix.resource}
	source, destination := cca.syncDirectoryResources(relativePath)

	var problems []string
	fail := func(format string, a ...any) { problems = append(problems, fmt.Sprintf(format, a...)) }

	if cca.source != srcFix.resource || cca.destination != dstFix.resource {
		fail("root resources were mutated")
	}
	if source.SAS != srcFix.resource.SAS || source.ExtraQuery != srcFix.resource.ExtraQuery ||
		destination.SAS != dstFix.resource.SAS || destination.ExtraQuery != dstFix.resource.ExtraQuery {
		fail("SAS/ExtraQuery not preserved")
	}

	check := func(role string, res common.ResourceString, fix syncPathFixture, loc common.Location, wantRel string) string {
		if loc.IsRemote() {
			u, err := url.Parse(res.Value)
			if err != nil {
				fail("%s: url.Parse: %v", role, err)
				return ""
			}
			if u.Fragment != "" || u.RawQuery != "" {
				fail("%s: path leaked into fragment %q / query %q", role, u.Fragment, u.RawQuery)
			}
			if strings.Contains(res.Value, `\`) {
				fail("%s: remote URL contains '\\'", role)
			}
		}
		full := res.Value
		if loc.IsRemote() {
			var err error
			if full, err = res.String(); err != nil {
				fail("%s: String: %v", role, err)
				return ""
			}
		}
		got, err := fix.parse(full)
		if err != nil {
			fail("%s: parse %q: %v", role, full, err)
			return ""
		}
		if want := fix.rootPath + wantRel; trimSlash(got) != trimSlash(want) {
			fail("%s: got path %q, want %q", role, got, want)
		}
		return got
	}

	segments := strings.Split(relativePath, "/")
	dstSegments := make([]string, len(segments))
	for i, s := range segments {
		dstSegments[i] = expectedDestinationName(s, fromTo)
	}
	srcDir := check("source", source, srcFix, fromTo.From(), relativePath)
	dstDir := check("destination", destination, dstFix, fromTo.To(), strings.Join(dstSegments, "/"))

	// Files found inside this directory are scheduled by scheduleCopyTransfer with
	// pathEncodeRules on the whole relative path. They must land inside the very
	// directory that was listed, otherwise comparison/deletion targets the wrong place.
	child := strings.TrimPrefix(relativePath, "/") + "f.txt"
	for _, side := range []struct {
		role     string
		isSource bool
		fix      syncPathFixture
		dir      string
		loc      common.Location
	}{
		{"source", true, srcFix, srcDir, fromTo.From()},
		{"destination", false, dstFix, dstDir, fromTo.To()},
	} {
		if side.dir == "" && relativePath != "/" {
			continue // already reported
		}
		rel := pathEncodeRules(child, fromTo, false, side.isSource)
		res := side.fix.resource
		sep := "/"
		if !side.loc.IsRemote() && runtime.GOOS == "windows" {
			sep, rel = `\`, strings.ReplaceAll(rel, "/", `\`)
		}
		res.Value += sep + rel
		full := res.Value
		if side.loc.IsRemote() {
			var err error
			if full, err = res.String(); err != nil {
				fail("%s transfer: String: %v", side.role, err)
				continue
			}
		}
		got, err := side.fix.parse(full)
		if err != nil {
			fail("%s transfer: parse %q: %v", side.role, full, err)
			continue
		}
		if want := trimSlash(side.dir) + "/f.txt"; got != want {
			fail("%s transfer path %q is not inside listed directory %q", side.role, got, side.dir)
		}
	}
	return problems
}

func TestSyncPathMatrixCoversEverySupportedDirection(t *testing.T) {
	var supported []common.FromTo
	for from := common.Location(0); from <= common.ELocation.FileNFS(); from++ {
		for to := common.Location(0); to <= common.ELocation.FileNFS(); to++ {
			ft := common.FromToValue(from, to)
			if validate(&cookedSyncCmdArgs{fromTo: ft}, &SyncOrchestratorOptions{}) == nil {
				supported = append(supported, ft)
			}
		}
	}
	assert.ElementsMatch(t, orchestratorDirections, supported,
		"a new sync orchestrator direction needs a fixture in syncPathFixtureFor")
}

// TestSyncDirectoryResourcesCharacterMatrix runs every name in
// syncPathMatrixNames, in every path shape, through every supported direction.
func TestSyncDirectoryResourcesCharacterMatrix(t *testing.T) {
	names := syncPathMatrixNames()
	for _, fromTo := range orchestratorDirections {
		t.Run(fromTo.String(), func(t *testing.T) {
			checked, failures := 0, 0
			for _, name := range names {
				if !legalName(name, fromTo.From()) {
					continue
				}
				for _, rel := range syncPathShapes(name) {
					checked++
					for _, p := range checkSyncDirectoryResources(fromTo, rel) {
						if failures++; failures <= 20 {
							t.Errorf("%q: %s", rel, p)
						}
					}
				}
			}
			if failures > 20 {
				t.Errorf("... %d more failures", failures-20)
			}
			require.Greater(t, checked, 500, "matrix unexpectedly small")
			t.Logf("%d relative paths checked", checked)
		})
	}
}

// FuzzSyncDirectoryResources explores arbitrary two-level directory names. The
// seed corpus runs in every `go test`; run with -fuzz to search further.
func FuzzSyncDirectoryResources(f *testing.F) {
	for _, seed := range [][2]string{
		{"#1 beta", "#1 beta"}, {"%41%42", "x%2Fy"}, {"a?b", "c#d"}, {"%", "%%"},
		{"%zz", "%2"}, {"[x]", "{y}"}, {"ends.", "."}, {"e\u0301", "\U0001f600"},
		{"\u202e", "\ufeff"}, {"\xff\xfe", "ok"}, {"\t", " "},
	} {
		f.Add(seed[0], seed[1])
	}
	f.Fuzz(func(t *testing.T, a, b string) {
		for _, fromTo := range orchestratorDirections {
			if !legalName(a, fromTo.From()) || !legalName(b, fromTo.From()) {
				continue
			}
			for _, p := range checkSyncDirectoryResources(fromTo, "/"+a+"/"+b+"/") {
				t.Errorf("%s %q/%q: %s", fromTo, a, b, p)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// End-to-end: generated trees through runSyncOrchestrator and the real traversers.
// ---------------------------------------------------------------------------

// syncListingRecorder counts how often each directory was listed per side.
type syncListingRecorder struct {
	mu         sync.Mutex
	listed     map[string]map[string]int // "src"/"dst" -> dir -> count
	unexpected []string
}

// runSyncOrchestratorAgainstTree serves tree as the source (File share or Blob
// container), an empty destination, and runs the orchestrator to completion.
func runSyncOrchestratorAgainstTree(t *testing.T, fromTo common.FromTo, tree fileShareTree, mergeJoin bool) *syncListingRecorder {
	t.Helper()
	t.Setenv(enum.EEnvironmentVariable.EnableAzFilesProactiveStats().Name, "")
	rec := &syncListingRecorder{listed: map[string]map[string]int{"src": {}, "dst": {}}}
	isFile := fromTo.From().IsFile()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		q := r.URL.Query()
		if !isFile && r.Method == http.MethodHead {
			w.Header().Set("x-ms-error-code", "BlobNotFound")
			w.WriteHeader(http.StatusNotFound)
			return
		}
		side, rest, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
		listOK := r.Method == http.MethodGet && q.Get("comp") == "list" &&
			(isFile && q.Get("restype") == "directory" || !isFile && q.Get("restype") == "container")
		if !listOK || (side != "src" && side != "dst") {
			rec.unexpected = append(rec.unexpected, r.Method+" "+r.URL.String())
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}

		dir := strings.TrimSuffix(rest, "/")
		if !isFile {
			dir = strings.TrimSuffix(q.Get("prefix"), "/")
		}
		rec.listed[side][dir]++

		var children []string
		if side == "src" {
			var exists bool
			children, exists = tree[dir]
			if isFile {
				assert.Equal(t, fileSyncTestSnapshot, q.Get("sharesnapshot"), "source listing must stay on the snapshot")
				assert.Equal(t, "true", r.Header.Get("x-ms-allow-trailing-dot"), "source listing must preserve trailing dots")
				if !exists {
					w.Header().Set("x-ms-error-code", "ParentNotFound")
					w.WriteHeader(http.StatusNotFound)
					return
				}
			}
		}
		children = append([]string(nil), children...)
		sort.Strings(children) // service listings are sorted; merge-join depends on it

		var body strings.Builder
		w.Header().Set("Content-Type", "application/xml")
		if isFile {
			for _, c := range children {
				fmt.Fprintf(&body, "<Directory><Name>%s</Name><Properties /></Directory>", html.EscapeString(c))
			}
			fmt.Fprintf(w, `<?xml version="1.0" encoding="utf-8"?><EnumerationResults><Entries>%s</Entries><NextMarker /></EnumerationResults>`, body.String())
			return
		}
		prefix := q.Get("prefix")
		for _, c := range children {
			fmt.Fprintf(&body, "<BlobPrefix><Name>%s</Name></BlobPrefix>", html.EscapeString(prefix+c+"/"))
		}
		fmt.Fprintf(w, `<?xml version="1.0" encoding="utf-8"?><EnumerationResults><Blobs>%s</Blobs><NextMarker /></EnumerationResults>`, body.String())
	}))
	t.Cleanup(server.Close)

	oldOptions, oldParallelism, oldThrottling, oldUseOrchestrator := orchestratorOptions, crawlParallelism, enableThrottling, UseSyncOrchestrator
	t.Cleanup(func() {
		orchestratorOptions, crawlParallelism, enableThrottling, UseSyncOrchestrator = oldOptions, oldParallelism, oldThrottling, oldUseOrchestrator
	})
	orchestratorOptions = &SyncOrchestratorOptions{fromTo: fromTo}
	crawlParallelism, enableThrottling, UseSyncOrchestrator = 16, false, true

	// localhost avoids the SDK's IP-address (emulator-style) URL parsing rules.
	baseURL := strings.Replace(server.URL, "127.0.0.1", "localhost", 1)
	cca := &cookedSyncCmdArgs{
		fromTo:                fromTo,
		source:                common.ResourceString{Value: baseURL + "/src"},
		destination:           common.ResourceString{Value: baseURL + "/dst"},
		useStreamingMergeJoin: mergeJoin,
	}
	if isFile {
		cca.source.ExtraQuery = "sharesnapshot=" + fileSyncTestSnapshot
	}
	errorChannel := make(chan TraverserErrorItemInfo, 4*len(tree)+16)
	options := InitResourceTraverserOptions{
		Credential:   &common.CredentialInfo{CredentialType: enum.ECredentialType.Anonymous()},
		ErrorChannel: errorChannel,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	primaryTraverser, err := InitResourceTraverser(cca.source, fromTo.From(), ctx, options)
	require.NoError(t, err)
	enumerator := &syncEnumerator{
		primaryTraverser:           primaryTraverser,
		primaryTraverserTemplate:   ResourceTraverserTemplate{location: fromTo.From(), options: options},
		secondaryTraverserTemplate: ResourceTraverserTemplate{location: fromTo.To(), options: options},
		objectIndexer:              newObjectIndexer(),
		objectComparator:           func(StoredObject) error { return nil },
		orchestratorOptions:        orchestratorOptions,
		ctp: &copyTransferProcessor{
			copyJobTemplate:        &common.CopyJobPartOrderRequest{FromTo: fromTo},
			folderPropertiesOption: common.EFolderPropertiesOption.NoFolders(),
		},
		finalize: func() error { return nil },
	}

	require.NoError(t, cca.runSyncOrchestrator(enumerator, ctx))
	require.NoError(t, ctx.Err(), "crawl must terminate on its own (runaway recursion?)")
	close(errorChannel)
	for e := range errorChannel {
		t.Errorf("traversal error for %s: %v", e.FullPath(), e.ErrorMessage())
	}
	return rec
}

// assertEachDirectoryListedOnce: every source directory listed exactly once
// (the root may also be probed by IsDirectory), and neither side lists a
// directory that does not exist.
func assertEachDirectoryListedOnce(t *testing.T, tree fileShareTree, rec *syncListingRecorder) {
	t.Helper()
	rec.mu.Lock()
	defer rec.mu.Unlock()
	assert.Empty(t, rec.unexpected)
	for _, dir := range tree.allDirs() {
		got := rec.listed["src"][dir]
		if dir == "" {
			assert.Contains(t, []int{1, 2}, got, "root listed %d times", got)
		} else {
			assert.Equal(t, 1, got, "source directory %q listed %d times", dir, got)
		}
	}
	for _, side := range []string{"src", "dst"} {
		for dir := range rec.listed[side] {
			_, exists := tree[dir]
			assert.True(t, exists, "%s listed phantom directory %q", side, dir)
		}
	}
}

// generatedSpecialTree builds a tree with one top-level directory per legal
// special name, each containing a same-named child (the Run2 recursion shape).
func generatedSpecialTree(loc common.Location) fileShareTree {
	tree := fileShareTree{"": nil}
	seen := map[string]bool{}
	var candidates []string
	for _, c := range append(asciiSpecialChars(), unicodeSpecialNames...) {
		candidates = append(candidates, c, "a"+c+"b", "#"+c, c+"%41")
	}
	for _, name := range candidates {
		if seen[name] || !legalName(name, loc) || strings.ContainsAny(name, "\t\x01\x1f\x7f") {
			continue
		}
		seen[name] = true
		tree[""] = append(tree[""], name)
		tree[name] = []string{name}
		tree[name+"/"+name] = nil
	}
	return tree
}

func TestSyncOrchestratorGeneratedSpecialNamesFileShare(t *testing.T) {
	tree := generatedSpecialTree(common.ELocation.File())
	require.Greater(t, len(tree), 100)
	assertEachDirectoryListedOnce(t, tree, runSyncOrchestratorAgainstTree(t, common.EFromTo.FileFile(), tree, false))
}

func TestSyncOrchestratorGeneratedSpecialNamesBlob(t *testing.T) {
	tree := generatedSpecialTree(common.ELocation.Blob())
	require.Greater(t, len(tree), 100)
	for _, mergeJoin := range []bool{false, true} {
		t.Run(fmt.Sprintf("mergeJoin=%t", mergeJoin), func(t *testing.T) {
			assertEachDirectoryListedOnce(t, tree, runSyncOrchestratorAgainstTree(t, common.EFromTo.BlobBlob(), tree, mergeJoin))
		})
	}
}
