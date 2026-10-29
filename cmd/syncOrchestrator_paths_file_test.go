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
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/buildmode"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const fileSyncTestSnapshot = "2026-10-27T09:30:47.0000000Z"

// Directory names that are legal on Azure Files (SMB) but change a URL's meaning
// when appended unescaped: '#' starts a fragment (the Run2 failure: listing
// ".../#1 beta" listed its parent and recursed without bound), '%' starts an
// escape ("%41%42" would decode to "AB", "%2F" to a separator), and ;&=+,$@
// are sub-delims. Brackets, backticks and braces must be escaped too, and a
// trailing '.' must survive for Azure Files trailing-dot support.
var fileSyncSpecialDirs = []string{
	"/",
	"/alpha/",
	"/alpha/#1 beta/",
	"/alpha/#1 beta/#1 beta/",
	"/gamma #2/delta#3#/",
	"/50% off/%41%42/",
	"/x%2Fy/z%23w/",
	"/q&r=s+t;u@v,w$/",
	"/[bracket]/(paren)!'~^`{brace}/",
	"/caf\u00e9 \u00f1/#\u00fc\u00df/",
	"/ends with dot./1/",
	"/\u65e5\u672c/\U0001f680/",
}

func TestSyncDirectoryResourcesFileShare(t *testing.T) {
	for _, relativePath := range fileSyncSpecialDirs {
		t.Run(relativePath, func(t *testing.T) {
			cca := cookedSyncCmdArgs{
				fromTo: common.EFromTo.FileFile(),
				source: common.ResourceString{
					Value:      "https://src.file.core.windows.net/share/root%20%23%25",
					SAS:        "sig=source",
					ExtraQuery: "sharesnapshot=" + fileSyncTestSnapshot,
				},
				destination: common.ResourceString{
					Value: "https://dst.file.core.windows.net/share/root%20%23%25",
					SAS:   "sig=target",
				},
			}
			source, destination := cca.syncDirectoryResources(relativePath)

			want := strings.TrimSuffix("root #%"+relativePath, "/")
			for _, tc := range []struct {
				name     string
				resource common.ResourceString
				snapshot string
			}{
				{"source", source, fileSyncTestSnapshot},
				{"destination", destination, ""},
			} {
				full, err := tc.resource.String()
				require.NoError(t, err, tc.name)
				parts, err := file.ParseURL(full)
				require.NoError(t, err, tc.name)
				assert.Equal(t, "share", parts.ShareName, tc.name)
				assert.Equal(t, tc.snapshot, parts.ShareSnapshot, tc.name)
				assert.Equal(t, want, strings.TrimSuffix(parts.DirectoryOrFilePath, "/"), tc.name)

				u, err := url.Parse(tc.resource.Value)
				require.NoError(t, err, tc.name)
				assert.Empty(t, u.Fragment, tc.name)
				assert.Empty(t, u.RawQuery, tc.name)
				assert.NotContains(t, tc.resource.Value, `\`, "remote URLs must keep '/' separators on every OS")
			}
			assert.Equal(t, cca.source.ExtraQuery, source.ExtraQuery)
			assert.Equal(t, cca.source.SAS, source.SAS)
			assert.Equal(t, cca.destination.SAS, destination.SAS)
		})
	}
}

// Local -> Azure Files: the source stays a literal filesystem path, and the
// destination directory must be encoded exactly as scheduleCopyTransfer encodes
// the files inside it, otherwise the destination listing compares the wrong
// directory and every file looks new (or extra, with delete-destination).
func TestSyncDirectoryResourcesLocalToFileShare(t *testing.T) {
	const relativePath = `/what?/x:y*z/q"|<>/#1 100%/`
	encoded := "what%3F/x%3Ay%2Az/q%22%7C%3C%3E/#1 100%"

	for _, tc := range []struct {
		fromTo      common.FromTo
		wantDstPath string
	}{
		{common.EFromTo.LocalFile(), encoded},
		{common.EFromTo.LocalFileNFS(), func() string {
			if buildmode.IsMover {
				return strings.Trim(relativePath, "/") // Mover keeps NFS-legal names literal.
			}
			return encoded
		}()},
	} {
		t.Run(tc.fromTo.String(), func(t *testing.T) {
			cca := cookedSyncCmdArgs{
				fromTo:      tc.fromTo,
				source:      common.ResourceString{Value: filepath.FromSlash("/source")},
				destination: common.ResourceString{Value: "https://dst.file.core.windows.net/share"},
			}
			source, destination := cca.syncDirectoryResources(relativePath)
			assert.Equal(t, filepath.FromSlash("/source"+relativePath), source.Value)

			parts, err := file.ParseURL(destination.Value)
			require.NoError(t, err)
			assert.Equal(t, tc.wantDstPath, strings.TrimSuffix(parts.DirectoryOrFilePath, "/"))

			transferDst := pathEncodeRules(strings.Trim(relativePath, "/")+"/f.txt", tc.fromTo, false, false)
			transferParts, err := file.ParseURL("https://dst.file.core.windows.net/share/" + transferDst)
			require.NoError(t, err)
			assert.Equal(t, parts.DirectoryOrFilePath, strings.TrimSuffix(transferParts.DirectoryOrFilePath, "f.txt"),
				"directory listing URL and transfer destination must agree")
		})
	}
}

// The sync orchestrator probes the source root with IsDirectory before crawling.
// For a share-snapshot source both probe shapes (share root listing, directory
// GetProperties) must target the snapshot, not the live share.
func TestFileTraverserIsDirectoryUsesShareSnapshot(t *testing.T) {
	t.Setenv(enum.EEnvironmentVariable.EnableAzFilesProactiveStats().Name, "")
	for _, dir := range []string{"", "Viesinimas/#4_PP 100%"} {
		t.Run("dir="+dir, func(t *testing.T) {
			var requests []string
			var mu sync.Mutex
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				requests = append(requests, r.Method+" "+r.URL.Path)
				mu.Unlock()
				assert.Equal(t, fileSyncTestSnapshot, r.URL.Query().Get("sharesnapshot"), "%s %s", r.Method, r.URL)
				assert.Equal(t, strings.TrimSuffix("/share/"+dir, "/"), strings.TrimSuffix(r.URL.Path, "/"))
				if r.URL.Query().Get("comp") == "list" {
					w.Header().Set("Content-Type", "application/xml")
					fmt.Fprint(w, `<?xml version="1.0" encoding="utf-8"?><EnumerationResults><Entries /><NextMarker /></EnumerationResults>`)
					return
				}
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()

			baseURL := strings.Replace(server.URL, "127.0.0.1", "localhost", 1)
			source := common.ResourceString{Value: baseURL + "/share", ExtraQuery: "sharesnapshot=" + fileSyncTestSnapshot}
			if dir != "" {
				source.Value += "/" + pathEncodeRules(dir, common.EFromTo.FileFile(), false, true)
			}
			traverser, err := InitResourceTraverser(source, common.ELocation.File(), context.Background(), InitResourceTraverserOptions{
				Credential: &common.CredentialInfo{CredentialType: enum.ECredentialType.Anonymous()},
			})
			require.NoError(t, err)
			isDir, err := traverser.IsDirectory(true)
			require.NoError(t, err)
			assert.True(t, isDir)
			mu.Lock()
			defer mu.Unlock()
			assert.Len(t, requests, 1)
		})
	}
}

// fileShareTree maps a share-relative directory path ("" for the root) to its
// child directory names.
type fileShareTree map[string][]string

func (tree fileShareTree) allDirs() []string {
	dirs := make([]string, 0, len(tree))
	for dir := range tree {
		dirs = append(dirs, dir)
	}
	sort.Strings(dirs)
	return dirs
}

// End-to-end through runSyncOrchestrator, the real file traverser and SDK pager:
// each source directory must be listed exactly once, at its own path, against
// the share snapshot. Before the fix, "#" directories listed their parent and the
// crawl recursed until the service rejected the path as too long.
func TestSyncOrchestratorFileShareSpecialDirectoryNames(t *testing.T) {
	t.Setenv(enum.EEnvironmentVariable.EnableAzFilesProactiveStats().Name, "")

	const pp = "#4_PP pranesimas apie patvirtinta PP"
	tree := fileShareTree{
		"":                                      {"Viesinimas"},
		"Viesinimas":                            {pp, "#1_PP", "100% done", "a&b=c+d;e", "Report 12.4.2019 \u0433."},
		"Viesinimas/" + pp:                      {pp, "#2 x"},
		"Viesinimas/" + pp + "/" + pp:           {},
		"Viesinimas/" + pp + "/#2 x":            {},
		"Viesinimas/#1_PP":                      {},
		"Viesinimas/100% done":                  {"50%25"},
		"Viesinimas/100% done/50%25":            {},
		"Viesinimas/a&b=c+d;e":                  {},
		"Viesinimas/Report 12.4.2019 \u0433.":   {"0"},
		"Viesinimas/Report 12.4.2019 \u0433./0": {},
	}

	var mu sync.Mutex
	listed := map[string]map[string]int{"src": {}, "dst": {}}
	var unexpected []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		q := r.URL.Query()
		if r.Method != http.MethodGet || q.Get("restype") != "directory" || q.Get("comp") != "list" {
			unexpected = append(unexpected, r.Method+" "+r.URL.String())
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		share, dir, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
		dir = strings.TrimSuffix(dir, "/")
		if share != "src" && share != "dst" {
			unexpected = append(unexpected, "share "+share)
			http.Error(w, "unexpected share", http.StatusBadRequest)
			return
		}
		listed[share][dir]++

		children, exists := tree[dir]
		if share == "src" {
			assert.Equal(t, fileSyncTestSnapshot, q.Get("sharesnapshot"), "source listing must stay on the snapshot: %s", r.URL.String())
			assert.Equal(t, "true", r.Header.Get("x-ms-allow-trailing-dot"))
			if !exists {
				w.Header().Set("x-ms-error-code", "ParentNotFound")
				w.WriteHeader(http.StatusNotFound)
				return
			}
		} else {
			children = nil // empty destination: everything is new
		}

		var entries strings.Builder
		for _, child := range children {
			fmt.Fprintf(&entries, "<Directory><Name>%s</Name><Properties /></Directory>", html.EscapeString(child))
		}
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprintf(w, `<?xml version="1.0" encoding="utf-8"?><EnumerationResults ShareName="%s" DirectoryPath="%s"><Entries>%s</Entries><NextMarker /></EnumerationResults>`,
			share, html.EscapeString(dir), entries.String())
	}))
	defer server.Close()

	oldOptions, oldParallelism, oldThrottling, oldUseOrchestrator := orchestratorOptions, crawlParallelism, enableThrottling, UseSyncOrchestrator
	t.Cleanup(func() {
		orchestratorOptions, crawlParallelism, enableThrottling, UseSyncOrchestrator = oldOptions, oldParallelism, oldThrottling, oldUseOrchestrator
	})
	orchestratorOptions = &SyncOrchestratorOptions{fromTo: common.EFromTo.FileFile()}
	crawlParallelism, enableThrottling, UseSyncOrchestrator = 8, false, true

	// localhost avoids the SDK's IP-address (emulator-style) URL parsing rules.
	baseURL := strings.Replace(server.URL, "127.0.0.1", "localhost", 1)
	cca := &cookedSyncCmdArgs{
		fromTo:      common.EFromTo.FileFile(),
		source:      common.ResourceString{Value: baseURL + "/src", ExtraQuery: "sharesnapshot=" + fileSyncTestSnapshot},
		destination: common.ResourceString{Value: baseURL + "/dst"},
	}
	errorChannel := make(chan TraverserErrorItemInfo, 64)
	options := InitResourceTraverserOptions{
		Credential:   &common.CredentialInfo{CredentialType: enum.ECredentialType.Anonymous()},
		ErrorChannel: errorChannel,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	primaryTraverser, err := InitResourceTraverser(cca.source, cca.fromTo.From(), ctx, options)
	require.NoError(t, err)
	enumerator := &syncEnumerator{
		primaryTraverser:           primaryTraverser,
		primaryTraverserTemplate:   ResourceTraverserTemplate{location: common.ELocation.File(), options: options},
		secondaryTraverserTemplate: ResourceTraverserTemplate{location: common.ELocation.File(), options: options},
		objectIndexer:              newObjectIndexer(),
		objectComparator:           func(StoredObject) error { return nil },
		orchestratorOptions:        orchestratorOptions,
		ctp: &copyTransferProcessor{
			copyJobTemplate:        &common.CopyJobPartOrderRequest{FromTo: cca.fromTo},
			folderPropertiesOption: common.EFolderPropertiesOption.NoFolders(),
		},
		finalize: func() error { return nil },
	}

	require.NoError(t, cca.runSyncOrchestrator(enumerator, ctx))
	require.NoError(t, ctx.Err(), "crawl must terminate on its own")
	close(errorChannel)
	for e := range errorChannel {
		t.Errorf("traversal error for %s: %v", e.FullPath(), e.ErrorMessage())
	}

	mu.Lock()
	defer mu.Unlock()
	assert.Empty(t, unexpected)
	for _, dir := range tree.allDirs() {
		want := 1
		if dir == "" {
			want = 2 // IsDirectory existence probe + the root listing itself
		}
		assert.Equal(t, want, listed["src"][dir], "source directory %q listed an unexpected number of times", dir)
	}
	for _, share := range []string{"src", "dst"} {
		for dir := range listed[share] {
			_, exists := tree[dir]
			assert.True(t, exists, "%s listed phantom directory %q", share, dir)
		}
	}
	assert.Len(t, listed["src"], len(tree))
}
