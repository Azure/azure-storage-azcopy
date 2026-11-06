//go:build smslidingwindow

// Copyright Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package cmd

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

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
	for _, dir := range []string{"", "alpha/#1_beta 50%"} {
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

	// Shapes mirror the failing Run1/Run2 data with neutral names:
	//  - Run2: names start with '#' followed by digits, '_', spaces and non-ASCII
	//    letters; plain names ('!', '(', ')', '-') sit between '#' levels, and
	//    the same '#' name repeats down the path.
	//  - Run1: a non-ASCII name with internal dots that ends in '.', with a child.
	const nested = "#7_gamma d\u00e9lta \u0161\u016b"
	const plain = "!old (copy)-1"
	tree := fileShareTree{
		"":                               {"alpha"},
		"alpha":                          {nested, "#1_beta", "50% off", "q&r=s+t;u_~$", "draft v1.2 \u00e9."},
		"alpha/" + nested:                {nested, "#3 x", plain},
		"alpha/" + nested + "/" + nested: {},
		"alpha/" + nested + "/#3 x":      {},
		"alpha/" + nested + "/" + plain:  {"#2 \u017e\u0117"},
		"alpha/" + nested + "/" + plain + "/#2 \u017e\u0117": {},
		"alpha/#1_beta":              {},
		"alpha/50% off":              {"%41%42"},
		"alpha/50% off/%41%42":       {},
		"alpha/q&r=s+t;u_~$":         {},
		"alpha/draft v1.2 \u00e9.":   {"1"},
		"alpha/draft v1.2 \u00e9./1": {},
	}

	assertEachDirectoryListedOnce(t, tree, runSyncOrchestratorAgainstTree(t, common.EFromTo.FileFile(), tree, false))
}
