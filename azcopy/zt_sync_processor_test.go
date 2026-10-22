// Copyright © 2017 Microsoft <wastore@microsoft.com>
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

package azcopy

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlobDeleterLiteralPaths(t *testing.T) {
	for _, tc := range []struct {
		name, root, key, want, neighbor string
		folder                          bool
	}{
		{"file", "", "a//file", "a//file", "a/file", false},
		{"file-under-prefix", "root//prefix/", "file", "root//prefix/file", "root/prefix/file", false},
		{"folder", "", "a//folder/", "a//folder/", "a/folder/", true},
		{"folder-under-prefix", "root//prefix/", "folder/", "root//prefix/folder/", "root/prefix/folder/", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			blobs, survivors := make(map[string]string), make(map[string]string)
			suffixes := []string{""}
			if tc.folder {
				suffixes = append(suffixes, "one.txt", "nested/two.txt")
			}
			for _, suffix := range suffixes {
				blobs[tc.want+suffix] = "delete:" + suffix
				blobs[tc.neighbor+suffix] = "keep:" + suffix
				survivors[tc.neighbor+suffix] = "keep:" + suffix
			}
			var mu sync.Mutex
			var listed, deleted []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				w.Header().Set("x-ms-request-id", "literal-path-test")
				if r.Method == http.MethodDelete {
					key := strings.TrimPrefix(r.URL.Path, "/container/")
					deleted = append(deleted, key)
					if _, exists := blobs[key]; !exists {
						w.Header().Set("x-ms-error-code", "BlobNotFound")
						w.WriteHeader(http.StatusNotFound)
						return
					}
					delete(blobs, key)
					w.WriteHeader(http.StatusAccepted)
					return
				}
				if !tc.folder || r.Method != http.MethodGet || r.URL.Query().Get("comp") != "list" || r.URL.Query().Get("delimiter") != "/" {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
					http.Error(w, "unexpected request", http.StatusBadRequest)
					return
				}
				prefix := r.URL.Query().Get("prefix")
				listed = append(listed, prefix)
				type entry struct {
					Name string `xml:"Name"`
				}
				var result struct {
					XMLName xml.Name `xml:"EnumerationResults"`
					Blobs   struct {
						Items    []entry `xml:"Blob"`
						Prefixes []entry `xml:"BlobPrefix"`
					} `xml:"Blobs"`
					NextMarker string `xml:"NextMarker"`
				}
				prefixes := make(map[string]bool)
				for key := range blobs {
					if !strings.HasPrefix(key, prefix) {
						continue
					}
					if i := strings.Index(strings.TrimPrefix(key, prefix), "/"); i >= 0 {
						prefixes[key[:len(prefix)+i+1]] = true
					} else {
						result.Blobs.Items = append(result.Blobs.Items, entry{key})
					}
				}
				for child := range prefixes {
					result.Blobs.Prefixes = append(result.Blobs.Prefixes, entry{child})
				}
				w.Header().Set("Content-Type", "application/xml")
				assert.NoError(t, xml.NewEncoder(w).Encode(result))
			}))
			defer server.Close()
			endpoint := strings.Replace(server.URL, "127.0.0.1", "localhost", 1)
			client, err := service.NewClientWithNoCredential(endpoint, &service.ClientOptions{
				ClientOptions: azcore.ClientOptions{Retry: policy.RetryOptions{MaxRetries: -1}},
			})
			require.NoError(t, err)
			rootURL, err := url.Parse(endpoint + "/container/")
			require.NoError(t, err)
			rootURL.Path += tc.root
			fpo, entity := common.EFolderPropertiesOption.NoFolders(), common.EEntityType.File()
			if tc.folder {
				fpo, entity = common.EFolderPropertiesOption.AllFolders(), common.EEntityType.Folder()
			}
			deleter, err := NewRemoteResourceDeleter(context.Background(),
				common.NewServiceClient(client, nil, nil), rootURL, fpo, false)
			require.NoError(t, err)
			deleter.recursive = tc.folder
			require.NoError(t, deleter.Delete("", common.ELocation.Blob(), traverser.StoredObject{RelativePath: tc.key, EntityType: entity}))
			mu.Lock()
			defer mu.Unlock()
			if tc.folder {
				require.NotEmpty(t, listed, "must exercise recursive listing")
				for _, prefix := range listed {
					assert.True(t, strings.HasPrefix(prefix, tc.want), "listing escaped selected prefix: %q", prefix)
				}
			} else {
				assert.Equal(t, []string{tc.want}, deleted)
			}
			assert.Equal(t, survivors, blobs, "delete only the extra blob; preserve normalized neighbors and their bytes")
		})
	}
}
