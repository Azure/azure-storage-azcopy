package traverser

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	fileservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

const filesTestTimestamp = "2020-09-13T12:26:40.0000000Z"

func filesTestListing(entries ...string) string {
	return `<?xml version="1.0" encoding="utf-8"?><EnumerationResults ServiceEndpoint="http://localhost/" ShareName="share" DirectoryPath="">` +
		`<Entries>` + strings.Join(entries, "") + `</Entries><NextMarker /></EnumerationResults>`
}

func filesTestFile(name string) string {
	return fmt.Sprintf(`<File><Name>%s</Name><FileId>1</FileId><Properties><Content-Length>5</Content-Length>`+
		`<CreationTime>%[2]s</CreationTime><LastWriteTime>%[2]s</LastWriteTime><ChangeTime>%[2]s</ChangeTime>`+
		`<Last-Modified>Sun, 13 Sep 2020 12:26:40 GMT</Last-Modified><Etag>"0x1"</Etag></Properties></File>`, name, filesTestTimestamp)
}

// newFilesTestTraverser serves a one-share fake Azure Files endpoint and counts per-file property requests.
func newFilesTestTraverser(t *testing.T, listing string, opts InitResourceTraverserOptions) (*fileTraverser, *atomic.Int32) {
	t.Helper()
	var fileProperties atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		header := w.Header()
		header.Set("Last-Modified", "Sun, 13 Sep 2020 12:26:40 GMT")
		header.Set("ETag", `"0x1"`)
		header.Set("x-ms-file-creation-time", filesTestTimestamp)
		header.Set("x-ms-file-last-write-time", filesTestTimestamp)
		header.Set("x-ms-file-change-time", filesTestTimestamp)
		header.Set("x-ms-file-id", "1")
		header.Set("x-ms-file-parent-id", "0")
		switch {
		case query.Get("comp") == "list":
			header.Set("Content-Type", "application/xml")
			fmt.Fprint(w, listing)
		case query.Get("restype") == "directory":
			header.Set("x-ms-file-attributes", "Directory")
		default:
			fileProperties.Add(1)
			header.Set("x-ms-file-attributes", "Archive")
			header.Set("x-ms-type", "File")
			header.Set("Content-Length", "5")
			header.Set("Content-Type", "text/plain")
			header.Set("Content-Encoding", "identity")
			header.Set("Content-Language", "en")
			header.Set("Content-Disposition", "inline")
			header.Set("Cache-Control", "no-cache")
			header.Set("x-ms-meta-owner", "mover")
		}
	}))
	t.Cleanup(server.Close)
	endpoint := strings.Replace(server.URL, "127.0.0.1", "localhost", 1)
	client, err := fileservice.NewClientWithNoCredential(endpoint, &fileservice.ClientOptions{
		ClientOptions: azcore.ClientOptions{Retry: policy.RetryOptions{MaxRetries: -1}},
	})
	require.NoError(t, err)
	return NewFileTraverser(endpoint+"/share", client, context.Background(), opts), &fileProperties
}

func collectFilesTestObjects(t *testing.T, traversal *fileTraverser) []StoredObject {
	t.Helper()
	var files []StoredObject
	err := traversal.Traverse(NoPreProccessor, func(object StoredObject) error {
		if object.EntityType == common.EEntityType.File() {
			files = append(files, object)
		}
		return nil
	}, nil)
	require.NoError(t, err)
	return files
}

// TestFileTraverserFetchesPropertiesOnlyWhenAsked pins that Files headers and metadata are not collected
// during enumeration by default; copy fetches them in the backend unless the caller asks for them up front.
func TestFileTraverserFetchesPropertiesOnlyWhenAsked(t *testing.T) {
	for _, getProperties := range []bool{false, true} {
		t.Run(fmt.Sprintf("getProperties=%t", getProperties), func(t *testing.T) {
			traversal, fileProperties := newFilesTestTraverser(t, filesTestListing(filesTestFile("a.txt")),
				InitResourceTraverserOptions{Recursive: true, GetPropertiesInFrontend: getProperties})
			files := collectFilesTestObjects(t, traversal)

			require.Len(t, files, 1)
			file := files[0]
			require.Equal(t, "a.txt", file.Name)
			require.EqualValues(t, 5, file.Size)

			fetchesPerFile := getProperties && !UseSyncOrchestrator
			if fetchesPerFile {
				require.EqualValues(t, 1, fileProperties.Load())
				require.Equal(t, "text/plain", file.ContentType)
				require.Equal(t, "identity", file.ContentEncoding)
				require.Equal(t, "en", file.ContentLanguage)
				require.Equal(t, "inline", file.ContentDisposition)
				require.Equal(t, "no-cache", file.CacheControl)
				require.Contains(t, file.Metadata, "Owner") // HTTP canonicalizes the x-ms-meta-owner header name
				require.False(t, file.LastModifiedTime.IsZero())
				return
			}
			require.EqualValues(t, 0, fileProperties.Load(), "no per-file property request without a request for them")
			require.Empty(t, file.ContentType)
			require.Empty(t, file.Metadata)
			if getProperties {
				// The sync orchestrator lists extended info instead of calling Get File Properties per file.
				require.False(t, file.LastModifiedTime.IsZero())
			} else {
				require.True(t, file.LastModifiedTime.IsZero())
			}
		})
	}
}

// TestFileTraverserReportsIncompleteListingEntries keeps one malformed entry from crashing the scan or hiding the rest.
func TestFileTraverserReportsIncompleteListingEntries(t *testing.T) {
	ch := make(chan TraverserErrorItemInfo, 4)
	counted := 0
	traversal, _ := newFilesTestTraverser(t,
		filesTestListing(`<File><Name>no-properties.txt</Name></File>`, filesTestFile("a.txt")),
		InitResourceTraverserOptions{
			Recursive: true, ErrorChannel: ch,
			IncrementScanError: func(common.EntityType) { counted++ },
		})
	files := collectFilesTestObjects(t, traversal)

	require.Len(t, files, 1)
	require.Equal(t, "a.txt", files[0].Name)
	require.Equal(t, 1, counted)
	require.Len(t, ch, 1)
	message := (<-ch).ErrorMessage().Error()
	require.Contains(t, message, `skipped file "no-properties.txt" in the Azure Files listing of share/`)
	require.Contains(t, message, "omitted its name and/or size")
}
