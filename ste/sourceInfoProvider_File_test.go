package ste

import (
	"context"
	"crypto/md5"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	fileservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fileSnapshotTransferManager struct {
	IJobPartTransferMgr
	info         *TransferInfo
	sourceClient *common.ServiceClient
}

func (m *fileSnapshotTransferManager) Info() *TransferInfo {
	return m.info
}

func (m *fileSnapshotTransferManager) Context() context.Context {
	return context.Background()
}

func (m *fileSnapshotTransferManager) SrcServiceClient() *common.ServiceClient {
	return m.sourceClient
}

func TestFileSourceInfoProviderReadsSelectedSnapshot(t *testing.T) {
	t.Setenv(enum.EEnvironmentVariable.EnableAzFilesProactiveStats().Name, "")
	const snapshot = "2026-10-27T09:30:47.0000000Z"
	const lastModified = "Tue, 27 Oct 2026 09:30:47 GMT"
	for _, fromTo := range []common.FromTo{common.EFromTo.FileFile(), common.EFromTo.FileNFSFileNFS()} {
		for _, selection := range []string{"url", "transfer-info", "live"} {
			for _, entity := range []common.EntityType{common.EEntityType.File(), common.EEntityType.Folder(), common.EEntityType.FileProperties(), common.EEntityType.Hardlink()} {
				t.Run(fmt.Sprintf("%s/%s/%s", fromTo, selection, entity), func(t *testing.T) {
					wantSnapshot := snapshot
					if selection == "live" {
						wantSnapshot = ""
					}
					var requests atomic.Int32
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						requests.Add(1)
						assert.Equal(t, wantSnapshot, r.URL.Query().Get("sharesnapshot"))
						assert.Equal(t, "/share/dir/f", r.URL.Path)
						w.Header().Set("Last-Modified", lastModified)
						w.Header().Set("x-ms-meta-version", "selected")
						w.Header().Set("x-ms-file-last-write-time", snapshot)
						w.Header().Set("x-ms-file-creation-time", snapshot)
						w.Header().Set("x-ms-owner", "1000")
						w.Header().Set("x-ms-group", "1001")
						w.Header().Set("x-ms-mode", "0644")
						if r.Method == http.MethodGet {
							fmt.Fprint(w, "abc")
						} else {
							w.Header().Set("Content-Length", "3")
							w.WriteHeader(http.StatusOK)
						}
					}))
					defer server.Close()
					service, err := fileservice.NewClientWithNoCredential(server.URL, nil)
					require.NoError(t, err)
					info := &TransferInfo{
						Source:                    server.URL + "/share/dir/f",
						SrcContainer:              "share",
						SrcFilePath:               "dir/f",
						EntityType:                entity,
						S2SGetPropertiesInBackend: true,
					}
					switch selection {
					case "url":
						info.Source += "?sharesnapshot=" + snapshot
					case "transfer-info":
						info.SnapshotID = snapshot
					}
					mgr := &fileSnapshotTransferManager{
						info:         info,
						sourceClient: common.NewServiceClient(nil, service, nil),
					}
					sip, err := newFileSourceInfoProvider(mgr)
					require.NoError(t, err)
					provider := sip.(*fileSourceInfoProvider)
					sourceURL, err := provider.PreSignedSourceURL()
					require.NoError(t, err)
					if wantSnapshot != "" {
						assert.Contains(t, sourceURL, "sharesnapshot=")
					} else {
						assert.NotContains(t, sourceURL, "sharesnapshot=")
					}
					props, err := provider.Properties()
					require.NoError(t, err)
					require.Contains(t, props.SrcMetadata, "Version")
					assert.Equal(t, "selected", *props.SrcMetadata["Version"])
					nfsProps, err := provider.GetNFSProperties()
					require.NoError(t, err)
					assert.Equal(t, snapshot, nfsProps.FileLastWriteTime().Format("2006-01-02T15:04:05.0000000Z"))
					_, err = provider.GetNFSPermissions()
					require.NoError(t, err)
					assert.EqualValues(t, 1, requests.Load(), "preservation properties should share one snapshot read")
					if entity == common.EEntityType.File() {
						lmt, err := provider.GetFreshFileLastModifiedTime()
						require.NoError(t, err)
						wantLMT, err := time.Parse(http.TimeFormat, lastModified)
						require.NoError(t, err)
						assert.True(t, wantLMT.Equal(lmt))
						hash, err := provider.GetMD5(0, 3)
						require.NoError(t, err)
						wantHash := md5.Sum([]byte("abc"))
						assert.Equal(t, wantHash[:], hash)
						assert.EqualValues(t, 3, requests.Load())
					}
				})
			}
		}
	}
}
