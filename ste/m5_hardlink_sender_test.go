package ste

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

type hardlinkSenderTransport func(*http.Request) (*http.Response, error)

func (f hardlinkSenderTransport) Do(request *http.Request) (*http.Response, error) { return f(request) }

type hardlinkSenderTestMgr struct {
	IJobPartTransferMgr
	info     TransferInfo
	failed   error
	warnings int
}

func (m *hardlinkSenderTestMgr) Info() *TransferInfo                  { return &m.info }
func (*hardlinkSenderTestMgr) FromTo() common.FromTo                  { return common.EFromTo.LocalFileNFS() }
func (*hardlinkSenderTestMgr) ShouldInferContentType() bool           { return false }
func (*hardlinkSenderTestMgr) GetForceIfReadOnly() bool               { return false }
func (m *hardlinkSenderTestMgr) FailActiveSend(_ string, err error)   { m.failed = err }
func (m *hardlinkSenderTestMgr) FailActiveUpload(_ string, err error) { m.failed = err }
func (m *hardlinkSenderTestMgr) Log(level common.LogLevel, _ string) {
	if level == common.LogWarning {
		m.warnings++
	}
}

func TestM5PreserveHardlinkReplacementIsModeScoped(t *testing.T) {
	for _, test := range []struct {
		name         string
		mode         common.HardlinkHandlingType
		linkCount    string
		deleteStatus int
		wantMethods  []string
		wantFailure  bool
		isSync       bool
		isAnchor     bool
	}{
		{"follow", common.EHardlinkHandlingType.Follow(), "2", 202, []string{"PUT"}, false, false, false},
		{"skip", common.EHardlinkHandlingType.Skip(), "2", 202, []string{"PUT"}, false, false, false},
		{"preserve", common.EHardlinkHandlingType.Preserve(), "2", 202, []string{"HEAD", "DELETE", "PUT"}, false, false, false},
		{"unlinked-file", common.EHardlinkHandlingType.Preserve(), "1", 202, []string{"HEAD", "PUT"}, false, false, false},
		{"unlink-failure", common.EHardlinkHandlingType.Preserve(), "2", 403, []string{"HEAD", "DELETE"}, true, false, false},
		{"sync-anchor", common.EHardlinkHandlingType.Preserve(), "2", 202, []string{"PUT"}, false, true, true},
		{"copy-anchor", common.EHardlinkHandlingType.Preserve(), "2", 202, []string{"HEAD", "DELETE", "PUT"}, false, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var methods []string
			transport := hardlinkSenderTransport(func(request *http.Request) (*http.Response, error) {
				methods = append(methods, request.Method)
				status := http.StatusCreated
				headers := make(http.Header)
				switch request.Method {
				case http.MethodHead:
					status = http.StatusOK
					headers.Set("x-ms-file-file-type", string(file.NFSFileTypeRegular))
					headers.Set("x-ms-link-count", test.linkCount)
				case http.MethodDelete:
					status = test.deleteStatus
					if status == http.StatusForbidden {
						headers.Set("x-ms-error-code", "AuthorizationFailure")
					}
				}
				return &http.Response{StatusCode: status, Header: headers, Body: io.NopCloser(strings.NewReader("")), Request: request}, nil
			})
			client, err := file.NewClientWithNoCredential("https://unit.invalid/share/file", &file.ClientOptions{
				ClientOptions: azcore.ClientOptions{Transport: transport, Retry: policy.RetryOptions{MaxRetries: -1}},
			})
			require.NoError(t, err)
			manager := &hardlinkSenderTestMgr{info: TransferInfo{HardlinkHandlingType: test.mode, EntityType: common.EEntityType.File()}}
			manager.info.IsSyncJob = test.isSync
			if test.isAnchor {
				manager.info.EntityType = common.EEntityType.Hardlink()
			}
			sender := &azureFileSenderBase{jptm: manager, ctx: context.Background(), fileOrDirClient: client}
			sender.Prologue(common.PrologueState{})
			require.Equal(t, test.wantMethods, methods)
			require.Equal(t, test.wantFailure, manager.failed != nil)
			if test.name == "preserve" || test.name == "unlink-failure" {
				require.Positive(t, manager.warnings)
			}
		})
	}
}
