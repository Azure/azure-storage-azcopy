package cmd

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/cred"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSyncSnapshotDisablesSourceChangeValidation(t *testing.T) {
	t.Setenv(enum.EEnvironmentVariable.EnableAzFilesProactiveStats().Name, "")
	oldCredentialManager := GetCredentialManager
	GetCredentialManager = func() cred.Manager { return cred.NewManager() }
	t.Cleanup(func() { GetCredentialManager = oldCredentialManager })
	oldNFS := common.IsNFSCopy()
	t.Cleanup(func() { common.SetNFSFlag(oldNFS) })
	for _, fromTo := range []common.FromTo{common.EFromTo.FileFile(), common.EFromTo.FileNFSFileNFS()} {
		for _, snapshot := range []string{"", "2026-10-27T09:30:47.0000000Z"} {
			t.Run(fmt.Sprintf("%s/snapshot=%s", fromTo, snapshot), func(t *testing.T) {
				isNFS := fromTo == common.EFromTo.FileNFSFileNFS()
				common.SetNFSFlag(isNFS)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if isNFS {
						w.Header().Set("x-ms-enabled-protocols", "NFS")
					} else {
						w.Header().Set("x-ms-enabled-protocols", "SMB")
					}
					if r.URL.Query().Get("comp") == "list" {
						w.Header().Set("Content-Type", "application/xml")
						fmt.Fprint(w, `<?xml version="1.0" encoding="utf-8"?><EnumerationResults><Entries /><NextMarker /></EnumerationResults>`)
					}
				}))
				defer server.Close()
				baseURL := strings.Replace(server.URL, "127.0.0.1", "localhost", 1)
				source := common.ResourceString{Value: baseURL + "/source", SAS: "sig=test"}
				if snapshot != "" {
					source.ExtraQuery = "sharesnapshot=" + snapshot
				}
				cca := cookedSyncCmdArgs{
					fromTo:            fromTo,
					source:            source,
					destination:       common.ResourceString{Value: baseURL + "/destination", SAS: "sig=test"},
					preserveInfo:      true,
					deleteDestination: common.EDeleteDestination.False(),
				}
				enumerator, err := cca.InitEnumerator(context.Background(), nil)
				require.NoError(t, err)
				assert.Equal(t, snapshot == "", enumerator.ctp.copyJobTemplate.S2SSourceChangeValidation)
				assert.True(t, enumerator.ctp.copyJobTemplate.S2SGetPropertiesInBackend)
				assert.True(t, enumerator.primaryTraverserTemplate.options.GetPropertiesInFrontend)
			})
		}
	}
}
