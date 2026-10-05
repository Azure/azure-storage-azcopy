package traverser

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/cred"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestM1PreservedMoverTraverserOptions(t *testing.T) {
	t.Setenv("AZCOPY_AZFILES_STATS_POLL", "false")
	fromTo := common.EFromTo.LocalFileNFS()
	root := t.TempDir()
	opts := InitResourceTraverserOptions{
		FromTo: fromTo, Credential: &cred.CredentialInfo{},
	}
	traverser, err := InitResourceTraverser(common.ResourceString{Value: root}, common.ELocation.Local(), context.Background(), opts)
	require.NoError(t, err)
	local, ok := traverser.(*localTraverser)
	require.True(t, ok)
	assert.Equal(t, fromTo, local.fromTo)
	assert.Equal(t, UseSyncOrchestrator, local.includeDirectoryOrPrefix)

	smb := NewFileTraverser("https://example.file.core.windows.net/share", nil, context.Background(), InitResourceTraverserOptions{
		FromTo: common.EFromTo.FileFile(), GetPropertiesInFrontend: true, IsSyncDestination: true,
	})
	assert.Equal(t, UseSyncOrchestrator, smb.includeExtendedInfo)
	nfs := NewFileTraverser("https://example.file.core.windows.net/share", nil, context.Background(), InitResourceTraverserOptions{
		FromTo: common.EFromTo.FileNFSFileNFS(), GetPropertiesInFrontend: true, IsSyncDestination: true,
	})
	assert.False(t, nfs.includeExtendedInfo)
}

func TestM1SyncComparisonUsesJobProtocol(t *testing.T) {
	now := time.Unix(1700000000, 0)
	source := StoredObject{EntityType: common.EEntityType.File(), Size: 1, LastWriteTime: now}
	destination := source
	destination.LastWriteTime = now.Add(100 * time.Nanosecond)
	for _, fromTo := range []common.FromTo{common.EFromTo.LocalFileNFS(), common.EFromTo.LocalFile()} {
		t.Run(fromTo.String(), func(t *testing.T) {
			t.Parallel()
			comparator := &SyncDestinationComparator{orchestratorOptions: &SyncOrchestratorOptions{fromTo: fromTo}}
			dataChanged, metadataChanged := comparator.CompareSourceAndDestinationObject(source, destination)
			assert.Equal(t, !fromTo.IsNFS(), dataChanged)
			assert.Equal(t, !fromTo.IsNFS(), metadataChanged)
		})
	}
}

func TestM2TraversalReusesSharedTransport(t *testing.T) {
	first := CreateClientOptions(nil, nil, nil)
	second := CreateClientOptions(nil, nil, nil)
	require.Same(t, common.GetGlobalHTTPClient(nil), first.Transport)
	require.Same(t, first.Transport, second.Transport)
	require.NotSame(t, first.PerRetryPolicies[len(first.PerRetryPolicies)-1], second.PerRetryPolicies[len(second.PerRetryPolicies)-1],
		"transport reuse must not share per-job credential policies")
}

func TestM2NonrecursiveSingleFileIgnoresRootPropertySelection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "file.txt")
	require.NoError(t, os.WriteFile(path, []byte("data"), 0600))
	for _, includeRoot := range []bool{false, true} {
		traverser, err := NewLocalTraverser(path, context.Background(), InitResourceTraverserOptions{
			Recursive: false, IncludeRoot: includeRoot,
		})
		require.NoError(t, err)
		var objects []StoredObject
		err = traverser.Traverse(nil, func(object StoredObject) error {
			objects = append(objects, object)
			return nil
		}, nil)
		require.NoError(t, err)
		require.Len(t, objects, 1)
		require.Equal(t, common.EEntityType.File(), objects[0].EntityType)
		require.Equal(t, "file.txt", objects[0].Name)
		require.Equal(t, int64(4), objects[0].Size)
	}
}
