package cmd

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/cred"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestM1MoverSymlinkFollowingRemainsAvailable(t *testing.T) {
	oldLCM, oldLogger := glcm, azcopyScanningLogger
	oldLogPath, oldLogLevel := common.LogPathFolder, LogLevel
	glcm = &mockedLifecycleManager{}
	common.LogPathFolder = t.TempDir()
	LogLevel = common.LogInfo
	t.Cleanup(func() {
		glcm, azcopyScanningLogger = oldLCM, oldLogger
		common.LogPathFolder, LogLevel = oldLogPath, oldLogLevel
	})

	for _, tt := range []struct {
		fromTo common.FromTo
		dst    string
	}{
		{common.EFromTo.LocalFile(), "https://example.file.core.windows.net/share"},
		{common.EFromTo.LocalBlob(), "https://example.blob.core.windows.net/container"},
	} {
		t.Run(tt.fromTo.String(), func(t *testing.T) {
			source := t.TempDir()
			args := RawMoverSyncCmdArgs{
				Src:                  source,
				Dst:                  tt.dst,
				FromTo:               tt.fromTo.String(),
				FollowSymlinks:       true,
				DeleteDestination:    "false",
				Md5ValidationOption:  common.DefaultHashValidationOption.String(),
				CompareHash:          common.ESyncHashType.None().String(),
				LocalHashStorageMode: common.EHashStorageMode.Default().String(),
				Hardlinks:            common.DefaultHardlinkHandlingType.String(),
			}
			cooked, err := CookRawSyncCmdArgs(args)
			if azcopyScanningLogger != oldLogger {
				azcopyScanningLogger.CloseLog()
			}
			require.NoError(t, err)
			assert.True(t, cooked.symlinkHandling.Follow())

			rawCLI := getDefaultSyncRawInput(source, tt.dst)
			rawCLI.fromTo = tt.fromTo.String()
			rawCLI.followSymlinks = true
			cliOptions, err := rawCLI.toOptions()
			require.NoError(t, err)
			require.ErrorContains(t, cliOptions.validate(), "not applicable for sync")
		})
	}
}

func TestM1CrossProtocolPreservation(t *testing.T) {
	oldLCM := glcm
	glcm = &mockedLifecycleManager{}
	t.Cleanup(func() { glcm = oldLCM })

	for _, fromTo := range []common.FromTo{common.EFromTo.FileNFSFileSMB(), common.EFromTo.FileSMBFileNFS()} {
		t.Run(fromTo.String(), func(t *testing.T) {
			parsed, err := ValidateFromTo("", "", fromTo.String())
			require.NoError(t, err)
			require.Equal(t, fromTo, parsed)
			require.True(t, fromTo.IsNFS())
			require.True(t, areBothLocationsNFSAware(fromTo))
			require.NoError(t, validatePreserveNFSPropertyOption(true, fromTo, PreserveInfoFlag))
			require.ErrorContains(t, validatePreserveNFSPropertyOption(true, fromTo, PreservePermissionsFlag), "cross-protocol")

			raw := getDefaultSyncRawInput("https://source.file.core.windows.net/share", "https://destination.file.core.windows.net/share")
			raw.fromTo = fromTo.String()
			raw.preserveInfo = true
			raw.hardlinks = common.DefaultHardlinkHandlingType.String()
			cooked, err := raw.toOptions()
			require.NoError(t, err)
			require.Equal(t, fromTo, cooked.fromTo)
			require.True(t, cooked.preserveInfo)
			require.NoError(t, cooked.validate())
		})
	}
}

func TestM1NFSDoesNotChangeSMBDefaults(t *testing.T) {
	require.True(t, GetPreserveInfoFlagDefault(nil, common.EFromTo.FileNFSFileNFS()))
	require.Equal(t, runtime.GOOS == "windows", GetPreserveInfoFlagDefault(nil, common.EFromTo.FileFile()))
	require.False(t, common.EFromTo.FileFile().IsNFS())
}

func TestM1PreservedMoverTraverserOptions(t *testing.T) {
	t.Setenv("AZCOPY_AZFILES_STATS_POLL", "false")
	fromTo := common.EFromTo.LocalFileNFS()
	root := t.TempDir()
	opts := InitResourceTraverserOptions{
		FromTo:     fromTo,
		Credential: &cred.CredentialInfo{},
	}
	traverser, err := InitResourceTraverser(common.ResourceString{Value: root}, common.ELocation.Local(), context.Background(), opts)
	require.NoError(t, err)
	local, ok := traverser.(*localTraverser)
	require.True(t, ok)
	assert.Equal(t, fromTo, local.fromTo)
	assert.Equal(t, UseSyncOrchestrator, local.includeDirectoryOrPrefix)

	smb := newFileTraverser("https://example.file.core.windows.net/share", nil, context.Background(), InitResourceTraverserOptions{
		FromTo:                  common.EFromTo.FileFile(),
		GetPropertiesInFrontend: true,
		IsSyncDestination:       true,
	})
	assert.Equal(t, UseSyncOrchestrator, smb.includeExtendedInfo)
	nfs := newFileTraverser("https://example.file.core.windows.net/share", nil, context.Background(), InitResourceTraverserOptions{
		FromTo:                  common.EFromTo.FileNFSFileNFS(),
		GetPropertiesInFrontend: true,
		IsSyncDestination:       true,
	})
	assert.False(t, nfs.includeExtendedInfo)
}

func TestM1SyncComparisonUsesJobProtocol(t *testing.T) {
	now := time.Unix(1700000000, 0)
	source := StoredObject{entityType: common.EEntityType.File(), size: 1, lastWriteTime: now}
	destination := source
	destination.lastWriteTime = now.Add(100 * time.Nanosecond)
	for _, fromTo := range []common.FromTo{common.EFromTo.LocalFileNFS(), common.EFromTo.LocalFile()} {
		t.Run(fromTo.String(), func(t *testing.T) {
			t.Parallel()
			comparator := &syncDestinationComparator{orchestratorOptions: &SyncOrchestratorOptions{fromTo: fromTo}}
			dataChanged, metadataChanged := comparator.compareSourceAndDestinationObject(source, destination)
			assert.Equal(t, !fromTo.IsNFS(), dataChanged)
			assert.Equal(t, !fromTo.IsNFS(), metadataChanged)
		})
	}
}
