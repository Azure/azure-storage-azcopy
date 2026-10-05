package cmd

import (
	"runtime"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestM1MoverSymlinkFollowingRemainsAvailable(t *testing.T) {
	oldLCM, oldLogger := glcm, common.AzcopyScanningLogger
	oldLogPath, oldLogLevel := common.LogPathFolder, LogLevel
	glcm = &mockedLifecycleManager{}
	common.LogPathFolder = t.TempDir()
	LogLevel = common.LogInfo
	t.Cleanup(func() {
		glcm, common.AzcopyScanningLogger = oldLCM, oldLogger
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
			if common.AzcopyScanningLogger != oldLogger {
				common.AzcopyScanningLogger.CloseLog()
			}
			require.NoError(t, err)
			assert.True(t, cooked.symlinkHandling.Follow())

			rawCLI := getDefaultSyncRawInput(source, tt.dst)
			rawCLI.fromTo = tt.fromTo.String()
			rawCLI.followSymlinks = true
			cliOptions, err := rawCLI.toCookedOptions()
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
			cooked, err := raw.toCookedOptions()
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
