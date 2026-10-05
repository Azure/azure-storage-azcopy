package azcopy

import (
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
	"github.com/stretchr/testify/require"
)

func TestM4CopyWildcardPreservesNamelessDirectories(t *testing.T) {
	for _, test := range []struct {
		name, source, expected string
		strip                  bool
	}{
		{"ordinary", "/container/dir/*", "/container/dir", true},
		{"nameless", "/container/dir//*", "/container/dir//", true},
		{"nested nameless", "/container/dir///*", "/container/dir///", true},
		{"container nameless", "/container//*", "/container//", true},
		{"literal star", "/container/dir/%2A", "/container/dir/%2A", false},
		{"encoded slash", "/container/%2F/*", "/container/%2F", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			const base = "https://account.blob.core.windows.net"
			result, strip, err := StripTrailingWildcardOnRemoteSource(base+test.source, common.ELocation.Blob())
			require.NoError(t, err)
			require.Equal(t, base+test.expected, result)
			require.Equal(t, test.strip, strip)
		})
	}
	_, _, err := StripTrailingWildcardOnRemoteSource("https://account.blob.core.windows.net/container/bad*//*", common.ELocation.Blob())
	require.Error(t, err)
}

func TestM4CopyPathPreservesLeadingAndRepeatedSlashes(t *testing.T) {
	options := CopyPathOptions{
		Source:      common.ResourceString{Value: "https://source.blob.core.windows.net/container"},
		Destination: common.ResourceString{Value: "https://destination.blob.core.windows.net/container"},
		FromTo:      common.EFromTo.BlobBlob(),
	}
	object := traverser.StoredObject{RelativePath: "/prefix//file", EntityType: common.EEntityType.File()}
	require.Equal(t, "//prefix//file", options.MakeEscapedRelativePath(true, true, object))
	require.Equal(t, "//prefix//file", options.MakeEscapedRelativePath(false, true, object))
}

func TestM4CopyPOSIXStylePreservesLegacyValidationAPI(t *testing.T) {
	var legacy func(common.FromTo, common.PreservePermissionsOption, bool, bool, ...common.HardlinkHandlingType) error = PerformSMBSpecificValidation
	fromTo := common.EFromTo.BlobBlob()
	permissions := common.EPreservePermissionsOption.None()
	hardlinks := []common.HardlinkHandlingType{common.EHardlinkHandlingType.Skip()}
	require.NoError(t, legacy(fromTo, permissions, false, true, hardlinks...))
	require.NoError(t, PerformSMBSpecificValidationWithPOSIXStyle(
		fromTo, permissions, false, true, common.AMLFSPosixPropertiesStyle, hardlinks...))
	require.ErrorContains(t, PerformSMBSpecificValidationWithPOSIXStyle(
		fromTo, permissions, false, false, common.AMLFSPosixPropertiesStyle), POSIXStyleMisuse)
	require.NoError(t, PerformSMBSpecificValidationWithPOSIXStyle(
		fromTo, permissions, false, false, common.StandardPosixPropertiesStyle))
	require.ErrorContains(t, PerformSMBSpecificValidationWithPOSIXStyle(
		fromTo, permissions, false, true, common.PosixPropertiesStyle(255)), "unsupported POSIX properties style")
	require.ErrorContains(t, legacy(fromTo, permissions, false, true, common.HardlinkHandlingType(255)), "unsupported --hardlinks")
}

func TestM4CookedCopyOptionsCarryAMLFSStyle(t *testing.T) {
	cooked := CookedTransferOptions{
		source:      common.ResourceString{Value: "https://source.blob.core.windows.net/container"},
		destination: common.ResourceString{Value: "https://destination.blob.core.windows.net/container"},
		fromTo:      common.EFromTo.BlobBlob(),
	}
	err := cooked.applyDefaultsAndInferOptions(CopyOptions{
		FromTo:                  common.EFromTo.BlobBlob(),
		PreservePosixProperties: true,
		PosixPropertiesStyle:    common.AMLFSPosixPropertiesStyle,
	})
	require.NoError(t, err)
	require.True(t, cooked.preservePosixProperties)
	require.Equal(t, common.AMLFSPosixPropertiesStyle, cooked.posixPropertiesStyle)
}

func TestM4CopyListOfFilesAllowsIncludePatternsNotIncludePaths(t *testing.T) {
	const source = "https://source.blob.core.windows.net/container"
	const destination = "https://destination.blob.core.windows.net/container"
	options := CopyOptions{FromTo: common.EFromTo.BlobBlob(), IncludePatterns: []string{"*.txt"}}
	options.listOfFiles = ".m4-copy-missing-list-" + common.NewJobID().String()
	_, err := newCookedCopyOptions(source, destination, options)
	require.ErrorContains(t, err, "cannot open")
	options.IncludePaths = []string{"directory"}
	_, err = newCookedCopyOptions(source, destination, options)
	require.EqualError(t, err, "cannot combine list of files and include path")
}
