package azcopy

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
	"github.com/stretchr/testify/require"
)

func TestCopyS2SPreservePropertiesDefaultsToTrueWithoutCountingAsSet(t *testing.T) {
	cooked := CookedTransferOptions{
		source:      common.ResourceString{Value: "https://source.blob.core.windows.net/container"},
		destination: common.ResourceString{Value: "https://destination.blob.core.windows.net/container"},
		fromTo:      common.EFromTo.BlobBlob(),
	}
	require.NoError(t, cooked.applyDefaultsAndInferOptions(CopyOptions{FromTo: common.EFromTo.BlobBlob()}))
	require.True(t, cooked.s2sPreserveProperties.Get())
	_, isSet := cooked.s2sPreserveProperties.GetIfSet()
	require.False(t, isSet, "an unset option must not be validated as if the caller had chosen it")

	for _, explicit := range []bool{true, false} {
		cooked := CookedTransferOptions{
			source:      common.ResourceString{Value: "https://source.blob.core.windows.net/container"},
			destination: common.ResourceString{Value: "https://destination.blob.core.windows.net/container"},
			fromTo:      common.EFromTo.BlobBlob(),
		}
		require.NoError(t, cooked.applyDefaultsAndInferOptions(CopyOptions{FromTo: common.EFromTo.BlobBlob(), S2SPreserveProperties: to.Ptr(explicit)}))
		require.Equal(t, explicit, cooked.s2sPreserveProperties.Get())
		value, isSet := cooked.s2sPreserveProperties.GetIfSet()
		require.True(t, isSet)
		require.Equal(t, explicit, value)
	}
}

// Where properties are read follows from S2SPreserveProperties: in the backend by default, in the frontend if the
// backend can't do it or something needs them during enumeration, and nowhere if they aren't preserved.
func TestCopyS2SPreservePropertiesDrivesWhereSourcePropertiesAreRead(t *testing.T) {
	const blobURL = "https://source.blob.core.windows.net/container"
	const fileURL = "https://source.file.core.windows.net/share"
	const destURL = "https://destination.blob.core.windows.net/container"
	includeAfter := time.Now()

	for _, test := range []struct {
		name             string
		fromTo           common.FromTo
		source           string
		options          CopyOptions
		wantInFrontend   bool
		wantInBackend    bool
		wantPreserveFlag bool
	}{
		{"blob default", common.EFromTo.BlobBlob(), blobURL, CopyOptions{}, false, true, true},
		{"blob not preserved", common.EFromTo.BlobBlob(), blobURL, CopyOptions{S2SPreserveProperties: to.Ptr(false)}, false, false, false},
		{"blob explicitly preserved", common.EFromTo.BlobBlob(), blobURL, CopyOptions{S2SPreserveProperties: to.Ptr(true)}, false, true, true},
		{"backend disabled falls back to frontend", common.EFromTo.BlobBlob(), blobURL, CopyOptions{s2SGetPropertiesInBackend: to.Ptr(false)}, true, false, true},
		{"backend disabled but nothing preserved", common.EFromTo.BlobBlob(), blobURL, CopyOptions{S2SPreserveProperties: to.Ptr(false), s2SGetPropertiesInBackend: to.Ptr(false)}, false, false, false},
		{"file default", common.EFromTo.FileBlob(), fileURL, CopyOptions{}, false, true, true},
		{"file with source change detection needs LMTs up front", common.EFromTo.FileBlob(), fileURL, CopyOptions{S2SDetectSourceChanged: true}, true, false, true},
		{"file with include-after needs LMTs up front", common.EFromTo.FileBlob(), fileURL, CopyOptions{IncludeAfter: &includeAfter}, true, false, true},
		{"file not preserved but source change detection", common.EFromTo.FileBlob(), fileURL, CopyOptions{S2SPreserveProperties: to.Ptr(false), S2SDetectSourceChanged: true}, true, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			cooked := CookedTransferOptions{
				source:      common.ResourceString{Value: test.source},
				destination: common.ResourceString{Value: destURL},
				fromTo:      test.fromTo,
			}
			test.options.FromTo = test.fromTo
			require.NoError(t, cooked.applyDefaultsAndInferOptions(test.options))
			require.Equal(t, test.wantPreserveFlag, cooked.s2sPreserveProperties.Get())
			require.Equal(t, test.wantInFrontend, cooked.getPropertiesInFrontend, "frontend")
			require.Equal(t, test.wantInBackend, cooked.s2sGetPropertiesInBackend, "backend")
		})
	}
}

// Only a caller that explicitly asks for S2SPreserveProperties on a non-S2S transfer is told it is unsupported.
func TestCopyS2SPreservePropertiesIsRejectedOnlyWhenExplicitlyEnabledOutsideS2S(t *testing.T) {
	local := filepath.ToSlash(t.TempDir())
	const blob = "https://account.blob.core.windows.net/container"
	const dfs = "https://account.dfs.core.windows.net/filesystem"
	const file = "https://account.file.core.windows.net/share"

	for _, test := range []struct {
		name     string
		fromTo   common.FromTo
		src, dst string
	}{
		{"upload to blob", common.EFromTo.LocalBlob(), local, blob},
		{"upload to blobfs", common.EFromTo.LocalBlobFS(), local, dfs},
		{"upload to file", common.EFromTo.LocalFile(), local, file},
		{"download from blob", common.EFromTo.BlobLocal(), blob, local},
		{"download from file", common.EFromTo.FileLocal(), file, local},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Mover builds require an ErrorChannel.
			opts := CopyOptions{FromTo: test.fromTo, ErrorChannel: make(chan traverser.TraverserErrorItemInfo)}

			opts.S2SPreserveProperties = to.Ptr(true)
			_, err := newCookedCopyOptions(test.src, test.dst, opts)
			require.ErrorContains(t, err, "s2s-preserve-properties is not supported")

			opts.S2SPreserveProperties = to.Ptr(false)
			_, err = newCookedCopyOptions(test.src, test.dst, opts)
			require.NoError(t, err)

			opts.S2SPreserveProperties = nil
			_, err = newCookedCopyOptions(test.src, test.dst, opts)
			require.NoError(t, err)
		})
	}

	for _, explicit := range []*bool{nil, to.Ptr(true), to.Ptr(false)} {
		_, err := newCookedCopyOptions(blob, "https://destination.blob.core.windows.net/container", CopyOptions{
			FromTo: common.EFromTo.BlobBlob(), S2SPreserveProperties: explicit, ErrorChannel: make(chan traverser.TraverserErrorItemInfo),
		})
		require.NoError(t, err, "S2S accepts every setting")
	}
}
