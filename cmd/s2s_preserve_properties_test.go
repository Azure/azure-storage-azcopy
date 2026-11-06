package cmd

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

// The library rejects an explicitly enabled S2SPreserveProperties outside S2S, so the adapter must only
// pass an enabled value on when the user asked for it. The flag's default must not count.
func TestCookedCopyAdapterForwardsS2SPreservePropertiesOnlyWhenChosen(t *testing.T) {
	raw := rawCopyCmdArgs{
		src: "source.txt", dst: "https://destination.blob.core.windows.net/container",
		fromTo: common.EFromTo.LocalBlob().String(),
	}
	raw.setMandatoryDefaults()
	cooked, err := raw.toOptions()
	require.NoError(t, err)
	cooked.jobID = common.NewJobID()

	for _, test := range []struct {
		name     string
		value    boolDefaultTrue
		expected *bool
	}{
		{"default", boolDefaultTrue{value: true}, nil},
		{"explicitly disabled", boolDefaultTrue{value: false, isManuallySet: true}, to.Ptr(false)},
		{"disabled by the Mover setter", boolDefaultTrue{value: false}, to.Ptr(false)},
		{"explicitly enabled", boolDefaultTrue{value: true, isManuallySet: true}, to.Ptr(true)},
	} {
		t.Run(test.name, func(t *testing.T) {
			cooked.s2sPreserveProperties = test.value
			options, err := cooked.ToCopyOptions()
			require.NoError(t, err)
			require.Equal(t, test.expected, options.S2SPreserveProperties)
		})
	}
}
