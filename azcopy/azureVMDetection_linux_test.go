package azcopy

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAzurePublicCloudAssetTagFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "asset-tag")
	require.False(t, azurePublicCloudFromAssetTagFile(path))
	for _, test := range []struct {
		value string
		want  bool
	}{
		{azurePublicCloudAssetTag + "\n", true},
		{"", false},
		{"Virtual Machine\n", false},
		{strings.Repeat(" ", 129) + azurePublicCloudAssetTag, false},
	} {
		require.NoError(t, os.WriteFile(path, []byte(test.value), 0600))
		require.Equal(t, test.want, azurePublicCloudFromAssetTagFile(path))
	}
	require.False(t, azurePublicCloudFromAssetTagFile(filepath.Dir(path)))
}
