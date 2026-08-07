package azcopy

import (
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func TestShouldSetConcurrencySettingsToAuto(t *testing.T) {
	tests := []struct {
		name             string
		fromTo           common.FromTo
		concurrencyValue string
		want             bool
	}{
		{
			name:             "file source with explicit value",
			fromTo:           common.EFromTo.FileBlob(),
			concurrencyValue: "2000",
			want:             false,
		},
		{
			name:             "file destination with explicit value",
			fromTo:           common.EFromTo.BlobFile(),
			concurrencyValue: "2000",
			want:             false,
		},
		{
			name:             "file to file with explicit value",
			fromTo:           common.EFromTo.FileFile(),
			concurrencyValue: "2000",
			want:             false,
		},
		{
			name:             "file source without explicit value",
			fromTo:           common.EFromTo.FileBlob(),
			concurrencyValue: "",
			want:             true,
		},
		{
			name:             "file destination without explicit value",
			fromTo:           common.EFromTo.BlobFile(),
			concurrencyValue: "",
			want:             true,
		},
		{
			name:             "file NFS without explicit value",
			fromTo:           common.EFromTo.FileNFSFileNFS(),
			concurrencyValue: "",
			want:             true,
		},
		{
			name:             "file NFS with explicit value",
			fromTo:           common.EFromTo.FileNFSFileNFS(),
			concurrencyValue: "2000",
			want:             false,
		},
		{
			name:             "non-file transfer without explicit value",
			fromTo:           common.EFromTo.BlobBlob(),
			concurrencyValue: "",
			want:             false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := shouldSetConcurrencySettingsToAuto(test.fromTo, test.concurrencyValue); got != test.want {
				t.Fatalf("shouldSetConcurrencySettingsToAuto(%s, %q) = %t, want %t",
					test.fromTo, test.concurrencyValue, got, test.want)
			}
		})
	}
}
