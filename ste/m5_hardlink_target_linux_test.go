package ste

import (
	"net/url"
	"path/filepath"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
)

func TestM5LinuxLocalHardlinkTargetPreservesLiteralNames(t *testing.T) {
	for _, name := range []string{"link name", "link%name", "link#name", `link\name`, `link % #\name`} {
		t.Run(name, func(t *testing.T) {
			root := `/source/root\literal`
			destination := (&url.URL{Scheme: "https", Host: "account.file.core.windows.net", Path: "/share/destination/" + name}).String()
			stub := &uploadStub{root: root, fromTo: common.EFromTo.LocalFileNFS()}
			target := computeAnyToRemoteHardlinkTarget(&TransferInfo{
				Source:                 filepath.Join(root, name),
				Destination:            destination,
				TargetHardlinkFilePath: "anchors/" + name,
			}, stub)
			assert.False(t, stub.failCalled)
			assert.Equal(t, "/destination/anchors/"+name, target)
		})
	}
}
