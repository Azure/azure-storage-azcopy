package ste

import (
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
)

func TestM5WindowsSourceHardlinkTarget(t *testing.T) {
	stub := &uploadStub{root: `C:\source\root`, fromTo: common.EFromTo.LocalFileNFS()}
	target := computeAnyToRemoteHardlinkTarget(&TransferInfo{
		Source:                 `C:\source\root\sub\link.txt`,
		Destination:            "https://account.file.core.windows.net/share/root/sub/link.txt",
		TargetHardlinkFilePath: "anchors/first.txt",
	}, stub)
	assert.False(t, stub.failCalled)
	assert.Equal(t, "/root/anchors/first.txt", target)
}
