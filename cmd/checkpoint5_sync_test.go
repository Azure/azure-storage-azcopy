package cmd

import (
	"regexp"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

func TestCheckpoint5LegacySyncReportsHardlinks(t *testing.T) {
	args := cookedSyncCmdArgs{}
	summary := common.ListJobSummaryResponse{
		HardlinksTransferCount: 7, HardlinksConvertedCount: 8,
		SkippedHardlinkCount: 2, HardlinksSkipped: 3,
	}
	for _, output := range []string{
		args.GetDefaultOutputMessage(summary, "", time.Second),
		args.GetElaborateOutputMessage(summary, "", time.Second),
	} {
		require.Regexp(t, regexp.MustCompile(`Number of Hardlinks Transferred:[. ]*7`), output)
		require.Regexp(t, regexp.MustCompile(`Number of Hardlinks Converted:[. ]*8`), output)
		require.Regexp(t, regexp.MustCompile(`Number of Hardlinks Skipped:[. ]*5`), output)
	}
}
