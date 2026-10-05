package jobsAdmin

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

func setupJobRemovalFolders(t *testing.T) (string, string) {
	t.Helper()
	root := filepath.Join(".", "m5-removal-"+common.NewJobID().String())
	plans, logs := filepath.Join(root, "plans"), filepath.Join(root, "logs")
	require.NoError(t, os.MkdirAll(plans, 0700))
	require.NoError(t, os.Mkdir(logs, 0700))
	oldPlans, oldLogs := common.AzcopyJobPlanFolder, common.LogPathFolder
	common.AzcopyJobPlanFolder, common.LogPathFolder = plans, logs
	t.Cleanup(func() {
		common.AzcopyJobPlanFolder, common.LogPathFolder = oldPlans, oldLogs
		require.NoError(t, os.RemoveAll(root))
	})
	return plans, logs
}

func writeRemovalTestFile(t *testing.T, dir, name string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte("closed test fixture"), 0600))
	return path
}

func TestM5RemoveSingleJobFilesRemovesOnlyExactInodeStore(t *testing.T) {
	plans, logs := setupJobRemovalFolders(t)
	id, other := common.NewJobID(), common.NewJobID()
	storeName := fmt.Sprintf("inodeStore-%s.txt", id)
	removed := []string{
		writeRemovalTestFile(t, plans, storeName),
		writeRemovalTestFile(t, plans, fmt.Sprintf("%s--00000.steV22", id)),
		writeRemovalTestFile(t, logs, id.String()+".log"),
	}
	retained := []string{
		writeRemovalTestFile(t, plans, fmt.Sprintf("inodeStore-%s.txt", other)),
		writeRemovalTestFile(t, plans, storeName+".bak"),
		writeRemovalTestFile(t, plans, storeName+".steV22"),
		writeRemovalTestFile(t, logs, storeName+".log"),
		writeRemovalTestFile(t, plans, "prefix-"+storeName),
	}
	count, err := RemoveSingleJobFiles(id)
	require.NoError(t, err)
	require.Equal(t, 3, count)
	for _, path := range removed {
		require.NoFileExists(t, path)
	}
	for _, path := range retained {
		require.FileExists(t, path)
	}
}

func TestM5RemoveSingleOrphanInodeStore(t *testing.T) {
	plans, _ := setupJobRemovalFolders(t)
	id := common.NewJobID()
	store := writeRemovalTestFile(t, plans, fmt.Sprintf("inodeStore-%s.txt", id))
	count, err := RemoveSingleJobFiles(id)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.NoFileExists(t, store)
}

func TestM5DeleteAllPreservesCurrentStoreAndInvalidNames(t *testing.T) {
	plans, logs := setupJobRemovalFolders(t)
	current, other := common.NewJobID(), common.NewJobID()
	currentStore := fmt.Sprintf("inodeStore-%s.txt", current)
	otherStore := fmt.Sprintf("inodeStore-%s.txt", other)
	retained := []string{
		writeRemovalTestFile(t, plans, currentStore),
		writeRemovalTestFile(t, plans, otherStore+".bak"),
		writeRemovalTestFile(t, plans, otherStore+".steV22"),
		writeRemovalTestFile(t, plans, "inodeStore-not-a-job-id.txt"),
		writeRemovalTestFile(t, plans, "prefix-"+otherStore),
		writeRemovalTestFile(t, logs, otherStore+".log"),
		writeRemovalTestFile(t, logs, current.String()+".log"),
	}
	directory := filepath.Join(plans, fmt.Sprintf("inodeStore-%s.txt", common.NewJobID()))
	require.NoError(t, os.Mkdir(directory, 0700))
	removed := []string{
		writeRemovalTestFile(t, plans, otherStore),
		writeRemovalTestFile(t, plans, fmt.Sprintf("%s--00000.steV21", other)),
		writeRemovalTestFile(t, logs, other.String()+".log"),
	}
	count, err := DeleteAllJobFilesExceptCurrent(current)
	require.NoError(t, err)
	require.Equal(t, 3, count)
	for _, path := range removed {
		require.NoFileExists(t, path)
	}
	for _, path := range retained {
		require.FileExists(t, path)
	}
	require.DirExists(t, directory)
}
