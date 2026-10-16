package common

import (
	"os"
	"path/filepath"
	"testing"
)

// TestHiddenFileDataAdapter_NestedDirectories tests that hash metadata files can be created
// for files in nested directory structures, reproducing the issue described in GitHub issue #3017
func TestHiddenFileDataAdapter_NestedDirectories(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "azcopy_test_nested_dirs")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create adapter with temp directory as base path
	adapter := &HiddenFileDataAdapter{
		hashBasePath: tempDir,
		dataBasePath: tempDir,
	}

	// Test case: nested directory structure like lib/MIME/Type.pm
	// This should reproduce the issue where lib directory doesn't exist
	nestedFilePath := "lib/MIME/Type.pm"
	hashData := &SyncHashData{
		Mode: ESyncHashType.MD5(),
		Data: "dummy_hash_value",
	}

	// This should not fail - it should create the necessary parent directories
	err = adapter.SetHashData(nestedFilePath, hashData)
	if err != nil {
		t.Errorf("SetHashData failed for nested directory structure: %v", err)
	}

	// Verify that the hash file was created in the correct location
	expectedHashFile := filepath.Join(tempDir, "lib", "MIME", ".Type.pm"+AzCopyHashDataStream)
	if _, err := os.Stat(expectedHashFile); os.IsNotExist(err) {
		t.Errorf("Hash metadata file was not created at expected location: %s", expectedHashFile)
	}

	// Test that we can also read the data back
	retrievedData, err := adapter.GetHashData(nestedFilePath)
	if err != nil {
		t.Errorf("GetHashData failed: %v", err)
	}

	if retrievedData == nil {
		t.Error("Retrieved hash data is nil")
	} else if retrievedData.Data != hashData.Data {
		t.Errorf("Retrieved hash data doesn't match. Expected: %s, Got: %s", hashData.Data, retrievedData.Data)
	}
}

// TestHiddenFileDataAdapter_IssueScenario tests the exact scenario described in GitHub issue #3017
// Directory structure:
// .
// └── abc
//     ├── home
//     │   ├── dfchk
//     │   ├── fix_eback.sh
//     │   └── static-ports.ini
//     ├── lib
//     │   └── MIME
//     │       └── Type.pm
//     └── VERSIE
func TestHiddenFileDataAdapter_IssueScenario(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "azcopy_test_issue_scenario")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create adapter with temp directory as base path
	adapter := &HiddenFileDataAdapter{
		hashBasePath: tempDir,
		dataBasePath: tempDir,
	}

	// Test the exact scenario from the issue: lib/MIME/Type.pm
	// where lib folder has only MIME as subfolder and no file
	libMimeFilePath := "lib/MIME/Type.pm"
	hashData := &SyncHashData{
		Mode: ESyncHashType.MD5(),
		Data: "test_hash_for_type_pm",
	}

	// This should not fail with the fix in place
	err = adapter.SetHashData(libMimeFilePath, hashData)
	if err != nil {
		t.Errorf("SetHashData failed for lib/MIME/Type.pm structure: %v", err)
	}

	// Verify that the hash file was created in the correct location
	expectedHashFile := filepath.Join(tempDir, "lib", "MIME", ".Type.pm"+AzCopyHashDataStream)
	if _, err := os.Stat(expectedHashFile); os.IsNotExist(err) {
		t.Errorf("Hash metadata file was not created at expected location: %s", expectedHashFile)
	}

	// Test that we can also read the data back
	retrievedData, err := adapter.GetHashData(libMimeFilePath)
	if err != nil {
		t.Errorf("GetHashData failed: %v", err)
	}

	if retrievedData == nil {
		t.Error("Retrieved hash data is nil")
	} else if retrievedData.Data != hashData.Data {
		t.Errorf("Retrieved hash data doesn't match. Expected: %s, Got: %s", hashData.Data, retrievedData.Data)
	}

	// Also test other files from the same directory structure to ensure they work
	testFiles := []string{
		"home/dfchk",
		"home/fix_eback.sh", 
		"home/static-ports.ini",
		"VERSIE",
	}

	for _, filePath := range testFiles {
		hashData := &SyncHashData{
			Mode: ESyncHashType.MD5(),
			Data: "hash_for_" + filepath.Base(filePath),
		}

		err := adapter.SetHashData(filePath, hashData)
		if err != nil {
			t.Errorf("SetHashData failed for %s: %v", filePath, err)
		}
	}
}