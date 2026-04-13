package c2ctest

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	datalakedirectory "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/directory"
	datalakefile "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/file"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Build helpers — two binaries with different build tags
// ---------------------------------------------------------------------------

func buildAzCopyMover(t *testing.T) string {
	t.Helper()
	if p := os.Getenv("AZCOPY_MOVER_BINARY"); p != "" {
		t.Logf("Using pre-built mover binary at %s", p)
		return p
	}
	return buildBinary(t, "netgo,smslidingwindow,mover", "mover")
}

func buildAzCopyStandard(t *testing.T) string {
	t.Helper()
	if p := os.Getenv("AZCOPY_STANDARD_BINARY"); p != "" {
		t.Logf("Using pre-built standard binary at %s", p)
		return p
	}
	return buildBinary(t, "netgo", "standard")
}

func buildBinary(t *testing.T, tags, label string) string {
	t.Helper()
	goarch := os.Getenv("GOARCH")
	if goarch == "" {
		goarch = runtime.GOARCH
	}

	binPath := fmt.Sprintf("/tmp/azcopy_acl_test_%s_%s", label, uuid.New().String()[:8])
	cmd := exec.Command("go", "build", "-tags", tags, "-o", binPath, ".")
	cmd.Dir = ".." // project root
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("GOARCH=%s", goarch),
		fmt.Sprintf("GOOS=%s", runtime.GOOS),
	)

	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "Failed to build azcopy (%s): %s", label, string(out))

	t.Cleanup(func() { os.Remove(binPath) })
	t.Logf("Built azcopy (%s) at %s", label, binPath)
	return binPath
}

// ---------------------------------------------------------------------------
// ACL helpers — set / get ACLs on HNS filesystem root and directories
// ---------------------------------------------------------------------------

const (
	// Use non-default ACLs so we can distinguish "was transferred" from "destination default"
	// Default HNS ACL is user::rwx,group::r-x,other::---
	srcACLRoot   = "user::rwx,group::rwx,other::r--"
	srcACLSubdir = "user::rwx,group::rwx,other::-w-"
)

func setRootACL(t *testing.T, account, fsName, acl string) {
	t.Helper()
	ctx := context.Background()
	cred, err := azidentity.NewAzureCLICredential(nil)
	require.NoError(t, err)

	// Use directory client with empty path to target the filesystem root
	dirURL := fmt.Sprintf("https://%s.dfs.core.windows.net/%s/", account, fsName)
	dirClient, err := datalakedirectory.NewClient(dirURL, cred, nil)
	require.NoError(t, err)

	_, err = dirClient.SetAccessControl(ctx, &datalakedirectory.SetAccessControlOptions{ACL: &acl})
	require.NoError(t, err, "SetAccessControl on root failed")
	t.Logf("Set root ACL on %s/%s: %s", account, fsName, acl)
}

func getRootACL(t *testing.T, account, fsName string) string {
	t.Helper()
	ctx := context.Background()
	cred, err := azidentity.NewAzureCLICredential(nil)
	require.NoError(t, err)

	dirURL := fmt.Sprintf("https://%s.dfs.core.windows.net/%s/", account, fsName)
	dirClient, err := datalakedirectory.NewClient(dirURL, cred, nil)
	require.NoError(t, err)

	resp, err := dirClient.GetAccessControl(ctx, nil)
	require.NoError(t, err, "GetAccessControl on root failed")
	require.NotNil(t, resp.ACL)
	return *resp.ACL
}

func setDirACL(t *testing.T, account, fsName, dirPath, acl string) {
	t.Helper()
	ctx := context.Background()
	cred, err := azidentity.NewAzureCLICredential(nil)
	require.NoError(t, err)

	dirURL := fmt.Sprintf("https://%s.dfs.core.windows.net/%s/%s", account, fsName, dirPath)
	dirClient, err := datalakedirectory.NewClient(dirURL, cred, nil)
	require.NoError(t, err)

	_, err = dirClient.SetAccessControl(ctx, &datalakedirectory.SetAccessControlOptions{ACL: &acl})
	require.NoError(t, err, "SetAccessControl on dir %s failed", dirPath)
	t.Logf("Set dir ACL on %s/%s/%s: %s", account, fsName, dirPath, acl)
}

func getDirACL(t *testing.T, account, fsName, dirPath string) string {
	t.Helper()
	ctx := context.Background()
	cred, err := azidentity.NewAzureCLICredential(nil)
	require.NoError(t, err)

	dirURL := fmt.Sprintf("https://%s.dfs.core.windows.net/%s/%s", account, fsName, dirPath)
	dirClient, err := datalakedirectory.NewClient(dirURL, cred, nil)
	require.NoError(t, err)

	resp, err := dirClient.GetAccessControl(ctx, nil)
	require.NoError(t, err, "GetAccessControl on dir %s failed", dirPath)
	require.NotNil(t, resp.ACL)
	return *resp.ACL
}

// ---------------------------------------------------------------------------
// Source filesystem setup — creates filesystem with known ACLs
// ---------------------------------------------------------------------------

func setupHNSSourceWithACLs(t *testing.T, account, fsName string) {
	t.Helper()
	ctx := context.Background()
	cred, err := azidentity.NewAzureCLICredential(nil)
	require.NoError(t, err)

	fsClient := newFilesystemClient(t, account, fsName)

	// Create filesystem
	_, err = fsClient.Create(ctx, nil)
	if err != nil && !strings.Contains(err.Error(), "FilesystemAlreadyExists") && !strings.Contains(err.Error(), "ContainerAlreadyExists") {
		require.NoError(t, err, "Create filesystem failed")
	}

	// Create sub1/ directory
	dirURL := fmt.Sprintf("https://%s.dfs.core.windows.net/%s/sub1", account, fsName)
	dirClient, err := datalakedirectory.NewClient(dirURL, cred, nil)
	require.NoError(t, err)
	_, err = dirClient.Create(ctx, nil)
	if err != nil && !strings.Contains(err.Error(), "PathAlreadyExists") {
		require.NoError(t, err, "Create sub1 dir failed")
	}

	// Create sub1/file.txt
	fileURL := fmt.Sprintf("https://%s.dfs.core.windows.net/%s/sub1/file.txt", account, fsName)
	fileClient, err := datalakefile.NewClient(fileURL, cred, nil)
	require.NoError(t, err)
	_, err = fileClient.Create(ctx, nil)
	require.NoError(t, err, "Create sub1/file.txt failed")
	err = fileClient.UploadBuffer(ctx, []byte("hello"), nil)
	require.NoError(t, err, "Upload sub1/file.txt failed")

	// Set ACLs
	setRootACL(t, account, fsName, srcACLRoot)
	setDirACL(t, account, fsName, "sub1", srcACLSubdir)
}

func createEmptyHNSFilesystem(t *testing.T, account, fsName string) {
	t.Helper()
	ctx := context.Background()
	fsClient := newFilesystemClient(t, account, fsName)
	_, err := fsClient.Create(ctx, nil)
	if err != nil && !strings.Contains(err.Error(), "FilesystemAlreadyExists") && !strings.Contains(err.Error(), "ContainerAlreadyExists") {
		require.NoError(t, err, "Create destination filesystem failed")
	}
}

// ---------------------------------------------------------------------------
// Run azcopy sync for ACL tests
// ---------------------------------------------------------------------------

func runACLSync(t *testing.T, binary, src, dst string, flags map[string]string) (string, string, int) {
	t.Helper()
	args := []string{"sync", src, dst}
	for k, v := range flags {
		if v == "" {
			args = append(args, fmt.Sprintf("--%s", k))
		} else {
			args = append(args, fmt.Sprintf("--%s=%s", k, v))
		}
	}

	cmd := exec.Command(binary, args...)
	cmd.Env = append(os.Environ(),
		"AZCOPY_AUTO_LOGIN_TYPE=AZCLI",
		"AZCOPY_LOG_LOCATION=/tmp/azcopy_acl_test_logs",
		"AZCOPY_JOB_PLAN_LOCATION=/tmp/azcopy_acl_test_plans",
	)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	t.Logf("Running: %s %s", binary, strings.Join(args, " "))
	err := cmd.Run()

	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			exitCode = -1
		}
	}

	stdoutStr := stdout.String()
	stderrStr := stderr.String()
	t.Logf("Exit code: %d", exitCode)
	if len(stdoutStr) > 0 {
		logStr := stdoutStr
		if len(logStr) > 2000 {
			logStr = logStr[len(logStr)-2000:]
		}
		t.Logf("Stdout (tail):\n%s", logStr)
	}
	if len(stderrStr) > 0 {
		t.Logf("Stderr:\n%s", stderrStr)
	}

	return stdoutStr, stderrStr, exitCode
}

// ---------------------------------------------------------------------------
// ACL comparison helper
// ---------------------------------------------------------------------------

// normalizeACL extracts just the base permission entries (user::, group::, other::)
// from an ACL string, ignoring mask and default entries that the service may add.
func normalizeACL(acl string) string {
	var parts []string
	for _, entry := range strings.Split(acl, ",") {
		entry = strings.TrimSpace(entry)
		if strings.HasPrefix(entry, "user::") ||
			strings.HasPrefix(entry, "group::") ||
			strings.HasPrefix(entry, "other::") {
			parts = append(parts, entry)
		}
	}
	return strings.Join(parts, ",")
}

// ---------------------------------------------------------------------------
// Test: Container-level root ACL transfer
// ---------------------------------------------------------------------------

func TestHNSRootACL_ContainerLevel_StandardBuild(t *testing.T) {
	binary := buildAzCopyStandard(t)

	srcFS := uniqueName("aclsrc")
	dstFS := uniqueName("acldst")

	// Setup source with ACLs
	setupHNSSourceWithACLs(t, c2cHNSSourceAccount, srcFS)
	t.Cleanup(func() { cleanupBlobFS(t, c2cHNSSourceAccount, srcFS) })

	// Create empty destination
	createEmptyHNSFilesystem(t, c2cHNSDestAccount, dstFS)
	t.Cleanup(func() { cleanupBlobFS(t, c2cHNSDestAccount, dstFS) })

	// Verify source ACLs are what we expect
	srcRootACL := getRootACL(t, c2cHNSSourceAccount, srcFS)
	t.Logf("Source root ACL: %s", srcRootACL)
	assert.Equal(t, srcACLRoot, normalizeACL(srcRootACL), "source root ACL should match what we set")

	// Run standard sync at container level with --include-root and --preserve-permissions
	// Standard build needs recursive=true to process folder properties
	srcURL := blobFSSyncURL(c2cHNSSourceAccount, srcFS)
	dstURL := blobFSSyncURL(c2cHNSDestAccount, dstFS)

	flags := map[string]string{
		"recursive":            "true",
		"preserve-permissions": "true",
		"include-root":         "true",
	}

	_, _, exitCode := runACLSync(t, binary, srcURL, dstURL, flags)
	require.Equal(t, 0, exitCode, "azcopy sync failed")

	// Verify destination root ACL matches source
	dstRootACL := getRootACL(t, c2cHNSDestAccount, dstFS)
	t.Logf("Destination root ACL: %s", dstRootACL)
	assert.Equal(t, normalizeACL(srcRootACL), normalizeACL(dstRootACL),
		"Standard build: destination root ACL should match source root ACL")
}

func TestHNSRootACL_ContainerLevel_MoverBuild(t *testing.T) {
	binary := buildAzCopyMover(t)

	srcFS := uniqueName("aclsrc")
	dstFS := uniqueName("acldst")

	// Setup source with ACLs
	setupHNSSourceWithACLs(t, c2cHNSSourceAccount, srcFS)
	t.Cleanup(func() { cleanupBlobFS(t, c2cHNSSourceAccount, srcFS) })

	// Create empty destination
	createEmptyHNSFilesystem(t, c2cHNSDestAccount, dstFS)
	t.Cleanup(func() { cleanupBlobFS(t, c2cHNSDestAccount, dstFS) })

	// Verify source ACLs are what we expect
	srcRootACL := getRootACL(t, c2cHNSSourceAccount, srcFS)
	t.Logf("Source root ACL: %s", srcRootACL)
	assert.Equal(t, srcACLRoot, normalizeACL(srcRootACL), "source root ACL should match what we set")

	// Run mover sync at container level with --include-root and --preserve-permissions
	// Mover build uses recursive=false (sync orchestrator handles recursion internally)
	srcURL := blobFSSyncURL(c2cHNSSourceAccount, srcFS)
	dstURL := blobFSSyncURL(c2cHNSDestAccount, dstFS)

	flags := map[string]string{
		"recursive":            "false",
		"preserve-permissions": "true",
		"include-root":         "true",
	}

	_, _, exitCode := runACLSync(t, binary, srcURL, dstURL, flags)
	require.Equal(t, 0, exitCode, "azcopy sync failed")

	// Check destination root ACL — expect it NOT to match (demonstrates the bug)
	dstRootACL := getRootACL(t, c2cHNSDestAccount, dstFS)
	t.Logf("Destination root ACL: %s", dstRootACL)

	// This assertion documents the bug: mover build does NOT transfer root ACLs
	assert.NotEqual(t, normalizeACL(srcRootACL), normalizeACL(dstRootACL),
		"Mover build BUG: destination root ACL should NOT match source (root ACL not transferred)")
	t.Logf("CONFIRMED BUG: mover build did not transfer root ACL. Source: %s, Dest: %s",
		normalizeACL(srcRootACL), normalizeACL(dstRootACL))
}

// ---------------------------------------------------------------------------
// Test: Subfolder-level ACL transfer
// ---------------------------------------------------------------------------

func TestHNSRootACL_SubfolderLevel_StandardBuild(t *testing.T) {
	binary := buildAzCopyStandard(t)

	srcFS := uniqueName("aclsrc")
	dstFS := uniqueName("acldst")

	// Setup source with ACLs
	setupHNSSourceWithACLs(t, c2cHNSSourceAccount, srcFS)
	t.Cleanup(func() { cleanupBlobFS(t, c2cHNSSourceAccount, srcFS) })

	// Create empty destination
	createEmptyHNSFilesystem(t, c2cHNSDestAccount, dstFS)
	t.Cleanup(func() { cleanupBlobFS(t, c2cHNSDestAccount, dstFS) })

	// Verify source subfolder ACL
	srcSubACL := getDirACL(t, c2cHNSSourceAccount, srcFS, "sub1")
	t.Logf("Source sub1 ACL: %s", srcSubACL)
	assert.Equal(t, srcACLSubdir, normalizeACL(srcSubACL), "source sub1 ACL should match what we set")

	// Run standard sync at subfolder level
	// Standard build needs recursive=true to process folder properties
	srcURL := blobFSSyncURL(c2cHNSSourceAccount, srcFS) + "/sub1"
	dstURL := blobFSSyncURL(c2cHNSDestAccount, dstFS) + "/sub1"

	flags := map[string]string{
		"recursive":            "true",
		"preserve-permissions": "true",
		"include-root":         "true",
	}

	_, _, exitCode := runACLSync(t, binary, srcURL, dstURL, flags)
	require.Equal(t, 0, exitCode, "azcopy sync failed")

	// Verify destination subfolder ACL matches source
	dstSubACL := getDirACL(t, c2cHNSDestAccount, dstFS, "sub1")
	t.Logf("Destination sub1 ACL: %s", dstSubACL)
	assert.Equal(t, normalizeACL(srcSubACL), normalizeACL(dstSubACL),
		"Standard build: destination sub1 ACL should match source sub1 ACL")
}

func TestHNSRootACL_SubfolderLevel_MoverBuild(t *testing.T) {
	binary := buildAzCopyMover(t)

	srcFS := uniqueName("aclsrc")
	dstFS := uniqueName("acldst")

	// Setup source with ACLs
	setupHNSSourceWithACLs(t, c2cHNSSourceAccount, srcFS)
	t.Cleanup(func() { cleanupBlobFS(t, c2cHNSSourceAccount, srcFS) })

	// Create empty destination
	createEmptyHNSFilesystem(t, c2cHNSDestAccount, dstFS)
	t.Cleanup(func() { cleanupBlobFS(t, c2cHNSDestAccount, dstFS) })

	// Verify source subfolder ACL
	srcSubACL := getDirACL(t, c2cHNSSourceAccount, srcFS, "sub1")
	t.Logf("Source sub1 ACL: %s", srcSubACL)
	assert.Equal(t, srcACLSubdir, normalizeACL(srcSubACL), "source sub1 ACL should match what we set")

	// Run mover sync at subfolder level
	// Mover build uses recursive=false (sync orchestrator handles recursion internally)
	srcURL := blobFSSyncURL(c2cHNSSourceAccount, srcFS) + "/sub1"
	dstURL := blobFSSyncURL(c2cHNSDestAccount, dstFS) + "/sub1"

	flags := map[string]string{
		"recursive":            "false",
		"preserve-permissions": "true",
		"include-root":         "true",
	}

	_, _, exitCode := runACLSync(t, binary, srcURL, dstURL, flags)
	require.Equal(t, 0, exitCode, "azcopy sync failed")

	// Check destination subfolder ACL — expect it NOT to match (same bug as container-level)
	// When sub1 is the root of the sync operation, the mover build skips its ACL too
	dstSubACL := getDirACL(t, c2cHNSDestAccount, dstFS, "sub1")
	t.Logf("Destination sub1 ACL: %s", dstSubACL)
	assert.NotEqual(t, normalizeACL(srcSubACL), normalizeACL(dstSubACL),
		"Mover build BUG: destination sub1 ACL should NOT match source (sync root ACL not transferred)")
	t.Logf("CONFIRMED BUG: mover build did not transfer subfolder ACL when subfolder is sync root. Source: %s, Dest: %s",
		normalizeACL(srcSubACL), normalizeACL(dstSubACL))
}
