package c2ctest

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	datalakefile "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/file"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/filesystem"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Account configuration
// ---------------------------------------------------------------------------

const (
	c2cBlobSourceAccount = "a2asource"
	c2cBlobDestAccount   = "a2atarget"
	c2cHNSSourceAccount  = "a2ahns"
	c2cHNSDestAccount    = "a2ahnstarget"
	c2cS3Region          = "us-east-1"
	c2cS3Endpoint        = "s3.amazonaws.com"
)

// ---------------------------------------------------------------------------
// SyncStats — parsed from azcopy stdout
// ---------------------------------------------------------------------------

type SyncStats struct {
	CopyFileTransfers     int // "Number of Copy Transfers for Files"
	CopyFolderTransfers   int // "Number of Copy Transfers for Folder Properties"
	TotalCopyTransfers    int // "Total Number of Copy Transfers"
	CopyCompleted         int // "Number of Copy Transfers Completed"
	CopyFailed            int // "Number of Copy Transfers Failed"
	Deletions             int // "Number of Deletions at Destination"
	FinalStatus           string
}

var (
	// Sync output uses dot-padding: "Number of Copy Transfers Completed: ............           13"
	// Use [.\s]* to match both dots and whitespace between label and value.
	reCopyFileTransfers   = regexp.MustCompile(`Number of Copy Transfers for Files:[.\s]*(\d+)`)
	reCopyFolderTransfers = regexp.MustCompile(`Number of Copy Transfers for Folder Properties:[.\s]*(\d+)`)
	reTotalCopyTransfers  = regexp.MustCompile(`Total Number of Copy Transfers:[.\s]*(\d+)`)
	reCopyCompleted       = regexp.MustCompile(`Number of Copy Transfers Completed:[.\s]*(\d+)`)
	reCopyFailed          = regexp.MustCompile(`Number of Copy Transfers Failed:[.\s]*(\d+)`)
	reDeletions           = regexp.MustCompile(`Number of Deletions at Destination:[.\s]*(\d+)`)
	reFinalStatus         = regexp.MustCompile(`Final Job Status:[.\s]*(\S+)`)
	reAlreadyInSync       = regexp.MustCompile(`already in sync`)
	reNowInSync           = regexp.MustCompile(`now in sync`)
)

func extractInt(re *regexp.Regexp, s string) int {
	m := re.FindStringSubmatch(s)
	if len(m) < 2 {
		return 0
	}
	v, _ := strconv.Atoi(m[1])
	return v
}

func parseSyncStats(stdout string) SyncStats {
	return SyncStats{
		CopyFileTransfers:   extractInt(reCopyFileTransfers, stdout),
		CopyFolderTransfers: extractInt(reCopyFolderTransfers, stdout),
		TotalCopyTransfers:  extractInt(reTotalCopyTransfers, stdout),
		CopyCompleted:       extractInt(reCopyCompleted, stdout),
		CopyFailed:          extractInt(reCopyFailed, stdout),
		Deletions:           extractInt(reDeletions, stdout),
		FinalStatus:         extractFinalStatus(stdout),
	}
}

func extractFinalStatus(s string) string {
	m := reFinalStatus.FindStringSubmatch(s)
	if len(m) < 2 {
		if reAlreadyInSync.MatchString(s) {
			return "AlreadyInSync"
		}
		if reNowInSync.MatchString(s) {
			return "NowInSync"
		}
		return "Unknown"
	}
	return m[1]
}

// ---------------------------------------------------------------------------
// Binary build helper
// ---------------------------------------------------------------------------

func buildAzCopy(t *testing.T) string {
	t.Helper()
	binPath := os.Getenv("AZCOPY_E2E_EXECUTABLE_PATH")
	if binPath != "" {
		t.Logf("Using pre-built azcopy at %s", binPath)
		return binPath
	}

	goarch := os.Getenv("GOARCH")
	if goarch == "" {
		goarch = runtime.GOARCH
	}

	binPath = fmt.Sprintf("/tmp/azcopy_c2c_test_%s", uuid.New().String()[:8])
	cmd := exec.Command("go", "build", "-tags", "netgo,smslidingwindow,mover", "-o", binPath, ".")
	cmd.Dir = ".." // project root (cmd/ is our package)
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("GOARCH=%s", goarch),
		fmt.Sprintf("GOOS=%s", runtime.GOOS),
	)

	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "Failed to build azcopy: %s", string(out))

	t.Cleanup(func() { os.Remove(binPath) })
	t.Logf("Built azcopy at %s", binPath)
	return binPath
}

// ---------------------------------------------------------------------------
// Run azcopy sync subprocess
// ---------------------------------------------------------------------------

func runAzCopySync(t *testing.T, binary, src, dst string, flags map[string]string) (string, string, int) {
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
		"SYNC_ORCHESTRATOR_TEST_MODE=DEFAULT",
		"AZCOPY_AUTO_LOGIN_TYPE=AZCLI",
		"AZCOPY_LOG_LOCATION=/tmp/azcopy_c2c_test_logs",
		"AZCOPY_JOB_PLAN_LOCATION=/tmp/azcopy_c2c_test_plans",
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
		// Log last 2000 chars of stdout to avoid overwhelming test output
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
// Unique name generator
// ---------------------------------------------------------------------------

func uniqueName(prefix string) string {
	return fmt.Sprintf("%s-%s", prefix, uuid.New().String()[:8])
}

// ---------------------------------------------------------------------------
// Azure Blob helpers
// ---------------------------------------------------------------------------

func newBlobClient(t *testing.T, account string) *azblob.Client {
	t.Helper()
	cred, err := azidentity.NewAzureCLICredential(nil)
	require.NoError(t, err, "AzureCLICredential failed — run 'az login' first")

	url := fmt.Sprintf("https://%s.blob.core.windows.net", account)
	client, err := azblob.NewClient(url, cred, nil)
	require.NoError(t, err, "azblob.NewClient failed for %s", account)
	return client
}

func setupBlobData(t *testing.T, account, containerName string, files map[string][]byte) {
	t.Helper()
	ctx := context.Background()
	client := newBlobClient(t, account)

	// Create container (ignore if exists)
	_, err := client.CreateContainer(ctx, containerName, nil)
	if err != nil && !strings.Contains(err.Error(), "ContainerAlreadyExists") {
		require.NoError(t, err, "CreateContainer failed for %s/%s", account, containerName)
	}

	for name, data := range files {
		_, err := client.UploadBuffer(ctx, containerName, name, data, &azblob.UploadBufferOptions{
			BlockSize: int64(len(data)) + 1,
		})
		require.NoError(t, err, "UploadBuffer failed for %s/%s/%s", account, containerName, name)
	}
}

func reuploadBlobs(t *testing.T, account, containerName string, files []string, allData map[string][]byte) {
	t.Helper()
	ctx := context.Background()
	client := newBlobClient(t, account)

	for _, name := range files {
		data, ok := allData[name]
		if !ok {
			data = []byte("re-uploaded-" + name)
		}
		_, err := client.UploadBuffer(ctx, containerName, name, data, &azblob.UploadBufferOptions{
			BlockSize: int64(len(data)) + 1,
		})
		require.NoError(t, err, "reuploadBlobs: UploadBuffer failed for %s", name)
	}
}

func cleanupBlob(t *testing.T, account, containerName string) {
	t.Helper()
	ctx := context.Background()
	client := newBlobClient(t, account)
	_, err := client.DeleteContainer(ctx, containerName, nil)
	if err != nil {
		t.Logf("Warning: cleanup failed for blob %s/%s: %v", account, containerName, err)
	}
}

func listBlob(t *testing.T, account, containerName string) []string {
	t.Helper()
	ctx := context.Background()
	client := newBlobClient(t, account)

	var names []string
	pager := client.NewListBlobsFlatPager(containerName, nil)
	for pager.More() {
		page, err := pager.NextPage(ctx)
		require.NoError(t, err, "listBlob: NextPage failed")
		for _, item := range page.Segment.BlobItems {
			if item.Name != nil {
				names = append(names, *item.Name)
			}
		}
	}
	sort.Strings(names)
	return names
}

func blobSyncURL(account, containerName string) string {
	return fmt.Sprintf("https://%s.blob.core.windows.net/%s", account, containerName)
}

// ---------------------------------------------------------------------------
// Azure BlobFS (HNS/DFS) helpers
// ---------------------------------------------------------------------------

func newFilesystemClient(t *testing.T, account, fsName string) *filesystem.Client {
	t.Helper()
	cred, err := azidentity.NewAzureCLICredential(nil)
	require.NoError(t, err, "AzureCLICredential failed — run 'az login' first")

	url := fmt.Sprintf("https://%s.dfs.core.windows.net/%s", account, fsName)
	client, err := filesystem.NewClient(url, cred, nil)
	require.NoError(t, err, "filesystem.NewClient failed for %s/%s", account, fsName)
	return client
}

func setupBlobFSData(t *testing.T, account, fsName string, files map[string][]byte) {
	t.Helper()
	ctx := context.Background()
	fsClient := newFilesystemClient(t, account, fsName)

	// Create filesystem (ignore if exists)
	_, err := fsClient.Create(ctx, nil)
	if err != nil && !strings.Contains(err.Error(), "FilesystemAlreadyExists") && !strings.Contains(err.Error(), "ContainerAlreadyExists") {
		require.NoError(t, err, "Create filesystem failed for %s/%s", account, fsName)
	}

	cred, err := azidentity.NewAzureCLICredential(nil)
	require.NoError(t, err)

	for name, data := range files {
		// Ensure parent directories exist by creating file directly
		fileURL := fmt.Sprintf("https://%s.dfs.core.windows.net/%s/%s", account, fsName, name)
		fileClient, err := datalakefile.NewClient(fileURL, cred, nil)
		require.NoError(t, err, "datalakefile.NewClient failed for %s", name)

		// Create file
		_, err = fileClient.Create(ctx, nil)
		require.NoError(t, err, "Create file failed for %s", name)

		// Upload data if non-empty
		if len(data) > 0 {
			err = fileClient.UploadBuffer(ctx, data, nil)
			require.NoError(t, err, "UploadBuffer failed for %s", name)
		}
	}
}

func reuploadBlobFS(t *testing.T, account, fsName string, files []string, allData map[string][]byte) {
	t.Helper()
	ctx := context.Background()
	cred, err := azidentity.NewAzureCLICredential(nil)
	require.NoError(t, err)

	for _, name := range files {
		data, ok := allData[name]
		if !ok {
			data = []byte("re-uploaded-" + name)
		}
		fileURL := fmt.Sprintf("https://%s.dfs.core.windows.net/%s/%s", account, fsName, name)
		fileClient, err := datalakefile.NewClient(fileURL, cred, nil)
		require.NoError(t, err)

		_, err = fileClient.Create(ctx, nil)
		require.NoError(t, err)

		if len(data) > 0 {
			err = fileClient.UploadBuffer(ctx, data, nil)
			require.NoError(t, err)
		}
	}
}

func cleanupBlobFS(t *testing.T, account, fsName string) {
	t.Helper()
	ctx := context.Background()
	fsClient := newFilesystemClient(t, account, fsName)
	_, err := fsClient.Delete(ctx, nil)
	if err != nil {
		t.Logf("Warning: cleanup failed for blobFS %s/%s: %v", account, fsName, err)
	}
}

func listBlobFS(t *testing.T, account, fsName string) []string {
	t.Helper()
	ctx := context.Background()
	fsClient := newFilesystemClient(t, account, fsName)

	recursive := true
	var names []string
	pager := fsClient.NewListPathsPager(recursive, &filesystem.ListPathsOptions{})
	for pager.More() {
		page, err := pager.NextPage(ctx)
		require.NoError(t, err, "listBlobFS: NextPage failed")
		for _, path := range page.Paths {
			if path.Name != nil {
				isDir := path.IsDirectory != nil && *path.IsDirectory
				if !isDir {
					names = append(names, *path.Name)
				}
			}
		}
	}
	sort.Strings(names)
	return names
}

func blobFSSyncURL(account, fsName string) string {
	return fmt.Sprintf("https://%s.dfs.core.windows.net/%s", account, fsName)
}

// ---------------------------------------------------------------------------
// Key Vault helpers
// ---------------------------------------------------------------------------

const c2cKeyVaultName = "shnayaksmbtest1"

// getSecretFromKeyVault retrieves a secret from Azure Key Vault using the az CLI.
// Falls back to the environment variable if the az CLI call fails.
func getSecretFromKeyVault(t *testing.T, vaultName, secretName, envFallback string) string {
	t.Helper()

	// Check env var first as an override
	if v := os.Getenv(envFallback); v != "" {
		return v
	}

	out, err := exec.Command("az", "keyvault", "secret", "show",
		"--vault-name", vaultName,
		"--name", secretName,
		"--query", "value",
		"--output", "tsv",
	).Output()
	if err != nil {
		t.Fatalf("Failed to retrieve secret %q from Key Vault %q (and %s env var not set): %v",
			secretName, vaultName, envFallback, err)
	}
	return strings.TrimSpace(string(out))
}

// ensureAWSCredsInEnv fetches AWS credentials from Key Vault (if not already
// in the environment) and sets them as env vars so that child azcopy processes
// inherit them automatically via os.Environ().
var awsCredsOnce sync.Once

func ensureAWSCredsInEnv(t *testing.T) {
	t.Helper()
	awsCredsOnce.Do(func() {
		if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
			v := getSecretFromKeyVault(t, c2cKeyVaultName, "AWS-ACCESS-KEY-ID", "AWS_ACCESS_KEY_ID")
			os.Setenv("AWS_ACCESS_KEY_ID", v)
		}
		if os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
			v := getSecretFromKeyVault(t, c2cKeyVaultName, "AWS-SECRET-ACCESS-KEY", "AWS_SECRET_ACCESS_KEY")
			os.Setenv("AWS_SECRET_ACCESS_KEY", v)
		}
	})
}

// ---------------------------------------------------------------------------
// AWS S3 helpers
// ---------------------------------------------------------------------------

func newS3Client(t *testing.T) *minio.Client {
	t.Helper()
	ensureAWSCredsInEnv(t)
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	require.NotEmpty(t, accessKey, "AWS_ACCESS_KEY_ID must be set")
	require.NotEmpty(t, secretKey, "AWS_SECRET_ACCESS_KEY must be set")

	client, err := minio.New(c2cS3Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: true,
		Region: c2cS3Region,
	})
	require.NoError(t, err, "minio.New failed")
	return client
}

func setupS3Data(t *testing.T, bucket string, files map[string][]byte) {
	t.Helper()
	ctx := context.Background()
	client := newS3Client(t)

	exists, err := client.BucketExists(ctx, bucket)
	require.NoError(t, err)
	if !exists {
		err = client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{Region: c2cS3Region})
		require.NoError(t, err, "MakeBucket failed for %s", bucket)
	}

	for name, data := range files {
		_, err := client.PutObject(ctx, bucket, name, bytes.NewReader(data), int64(len(data)),
			minio.PutObjectOptions{ContentType: "application/octet-stream"})
		require.NoError(t, err, "PutObject failed for %s/%s", bucket, name)
	}
}

func reuploadS3Objects(t *testing.T, bucket string, files []string, allData map[string][]byte) {
	t.Helper()
	ctx := context.Background()
	client := newS3Client(t)

	for _, name := range files {
		data, ok := allData[name]
		if !ok {
			data = []byte("re-uploaded-" + name)
		}
		_, err := client.PutObject(ctx, bucket, name, bytes.NewReader(data), int64(len(data)),
			minio.PutObjectOptions{ContentType: "application/octet-stream"})
		require.NoError(t, err, "reuploadS3Objects: PutObject failed for %s", name)
	}
}

func cleanupS3(t *testing.T, bucket string) {
	t.Helper()
	ctx := context.Background()
	client := newS3Client(t)

	exists, err := client.BucketExists(ctx, bucket)
	if err != nil || !exists {
		return
	}

	// Remove all objects first
	objectCh := client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Recursive: true})
	for obj := range objectCh {
		if obj.Err != nil {
			t.Logf("Warning: list error during S3 cleanup: %v", obj.Err)
			continue
		}
		err := client.RemoveObject(ctx, bucket, obj.Key, minio.RemoveObjectOptions{})
		if err != nil {
			t.Logf("Warning: remove object failed during S3 cleanup: %v", err)
		}
	}

	err = client.RemoveBucket(ctx, bucket)
	if err != nil {
		t.Logf("Warning: cleanup failed for S3 bucket %s: %v", bucket, err)
	}
}

func listS3(t *testing.T, bucket string) []string {
	t.Helper()
	ctx := context.Background()
	client := newS3Client(t)

	var names []string
	objectCh := client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Recursive: true})
	for obj := range objectCh {
		require.NoError(t, obj.Err, "listS3: error listing %s", bucket)
		// Skip directory marker objects (zero-byte objects ending in /)
		if !strings.HasSuffix(obj.Key, "/") || obj.Size > 0 {
			names = append(names, obj.Key)
		}
	}
	sort.Strings(names)
	return names
}

func s3SyncURL(bucket string) string {
	return fmt.Sprintf("https://s3.amazonaws.com/%s", bucket)
}

// ---------------------------------------------------------------------------
// Dataset generators
// ---------------------------------------------------------------------------

func mainDataset() map[string][]byte {
	return map[string][]byte{
		// Root-level files
		"root_file1.txt":  []byte("root1"),
		"root_file2.txt":  []byte("root2"),
		"root_file3.txt":  []byte("root3"),
		"root_config.json": []byte(`{"key":"value"}`),

		// dir1/ — broad and deep
		"dir1/file_a.txt":                 []byte("dir1-a"),
		"dir1/file_b.txt":                 []byte("dir1-b"),
		"dir1/file_c.txt":                 []byte("dir1-c"),
		"dir1/sub1/deep_file.txt":         []byte("deep1"),
		"dir1/sub1/deep_file2.txt":        []byte("deep2"),
		"dir1/sub1/sub2/level4.txt":       []byte("level4"),
		"dir1/sub1/sub2/sub3/level5.txt":  []byte("level5"),

		// dir2/ — deeper nesting
		"dir2/only_file.txt":   []byte("dir2-only"),
		"dir2/nested/file.txt": []byte("dir2-nested"),

		// dir3/ — mid-level branching
		"dir3/mid/bottom/leaf.txt":   []byte("leaf"),
		"dir3/mid/bottom/leaf2.json": []byte(`{"leaf":2}`),
		"dir3/mid/sibling.txt":       []byte("sibling"),

		// dir10/ — prefix-confusable with dir1/
		"dir10/file_x.txt": []byte("dir10-x"),

		// Unicode directories
		"打麻将/日本語ファイル.txt": []byte("unicode-content"),
		"données/café.txt":        []byte("données-café"),
		"Кириллица/файл.txt":      []byte("cyrillic-content"),

		// Spaces in names
		"spaces dir/file with spaces.txt": []byte("spaces-content"),
		"spaces dir/another file.log":     []byte("spaces-log"),

		// Dense directory with 5 files
		"dense/f1.txt": []byte("dense1"),
		"dense/f2.txt": []byte("dense2"),
		"dense/f3.txt": []byte("dense3"),
		"dense/f4.txt": []byte("dense4"),
		"dense/f5.txt": []byte("dense5"),

		// Prefix-confusable root file
		"dir1_extra.txt": []byte("dir1-extra-root"),
	}
}

func namelessDirDataset() map[string][]byte {
	return map[string][]byte{
		"normal.txt":                        []byte("normal"),
		"a/file.txt":                        []byte("a-file"),
		"a//nameless1.txt":                  []byte("nameless1"),
		"a///double_nameless.txt":           []byte("double-nameless"),
		"//root_nameless.txt":               []byte("root-nameless"),
		"//nested/named_under_nameless.txt": []byte("named-under-nameless"),
		"s//mx.json":                        []byte("s-mx"),
		"s/regular.txt":                     []byte("s-regular"),
	}
}

func deepNamelessDirDataset() map[string][]byte {
	return map[string][]byte{
		"m//single.txt":             []byte("single"),
		"m///double.txt":            []byte("double"),
		"m////triple.txt":           []byte("triple"),
		"m/////quad.txt":            []byte("quad"),
		"m//a//b//alternating.txt":  []byte("alternating"),
	}
}

func crossTypeDataset() map[string][]byte {
	return map[string][]byte{
		"proj/src/main.go":        []byte("package main"),
		"proj/src/util.go":        []byte("package main // util"),
		"proj/docs/readme.txt":    []byte("readme"),
		"proj//nameless_file.txt": []byte("nameless-proj"),
		"proj/src//hidden.go":     []byte("hidden"),
	}
}

func crossTypeDatasetNormalized() map[string][]byte {
	// What HNS produces after collapsing //
	return map[string][]byte{
		"proj/src/main.go":      []byte("package main"),
		"proj/src/util.go":      []byte("package main // util"),
		"proj/docs/readme.txt":  []byte("readme"),
		"proj/nameless_file.txt": []byte("nameless-proj"),
		"proj/src/hidden.go":    []byte("hidden"),
	}
}

func mixedDirDataset() map[string][]byte {
	return map[string][]byte{
		"stubbed/file.txt":           []byte("stubbed"),
		"virtual/file.txt":           []byte("virtual"),
		"nameless//file.txt":         []byte("nameless"),
		"mixed/regular.txt":          []byte("mixed-regular"),
		"mixed//nameless_child.txt":  []byte("mixed-nameless"),
	}
}

func rootOnlyDataset() map[string][]byte {
	return map[string][]byte{
		"a.txt": []byte("a"),
		"b.txt": []byte("b"),
		"c.txt": []byte("c"),
	}
}

func datasetKeys(d map[string][]byte) []string {
	keys := make([]string, 0, len(d))
	for k := range d {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// filterByPrefix returns keys that start with prefix + "/".
func filterByPrefix(keys []string, prefix string) []string {
	p := prefix + "/"
	var out []string
	for _, k := range keys {
		if strings.HasPrefix(k, p) {
			out = append(out, k)
		}
	}
	return out
}

// filterOutPrefix returns keys that do NOT start with prefix + "/".
func filterOutPrefix(keys []string, prefix string) []string {
	p := prefix + "/"
	var out []string
	for _, k := range keys {
		if !strings.HasPrefix(k, p) {
			out = append(out, k)
		}
	}
	return out
}

// subpathDataset returns only entries from d whose keys start with prefix + "/".
func subpathDataset(d map[string][]byte, prefix string) map[string][]byte {
	p := prefix + "/"
	out := make(map[string][]byte)
	for k, v := range d {
		if strings.HasPrefix(k, p) {
			out[k] = v
		}
	}
	return out
}

// stripPrefix removes prefix + "/" from each key.
func stripPrefix(keys []string, prefix string) []string {
	p := prefix + "/"
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, strings.TrimPrefix(k, p))
	}
	sort.Strings(out)
	return out
}

// ---------------------------------------------------------------------------
// Assertion helpers
// ---------------------------------------------------------------------------

func assertDestContains(t *testing.T, actual, expected []string) {
	t.Helper()
	sort.Strings(actual)
	sort.Strings(expected)
	assert.Equal(t, expected, actual,
		"Destination mismatch.\nExpected (%d): %v\nActual   (%d): %v",
		len(expected), expected, len(actual), actual)
}

func assertNoLeadingSlash(t *testing.T, names []string) {
	t.Helper()
	for _, n := range names {
		// A leading slash in a blob name means the actual blob key starts with /
		// which is wrong — azcopy should strip the internal "/" prefix.
		// Exception: nameless dir paths legitimately start with // at the source.
		// We only check for single leading / that isn't followed by another /
		// (double // is a valid nameless dir blob name).
		if strings.HasPrefix(n, "/") && !strings.HasPrefix(n, "//") {
			t.Errorf("Destination blob has unexpected leading '/': %q", n)
		}
	}
}

// ---------------------------------------------------------------------------
// C2C pair abstraction
// ---------------------------------------------------------------------------

type c2cPair struct {
	name       string
	srcAccount string
	dstAccount string

	srcSetup    func(t *testing.T, container string, files map[string][]byte)
	dstSetup    func(t *testing.T, container string, files map[string][]byte)
	srcReupload func(t *testing.T, container string, files []string, allData map[string][]byte)
	dstReupload func(t *testing.T, container string, files []string, allData map[string][]byte)
	srcCleanup  func(t *testing.T, container string)
	dstCleanup  func(t *testing.T, container string)
	dstList     func(t *testing.T, container string) []string
	srcURL      func(container string) string
	dstURL      func(container string) string

	// supportsNamelessDirs indicates whether this pair's source supports
	// nameless virtual directories (paths with //).
	supportsNamelessDirs bool
	// isHNSDest indicates whether the destination is HNS (normalizes //)
	isHNSDest bool
}

func allC2CPairs() []c2cPair {
	return []c2cPair{
		{
			name:       "S3ToBlob",
			srcAccount: "s3",
			dstAccount: c2cBlobDestAccount,
			srcSetup:   func(t *testing.T, c string, f map[string][]byte) { setupS3Data(t, c, f) },
			dstSetup:   func(t *testing.T, c string, f map[string][]byte) { setupBlobData(t, c2cBlobDestAccount, c, f) },
			srcReupload: func(t *testing.T, c string, files []string, d map[string][]byte) { reuploadS3Objects(t, c, files, d) },
			dstReupload: func(t *testing.T, c string, files []string, d map[string][]byte) { reuploadBlobs(t, c2cBlobDestAccount, c, files, d) },
			srcCleanup: func(t *testing.T, c string) { cleanupS3(t, c) },
			dstCleanup: func(t *testing.T, c string) { cleanupBlob(t, c2cBlobDestAccount, c) },
			dstList:    func(t *testing.T, c string) []string { return listBlob(t, c2cBlobDestAccount, c) },
			srcURL:     func(c string) string { return s3SyncURL(c) },
			dstURL:     func(c string) string { return blobSyncURL(c2cBlobDestAccount, c) },
			supportsNamelessDirs: true,
			isHNSDest:            false,
		},
		{
			name:       "BlobToBlob",
			srcAccount: c2cBlobSourceAccount,
			dstAccount: c2cBlobDestAccount,
			srcSetup:   func(t *testing.T, c string, f map[string][]byte) { setupBlobData(t, c2cBlobSourceAccount, c, f) },
			dstSetup:   func(t *testing.T, c string, f map[string][]byte) { setupBlobData(t, c2cBlobDestAccount, c, f) },
			srcReupload: func(t *testing.T, c string, files []string, d map[string][]byte) { reuploadBlobs(t, c2cBlobSourceAccount, c, files, d) },
			dstReupload: func(t *testing.T, c string, files []string, d map[string][]byte) { reuploadBlobs(t, c2cBlobDestAccount, c, files, d) },
			srcCleanup: func(t *testing.T, c string) { cleanupBlob(t, c2cBlobSourceAccount, c) },
			dstCleanup: func(t *testing.T, c string) { cleanupBlob(t, c2cBlobDestAccount, c) },
			dstList:    func(t *testing.T, c string) []string { return listBlob(t, c2cBlobDestAccount, c) },
			srcURL:     func(c string) string { return blobSyncURL(c2cBlobSourceAccount, c) },
			dstURL:     func(c string) string { return blobSyncURL(c2cBlobDestAccount, c) },
			supportsNamelessDirs: true,
			isHNSDest:            false,
		},
		{
			name:       "BlobToBlobFS",
			srcAccount: c2cBlobSourceAccount,
			dstAccount: c2cHNSSourceAccount,
			srcSetup:   func(t *testing.T, c string, f map[string][]byte) { setupBlobData(t, c2cBlobSourceAccount, c, f) },
			dstSetup:   func(t *testing.T, c string, f map[string][]byte) { setupBlobFSData(t, c2cHNSSourceAccount, c, f) },
			srcReupload: func(t *testing.T, c string, files []string, d map[string][]byte) { reuploadBlobs(t, c2cBlobSourceAccount, c, files, d) },
			dstReupload: func(t *testing.T, c string, files []string, d map[string][]byte) { reuploadBlobFS(t, c2cHNSSourceAccount, c, files, d) },
			srcCleanup: func(t *testing.T, c string) { cleanupBlob(t, c2cBlobSourceAccount, c) },
			dstCleanup: func(t *testing.T, c string) { cleanupBlobFS(t, c2cHNSSourceAccount, c) },
			dstList:    func(t *testing.T, c string) []string { return listBlobFS(t, c2cHNSSourceAccount, c) },
			srcURL:     func(c string) string { return blobSyncURL(c2cBlobSourceAccount, c) },
			dstURL:     func(c string) string { return blobFSSyncURL(c2cHNSSourceAccount, c) },
			supportsNamelessDirs: true,
			isHNSDest:            true,
		},
		{
			name:       "BlobFSToBlob",
			srcAccount: c2cHNSSourceAccount,
			dstAccount: c2cBlobDestAccount,
			srcSetup:   func(t *testing.T, c string, f map[string][]byte) { setupBlobFSData(t, c2cHNSSourceAccount, c, f) },
			dstSetup:   func(t *testing.T, c string, f map[string][]byte) { setupBlobData(t, c2cBlobDestAccount, c, f) },
			srcReupload: func(t *testing.T, c string, files []string, d map[string][]byte) { reuploadBlobFS(t, c2cHNSSourceAccount, c, files, d) },
			dstReupload: func(t *testing.T, c string, files []string, d map[string][]byte) { reuploadBlobs(t, c2cBlobDestAccount, c, files, d) },
			srcCleanup: func(t *testing.T, c string) { cleanupBlobFS(t, c2cHNSSourceAccount, c) },
			dstCleanup: func(t *testing.T, c string) { cleanupBlob(t, c2cBlobDestAccount, c) },
			dstList:    func(t *testing.T, c string) []string { return listBlob(t, c2cBlobDestAccount, c) },
			srcURL:     func(c string) string { return blobFSSyncURL(c2cHNSSourceAccount, c) },
			dstURL:     func(c string) string { return blobSyncURL(c2cBlobDestAccount, c) },
			supportsNamelessDirs: false,
			isHNSDest:            false,
		},
		{
			name:       "BlobFSToBlobFS",
			srcAccount: c2cHNSSourceAccount,
			dstAccount: c2cHNSDestAccount,
			srcSetup:   func(t *testing.T, c string, f map[string][]byte) { setupBlobFSData(t, c2cHNSSourceAccount, c, f) },
			dstSetup:   func(t *testing.T, c string, f map[string][]byte) { setupBlobFSData(t, c2cHNSDestAccount, c, f) },
			srcReupload: func(t *testing.T, c string, files []string, d map[string][]byte) { reuploadBlobFS(t, c2cHNSSourceAccount, c, files, d) },
			dstReupload: func(t *testing.T, c string, files []string, d map[string][]byte) { reuploadBlobFS(t, c2cHNSDestAccount, c, files, d) },
			srcCleanup: func(t *testing.T, c string) { cleanupBlobFS(t, c2cHNSSourceAccount, c) },
			dstCleanup: func(t *testing.T, c string) { cleanupBlobFS(t, c2cHNSDestAccount, c) },
			dstList:    func(t *testing.T, c string) []string { return listBlobFS(t, c2cHNSDestAccount, c) },
			srcURL:     func(c string) string { return blobFSSyncURL(c2cHNSSourceAccount, c) },
			dstURL:     func(c string) string { return blobFSSyncURL(c2cHNSDestAccount, c) },
			supportsNamelessDirs: false,
			isHNSDest:            true,
		},
	}
}

// flatNamespacePairs returns only pairs where both source and destination support nameless dirs (no HNS dest).
func flatNamespacePairs() []c2cPair {
	var pairs []c2cPair
	for _, p := range allC2CPairs() {
		if p.supportsNamelessDirs && !p.isHNSDest {
			pairs = append(pairs, p)
		}
	}
	return pairs
}

// blobToBlobPair returns only the BlobToBlob pair.
func blobToBlobPair() []c2cPair {
	for _, p := range allC2CPairs() {
		if p.name == "BlobToBlob" {
			return []c2cPair{p}
		}
	}
	return nil
}

// blobToBlobFSPair returns only the BlobToBlobFS pair.
func blobToBlobFSPair() []c2cPair {
	for _, p := range allC2CPairs() {
		if p.name == "BlobToBlobFS" {
			return []c2cPair{p}
		}
	}
	return nil
}

// blobFSToBlobPair returns only the BlobFSToBlob pair.
func blobFSToBlobPair() []c2cPair {
	for _, p := range allC2CPairs() {
		if p.name == "BlobFSToBlob" {
			return []c2cPair{p}
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Default sync flags
// ---------------------------------------------------------------------------

func defaultSyncFlags() map[string]string {
	return map[string]string{
		"recursive": "false",
	}
}

func mirrorSyncFlags() map[string]string {
	return map[string]string{
		"recursive":          "false",
		"delete-destination": "true",
	}
}

// ===========================================================================
// Test Scenarios
// ===========================================================================

// ---------------------------------------------------------------------------
// Scenario 1: Fresh Sync (source → empty destination) — All 5 C2C pairs
// ---------------------------------------------------------------------------

func TestSync_C2C_FreshSync(t *testing.T) {
	binary := buildAzCopy(t)
	dataset := mainDataset()
	expected := datasetKeys(dataset)

	for _, pair := range allC2CPairs() {
		pair := pair
		t.Run(pair.name, func(t *testing.T) {
			t.Parallel()
			srcContainer := uniqueName("c2c-fresh-src")
			dstContainer := uniqueName("c2c-fresh-dst")
			defer pair.srcCleanup(t, srcContainer)
			defer pair.dstCleanup(t, dstContainer)

			// Setup source with full dataset
			pair.srcSetup(t, srcContainer, dataset)

			// Create empty destination container
			pair.dstSetup(t, dstContainer, map[string][]byte{})

			// Run sync
			stdout, _, exitCode := runAzCopySync(t, binary,
				pair.srcURL(srcContainer), pair.dstURL(dstContainer),
				defaultSyncFlags())

			require.Equal(t, 0, exitCode, "azcopy sync failed")

			// Validate destination
			actual := pair.dstList(t, dstContainer)
			assertDestContains(t, actual, expected)
			assertNoLeadingSlash(t, actual)

			// Validate stats
			stats := parseSyncStats(stdout)
			assert.Equal(t, len(expected)+stats.CopyFolderTransfers, stats.CopyCompleted,
				"Expected %d file transfers completed", len(expected))
			assert.Equal(t, 0, stats.CopyFailed, "Expected 0 failed transfers")
		})
	}
}

// ---------------------------------------------------------------------------
// Scenario 2: Incremental Sync (modified source files) — All 5 C2C pairs
// ---------------------------------------------------------------------------

func TestSync_C2C_IncrementalSync(t *testing.T) {
	binary := buildAzCopy(t)
	dataset := mainDataset()
	expected := datasetKeys(dataset)

	modifiedFiles := []string{
		"root_file1.txt",
		"dir1/file_a.txt",
		"dir1/sub1/deep_file.txt",
		"dir3/mid/bottom/leaf.txt",
		"打麻将/日本語ファイル.txt",
	}

	for _, pair := range allC2CPairs() {
		pair := pair
		t.Run(pair.name, func(t *testing.T) {
			t.Parallel()
			srcContainer := uniqueName("c2c-incr-src")
			dstContainer := uniqueName("c2c-incr-dst")
			defer pair.srcCleanup(t, srcContainer)
			defer pair.dstCleanup(t, dstContainer)

			// Upload full dataset to source
			pair.srcSetup(t, srcContainer, dataset)

			// Wait to establish LMT gap
			time.Sleep(2 * time.Second)

			// Upload full dataset to destination (dest is newer)
			pair.dstSetup(t, dstContainer, dataset)

			// Wait again
			time.Sleep(2 * time.Second)

			// Re-upload 5 files to source (source now newer for these)
			pair.srcReupload(t, srcContainer, modifiedFiles, dataset)

			// Run sync
			stdout, _, exitCode := runAzCopySync(t, binary,
				pair.srcURL(srcContainer), pair.dstURL(dstContainer),
				defaultSyncFlags())

			require.Equal(t, 0, exitCode, "azcopy sync failed")

			// Validate destination still has all files
			actual := pair.dstList(t, dstContainer)
			assertDestContains(t, actual, expected)
			assertNoLeadingSlash(t, actual)

			// Validate stats: only modified files should transfer
			stats := parseSyncStats(stdout)
			assert.Equal(t, len(modifiedFiles), stats.CopyFileTransfers,
				"Expected %d copy transfers scheduled", len(modifiedFiles))
			assert.Equal(t, len(modifiedFiles)+stats.CopyFolderTransfers, stats.CopyCompleted,
				"Expected %d files transferred", len(modifiedFiles))
		})
	}
}

// ---------------------------------------------------------------------------
// Scenario 3: Mirror Mode — File Deletion — All 5 C2C pairs
// ---------------------------------------------------------------------------

func TestSync_C2C_MirrorFileDelete(t *testing.T) {
	binary := buildAzCopy(t)
	fullDataset := mainDataset()
	subsetFiles := map[string][]byte{
		"root_file1.txt":          []byte("root1"),
		"dir1/file_a.txt":         []byte("dir1-a"),
		"dir1/sub1/deep_file.txt": []byte("deep1"),
		"dir3/mid/bottom/leaf.txt": []byte("leaf"),
	}
	expectedKeys := datasetKeys(subsetFiles)

	for _, pair := range allC2CPairs() {
		pair := pair
		t.Run(pair.name, func(t *testing.T) {
			t.Parallel()
			srcContainer := uniqueName("c2c-mirdel-src")
			dstContainer := uniqueName("c2c-mirdel-dst")
			defer pair.srcCleanup(t, srcContainer)
			defer pair.dstCleanup(t, dstContainer)

			// Source: subset only
			pair.srcSetup(t, srcContainer, subsetFiles)

			// Destination: full dataset
			pair.dstSetup(t, dstContainer, fullDataset)

			// Run sync with mirror mode
			stdout, _, exitCode := runAzCopySync(t, binary,
				pair.srcURL(srcContainer), pair.dstURL(dstContainer),
				mirrorSyncFlags())

			require.Equal(t, 0, exitCode, "azcopy sync failed")

			// Validate only subset remains at destination
			actual := pair.dstList(t, dstContainer)
			assertDestContains(t, actual, expectedKeys)
			assertNoLeadingSlash(t, actual)

			_ = stdout // stats logged above
		})
	}
}

// ---------------------------------------------------------------------------
// Scenario 4: Mirror Mode — Directory Deletion — All 5 C2C pairs
// ---------------------------------------------------------------------------

func TestSync_C2C_MirrorDirDelete(t *testing.T) {
	binary := buildAzCopy(t)
	fullDataset := mainDataset()

	// Source: only root-level files, no subdirectories
	rootOnlyFiles := map[string][]byte{
		"root_file1.txt": []byte("root1"),
		"root_file2.txt": []byte("root2"),
		"root_file3.txt": []byte("root3"),
		"dir1_extra.txt": []byte("dir1-extra-root"),
	}
	expectedKeys := datasetKeys(rootOnlyFiles)

	for _, pair := range allC2CPairs() {
		pair := pair
		t.Run(pair.name, func(t *testing.T) {
			t.Parallel()
			srcContainer := uniqueName("c2c-dirrel-src")
			dstContainer := uniqueName("c2c-dirdel-dst")
			defer pair.srcCleanup(t, srcContainer)
			defer pair.dstCleanup(t, dstContainer)

			// Source: root-level files only
			pair.srcSetup(t, srcContainer, rootOnlyFiles)

			// Destination: full dataset (includes dirs and nested content)
			pair.dstSetup(t, dstContainer, fullDataset)

			// Run sync with mirror mode
			_, _, exitCode := runAzCopySync(t, binary,
				pair.srcURL(srcContainer), pair.dstURL(dstContainer),
				mirrorSyncFlags())

			require.Equal(t, 0, exitCode, "azcopy sync failed")

			// Validate only root-level files remain
			actual := pair.dstList(t, dstContainer)
			assertDestContains(t, actual, expectedKeys)
			assertNoLeadingSlash(t, actual)
		})
	}
}

// ---------------------------------------------------------------------------
// Scenario 5: Nameless Dir Fresh Sync — Blob→Blob, S3→Blob only
// ---------------------------------------------------------------------------

func TestSync_C2C_NamelessDirFreshSync(t *testing.T) {
	binary := buildAzCopy(t)
	dataset := namelessDirDataset()
	expected := datasetKeys(dataset)

	for _, pair := range flatNamespacePairs() {
		pair := pair
		t.Run(pair.name, func(t *testing.T) {
			t.Parallel()
			srcContainer := uniqueName("c2c-nlfresh-src")
			dstContainer := uniqueName("c2c-nlfresh-dst")
			defer pair.srcCleanup(t, srcContainer)
			defer pair.dstCleanup(t, dstContainer)

			pair.srcSetup(t, srcContainer, dataset)
			pair.dstSetup(t, dstContainer, map[string][]byte{})

			stdout, _, exitCode := runAzCopySync(t, binary,
				pair.srcURL(srcContainer), pair.dstURL(dstContainer),
				defaultSyncFlags())

			require.Equal(t, 0, exitCode, "azcopy sync failed")

			actual := pair.dstList(t, dstContainer)
			assertDestContains(t, actual, expected)

			stats := parseSyncStats(stdout)
			assert.Equal(t, len(expected)+stats.CopyFolderTransfers, stats.CopyCompleted,
				"Expected %d file transfers", len(expected))
		})
	}
}

// ---------------------------------------------------------------------------
// Scenario 6: Nameless Dir Incremental Sync — Blob→Blob, S3→Blob only
// ---------------------------------------------------------------------------

func TestSync_C2C_NamelessDirIncrementalSync(t *testing.T) {
	binary := buildAzCopy(t)
	dataset := namelessDirDataset()
	expected := datasetKeys(dataset)

	modifiedFiles := []string{
		"a//nameless1.txt",
		"//root_nameless.txt",
		"s//mx.json",
	}

	for _, pair := range flatNamespacePairs() {
		pair := pair
		t.Run(pair.name, func(t *testing.T) {
			t.Parallel()
			srcContainer := uniqueName("c2c-nlincr-src")
			dstContainer := uniqueName("c2c-nlincr-dst")
			defer pair.srcCleanup(t, srcContainer)
			defer pair.dstCleanup(t, dstContainer)

			// Upload full to source
			pair.srcSetup(t, srcContainer, dataset)
			time.Sleep(2 * time.Second)

			// Upload full to dest (newer)
			pair.dstSetup(t, dstContainer, dataset)
			time.Sleep(2 * time.Second)

			// Re-upload 3 files to source
			pair.srcReupload(t, srcContainer, modifiedFiles, dataset)

			stdout, _, exitCode := runAzCopySync(t, binary,
				pair.srcURL(srcContainer), pair.dstURL(dstContainer),
				defaultSyncFlags())

			require.Equal(t, 0, exitCode, "azcopy sync failed")

			actual := pair.dstList(t, dstContainer)
			assertDestContains(t, actual, expected)

			stats := parseSyncStats(stdout)
			assert.Equal(t, len(modifiedFiles), stats.CopyFileTransfers,
				"Expected %d copy transfers scheduled", len(modifiedFiles))
			assert.Equal(t, len(modifiedFiles)+stats.CopyFolderTransfers, stats.CopyCompleted,
				"Expected %d files transferred", len(modifiedFiles))
		})
	}
}

// ---------------------------------------------------------------------------
// Scenario 7: Nameless Dir Mirror Delete — Blob→Blob, S3→Blob only
// ---------------------------------------------------------------------------

func TestSync_C2C_NamelessDirMirrorDelete(t *testing.T) {
	binary := buildAzCopy(t)
	fullDataset := namelessDirDataset()

	// Source: only files under named dirs (no nameless dirs)
	sourceFiles := map[string][]byte{
		"normal.txt":    []byte("normal"),
		"a/file.txt":    []byte("a-file"),
		"s/regular.txt": []byte("s-regular"),
	}
	expectedKeys := datasetKeys(sourceFiles)

	for _, pair := range flatNamespacePairs() {
		pair := pair
		t.Run(pair.name, func(t *testing.T) {
			t.Parallel()
			srcContainer := uniqueName("c2c-nldel-src")
			dstContainer := uniqueName("c2c-nldel-dst")
			defer pair.srcCleanup(t, srcContainer)
			defer pair.dstCleanup(t, dstContainer)

			pair.dstSetup(t, dstContainer, fullDataset)
			time.Sleep(5 * time.Second)
			pair.srcSetup(t, srcContainer, sourceFiles)

			_, _, exitCode := runAzCopySync(t, binary,
				pair.srcURL(srcContainer), pair.dstURL(dstContainer),
				mirrorSyncFlags())

			require.Equal(t, 0, exitCode, "azcopy sync failed")

			actual := pair.dstList(t, dstContainer)
			assertDestContains(t, actual, expectedKeys)
		})
	}
}

// ---------------------------------------------------------------------------
// Scenario 8: Deeply Nested Nameless Dirs — Blob→Blob only
// ---------------------------------------------------------------------------

func TestSync_C2C_DeepNamelessDirs(t *testing.T) {
	binary := buildAzCopy(t)
	dataset := deepNamelessDirDataset()
	expected := datasetKeys(dataset)

	for _, pair := range blobToBlobPair() {
		pair := pair
		t.Run(pair.name, func(t *testing.T) {
			t.Parallel()
			srcContainer := uniqueName("c2c-deepnl-src")
			dstContainer := uniqueName("c2c-deepnl-dst")
			defer pair.srcCleanup(t, srcContainer)
			defer pair.dstCleanup(t, dstContainer)

			pair.srcSetup(t, srcContainer, dataset)
			pair.dstSetup(t, dstContainer, map[string][]byte{})

			stdout, _, exitCode := runAzCopySync(t, binary,
				pair.srcURL(srcContainer), pair.dstURL(dstContainer),
				defaultSyncFlags())

			require.Equal(t, 0, exitCode, "azcopy sync failed")

			actual := pair.dstList(t, dstContainer)
			assertDestContains(t, actual, expected)

			stats := parseSyncStats(stdout)
			assert.Equal(t, len(expected)+stats.CopyFolderTransfers, stats.CopyCompleted,
				"Expected %d file transfers for deeply nested nameless dirs", len(expected))
			assert.Equal(t, 0, stats.CopyFailed, "Expected 0 failed")
		})
	}
}

// ---------------------------------------------------------------------------
// Scenario 9: Blob (with nameless dirs) → BlobFS — Blob→BlobFS pair
// ---------------------------------------------------------------------------

func TestSync_C2C_BlobNamelessToBlobFS(t *testing.T) {
	binary := buildAzCopy(t)
	srcDataset := crossTypeDataset()

	// HNS normalizes // → /
	expectedKeys := datasetKeys(crossTypeDatasetNormalized())

	for _, pair := range blobToBlobFSPair() {
		pair := pair
		t.Run(pair.name, func(t *testing.T) {
			t.Parallel()
			srcContainer := uniqueName("c2c-bfs-src")
			dstContainer := uniqueName("c2c-bfs-dst")
			defer pair.srcCleanup(t, srcContainer)
			defer pair.dstCleanup(t, dstContainer)

			pair.srcSetup(t, srcContainer, srcDataset)
			pair.dstSetup(t, dstContainer, map[string][]byte{})

			stdout, _, exitCode := runAzCopySync(t, binary,
				pair.srcURL(srcContainer), pair.dstURL(dstContainer),
				defaultSyncFlags())

			require.Equal(t, 0, exitCode, "azcopy sync failed")

			actual := pair.dstList(t, dstContainer)
			assertDestContains(t, actual, expectedKeys)

			stats := parseSyncStats(stdout)
			// All source files attempted transfer
			assert.Equal(t, len(srcDataset), stats.CopyFileTransfers,
				"Expected %d file transfers attempted", len(srcDataset))
		})
	}
}

// ---------------------------------------------------------------------------
// Scenario 10: BlobFS → Blob — BlobFS→Blob pair
// ---------------------------------------------------------------------------

func TestSync_C2C_BlobFSToBlob(t *testing.T) {
	binary := buildAzCopy(t)
	dataset := mainDataset()
	expected := datasetKeys(dataset)

	for _, pair := range blobFSToBlobPair() {
		pair := pair
		t.Run(pair.name, func(t *testing.T) {
			t.Parallel()
			srcContainer := uniqueName("c2c-fsb-src")
			dstContainer := uniqueName("c2c-fsb-dst")
			defer pair.srcCleanup(t, srcContainer)
			defer pair.dstCleanup(t, dstContainer)

			pair.srcSetup(t, srcContainer, dataset)
			pair.dstSetup(t, dstContainer, map[string][]byte{})

			stdout, _, exitCode := runAzCopySync(t, binary,
				pair.srcURL(srcContainer), pair.dstURL(dstContainer),
				defaultSyncFlags())

			require.Equal(t, 0, exitCode, "azcopy sync failed")

			actual := pair.dstList(t, dstContainer)
			assertDestContains(t, actual, expected)
			assertNoLeadingSlash(t, actual)

			stats := parseSyncStats(stdout)
			assert.Equal(t, len(expected)+stats.CopyFolderTransfers, stats.CopyCompleted,
				"Expected %d file transfers", len(expected))
		})
	}
}

// ---------------------------------------------------------------------------
// Scenario 10a: BlobFS → Blob Merge with Nameless Dirs at Dest — BlobFS→Blob
// Destination already has nameless dirs; merge should preserve them.
// ---------------------------------------------------------------------------

func TestSync_C2C_BlobFSToBlobNamelessMerge(t *testing.T) {
	binary := buildAzCopy(t)

	// Source (BlobFS): only named-dir files
	sourceFiles := map[string][]byte{
		"normal.txt":    []byte("normal-updated"),
		"a/file.txt":    []byte("a-file-updated"),
		"s/regular.txt": []byte("s-regular-updated"),
	}

	// Destination (Blob): full nameless dir dataset
	dstDataset := namelessDirDataset()
	expectedKeys := datasetKeys(dstDataset) // all 8 files should remain

	for _, pair := range blobFSToBlobPair() {
		pair := pair
		t.Run(pair.name, func(t *testing.T) {
			t.Parallel()
			srcContainer := uniqueName("c2c-fsbns-src")
			dstContainer := uniqueName("c2c-fsbns-dst")
			defer pair.srcCleanup(t, srcContainer)
			defer pair.dstCleanup(t, dstContainer)

			// Destination first (older)
			pair.dstSetup(t, dstContainer, dstDataset)
			time.Sleep(2 * time.Second)

			// Source second (newer)
			pair.srcSetup(t, srcContainer, sourceFiles)

			stdout, _, exitCode := runAzCopySync(t, binary,
				pair.srcURL(srcContainer), pair.dstURL(dstContainer),
				defaultSyncFlags())

			require.Equal(t, 0, exitCode, "azcopy sync failed")

			actual := pair.dstList(t, dstContainer)
			assertDestContains(t, actual, expectedKeys)

			// Source files should be transferred (newer)
			stats := parseSyncStats(stdout)
			assert.Equal(t, len(sourceFiles), stats.CopyFileTransfers,
				"Expected %d file transfers", len(sourceFiles))
			assert.Equal(t, 0, stats.CopyFailed, "Expected 0 failed transfers")
		})
	}
}

// ---------------------------------------------------------------------------
// Scenario 10b: BlobFS → Blob Mirror with Nameless Dirs at Dest — BlobFS→Blob
// Destination has nameless dirs; mirror should delete them, keep matching files.
// ---------------------------------------------------------------------------

func TestSync_C2C_BlobFSToBlobNamelessMirrorDelete(t *testing.T) {
	binary := buildAzCopy(t)

	// Source (BlobFS): only named-dir files
	sourceFiles := map[string][]byte{
		"normal.txt":    []byte("normal"),
		"a/file.txt":    []byte("a-file"),
		"s/regular.txt": []byte("s-regular"),
	}
	expectedKeys := datasetKeys(sourceFiles)

	// Destination (Blob): full nameless dir dataset
	dstDataset := namelessDirDataset()

	for _, pair := range blobFSToBlobPair() {
		pair := pair
		t.Run(pair.name, func(t *testing.T) {
			t.Parallel()
			srcContainer := uniqueName("c2c-fsbnm-src")
			dstContainer := uniqueName("c2c-fsbnm-dst")
			defer pair.srcCleanup(t, srcContainer)
			defer pair.dstCleanup(t, dstContainer)

			pair.srcSetup(t, srcContainer, sourceFiles)
			pair.dstSetup(t, dstContainer, dstDataset)

			_, _, exitCode := runAzCopySync(t, binary,
				pair.srcURL(srcContainer), pair.dstURL(dstContainer),
				mirrorSyncFlags())

			require.Equal(t, 0, exitCode, "azcopy sync failed")

			actual := pair.dstList(t, dstContainer)
			assertDestContains(t, actual, expectedKeys)
		})
	}
}

// ---------------------------------------------------------------------------
// Scenario 11: Blob (nameless) → BlobFS Mirror Delete — Blob→BlobFS pair
// ---------------------------------------------------------------------------

func TestSync_C2C_BlobNamelessToBlobFSMirrorDelete(t *testing.T) {
	binary := buildAzCopy(t)

	// First sync full cross-type dataset to BlobFS to populate destination
	srcDataset := crossTypeDataset()
	normalizedDataset := crossTypeDatasetNormalized()

	// Then sync with only one file to trigger deletions
	mirrorSrcFiles := map[string][]byte{
		"proj/src/main.go": []byte("package main"),
	}
	expectedKeys := datasetKeys(mirrorSrcFiles)

	for _, pair := range blobToBlobFSPair() {
		pair := pair
		t.Run(pair.name, func(t *testing.T) {
			t.Parallel()
			srcContainer := uniqueName("c2c-bfsdel-src")
			dstContainer := uniqueName("c2c-bfsdel-dst")
			defer pair.srcCleanup(t, srcContainer)
			defer pair.dstCleanup(t, dstContainer)

			// Step 1: Populate destination with normalized dataset directly
			pair.dstSetup(t, dstContainer, normalizedDataset)

			// Verify destination is populated
			actual := pair.dstList(t, dstContainer)
			assert.Len(t, actual, len(normalizedDataset),
				"Destination should be populated before mirror sync")

			// Step 2: Set up source with only one file
			pair.srcSetup(t, srcContainer, mirrorSrcFiles)

			// Step 3: Mirror sync — should delete extras from BlobFS
			_, _, exitCode := runAzCopySync(t, binary,
				pair.srcURL(srcContainer), pair.dstURL(dstContainer),
				mirrorSyncFlags())

			require.Equal(t, 0, exitCode, "azcopy sync failed")

			actual = pair.dstList(t, dstContainer)
			assertDestContains(t, actual, expectedKeys)

			_ = srcDataset // referenced for context
		})
	}
}

// ---------------------------------------------------------------------------
// Scenario 12: Root-Only Files — Blob→Blob only
// ---------------------------------------------------------------------------

func TestSync_C2C_RootOnlyFiles(t *testing.T) {
	binary := buildAzCopy(t)
	dataset := rootOnlyDataset()
	expected := datasetKeys(dataset)

	for _, pair := range blobToBlobPair() {
		pair := pair
		t.Run(pair.name, func(t *testing.T) {
			t.Parallel()
			srcContainer := uniqueName("c2c-rootonly-src")
			dstContainer := uniqueName("c2c-rootonly-dst")
			defer pair.srcCleanup(t, srcContainer)
			defer pair.dstCleanup(t, dstContainer)

			pair.srcSetup(t, srcContainer, dataset)
			pair.dstSetup(t, dstContainer, map[string][]byte{})

			stdout, _, exitCode := runAzCopySync(t, binary,
				pair.srcURL(srcContainer), pair.dstURL(dstContainer),
				defaultSyncFlags())

			require.Equal(t, 0, exitCode, "azcopy sync failed")

			actual := pair.dstList(t, dstContainer)
			assertDestContains(t, actual, expected)
			assertNoLeadingSlash(t, actual)

			stats := parseSyncStats(stdout)
			assert.Equal(t, len(expected)+stats.CopyFolderTransfers, stats.CopyCompleted,
				"Expected %d file transfers", len(expected))
		})
	}
}

// ---------------------------------------------------------------------------
// Scenario 13: Prefix Collision Safety — Blob→Blob only
// dir1_extra.txt should NOT be confused with dir1/ prefix
// ---------------------------------------------------------------------------

func TestSync_C2C_PrefixCollision(t *testing.T) {
	binary := buildAzCopy(t)

	// Dataset with collision: dir1/ directory + dir1_extra.txt at root
	dataset := map[string][]byte{
		"dir1/file_a.txt": []byte("dir1-a"),
		"dir1/file_b.txt": []byte("dir1-b"),
		"dir1_extra.txt":  []byte("dir1-extra-root"),
		"dir1_suffix.txt": []byte("dir1-suffix"),
	}
	expected := datasetKeys(dataset)

	for _, pair := range blobToBlobPair() {
		pair := pair
		t.Run(pair.name, func(t *testing.T) {
			t.Parallel()
			srcContainer := uniqueName("c2c-prefix-src")
			dstContainer := uniqueName("c2c-prefix-dst")
			defer pair.srcCleanup(t, srcContainer)
			defer pair.dstCleanup(t, dstContainer)

			pair.srcSetup(t, srcContainer, dataset)
			pair.dstSetup(t, dstContainer, map[string][]byte{})

			stdout, _, exitCode := runAzCopySync(t, binary,
				pair.srcURL(srcContainer), pair.dstURL(dstContainer),
				defaultSyncFlags())

			require.Equal(t, 0, exitCode, "azcopy sync failed")

			actual := pair.dstList(t, dstContainer)
			assertDestContains(t, actual, expected)

			stats := parseSyncStats(stdout)
			assert.Equal(t, len(expected)+stats.CopyFolderTransfers, stats.CopyCompleted,
				"Expected %d file transfers", len(expected))

			// Specifically verify dir1_extra.txt is at root, not under dir1/
			assert.Contains(t, actual, "dir1_extra.txt",
				"dir1_extra.txt should be at root level")
			assert.Contains(t, actual, "dir1_suffix.txt",
				"dir1_suffix.txt should be at root level")
			// Ensure no mangled paths
			for _, name := range actual {
				assert.False(t, strings.HasPrefix(name, "dir1/dir1_extra"),
					"dir1_extra.txt should NOT be under dir1/")
				assert.False(t, strings.HasPrefix(name, "dir1/dir1_suffix"),
					"dir1_suffix.txt should NOT be under dir1/")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Scenario 14: Mixed Named + Nameless Dirs — Blob→Blob only
// ---------------------------------------------------------------------------

func TestSync_C2C_MixedNamedNamelessDirs(t *testing.T) {
	binary := buildAzCopy(t)
	dataset := mixedDirDataset()
	expected := datasetKeys(dataset)

	for _, pair := range blobToBlobPair() {
		pair := pair
		t.Run(pair.name, func(t *testing.T) {
			t.Parallel()
			srcContainer := uniqueName("c2c-mixed-src")
			dstContainer := uniqueName("c2c-mixed-dst")
			defer pair.srcCleanup(t, srcContainer)
			defer pair.dstCleanup(t, dstContainer)

			pair.srcSetup(t, srcContainer, dataset)
			pair.dstSetup(t, dstContainer, map[string][]byte{})

			stdout, _, exitCode := runAzCopySync(t, binary,
				pair.srcURL(srcContainer), pair.dstURL(dstContainer),
				defaultSyncFlags())

			require.Equal(t, 0, exitCode, "azcopy sync failed")

			actual := pair.dstList(t, dstContainer)
			assertDestContains(t, actual, expected)

			stats := parseSyncStats(stdout)
			assert.Equal(t, len(expected)+stats.CopyFolderTransfers, stats.CopyCompleted,
				"Expected %d file transfers for mixed dir types", len(expected))
		})
	}
}

// ===========================================================================
// Blob upload helper with directory stubs (for use with block blob client)
// ===========================================================================

// setupBlobDataWithStubs uploads blobs and creates directory stub blobs
// (zero-byte blobs with hdi_isfolder=true metadata) for specified directories.
func setupBlobDataWithStubs(t *testing.T, account, containerName string, files map[string][]byte, stubDirs []string) {
	t.Helper()
	ctx := context.Background()

	cred, err := azidentity.NewAzureCLICredential(nil)
	require.NoError(t, err)

	containerURL := fmt.Sprintf("https://%s.blob.core.windows.net/%s", account, containerName)
	containerClient, err := container.NewClient(containerURL, cred, nil)
	require.NoError(t, err)

	// Create container (ignore if exists)
	_, err = containerClient.Create(ctx, nil)
	if err != nil && !strings.Contains(err.Error(), "ContainerAlreadyExists") {
		require.NoError(t, err)
	}

	// Upload files
	for name, data := range files {
		bbClient := containerClient.NewBlockBlobClient(name)
		_, err := bbClient.UploadBuffer(ctx, data, &blockblob.UploadBufferOptions{})
		require.NoError(t, err, "upload failed for %s", name)
	}

	// Create directory stubs
	for _, dir := range stubDirs {
		bbClient := containerClient.NewBlockBlobClient(dir)
		_, err := bbClient.UploadBuffer(ctx, []byte{}, &blockblob.UploadBufferOptions{
			Metadata: map[string]*string{
				"hdi_isfolder": strPtr("true"),
			},
		})
		require.NoError(t, err, "stub creation failed for %s", dir)
	}
}

func strPtr(s string) *string { return &s }

// ===========================================================================
// Mirror Delete with Old Source — source files older than destination
// ===========================================================================
//
// These tests ensure mirror delete works correctly when source files are OLDER
// than destination files. Since LMT comparison skips the overlapping files
// (no copy), only deletions occur. This catches over-deletion bugs that are
// masked when copies happen to restore incorrectly deleted files.

// ---------------------------------------------------------------------------
// Nameless Dir Mirror Delete with Old Source — flat namespace pairs
// ---------------------------------------------------------------------------

func TestSync_C2C_NamelessDirMirrorDeleteOldSource(t *testing.T) {
	binary := buildAzCopy(t)
	fullDataset := namelessDirDataset()

	// Source: only files under named dirs (no nameless dirs)
	sourceFiles := map[string][]byte{
		"normal.txt":    []byte("normal"),
		"a/file.txt":    []byte("a-file"),
		"s/regular.txt": []byte("s-regular"),
	}
	expectedKeys := datasetKeys(sourceFiles)

	for _, pair := range flatNamespacePairs() {
		pair := pair
		t.Run(pair.name, func(t *testing.T) {
			t.Parallel()
			srcContainer := uniqueName("c2c-nldos-src")
			dstContainer := uniqueName("c2c-nldos-dst")
			defer pair.srcCleanup(t, srcContainer)
			defer pair.dstCleanup(t, dstContainer)

			// Upload source FIRST so it has older LMT
			pair.srcSetup(t, srcContainer, sourceFiles)
			time.Sleep(5 * time.Second)
			// Upload dest SECOND so overlapping files are newer → skipped (no copy)
			pair.dstSetup(t, dstContainer, fullDataset)

			stdout, _, exitCode := runAzCopySync(t, binary,
				pair.srcURL(srcContainer), pair.dstURL(dstContainer),
				mirrorSyncFlags())

			require.Equal(t, 0, exitCode, "azcopy sync failed")

			actual := pair.dstList(t, dstContainer)
			assertDestContains(t, actual, expectedKeys)

			stats := parseSyncStats(stdout)
			assert.Equal(t, 0, stats.CopyFileTransfers,
				"Expected 0 file transfers (source is older)")
		})
	}
}

// ---------------------------------------------------------------------------
// BlobFS→Blob Nameless Dir Mirror Delete with Old Source
// ---------------------------------------------------------------------------

func TestSync_C2C_BlobFSToBlobNamelessMirrorDeleteOldSource(t *testing.T) {
	binary := buildAzCopy(t)

	// Source (BlobFS): only named-dir files
	sourceFiles := map[string][]byte{
		"normal.txt":    []byte("normal"),
		"a/file.txt":    []byte("a-file"),
		"s/regular.txt": []byte("s-regular"),
	}
	expectedKeys := datasetKeys(sourceFiles)

	// Destination (Blob): full nameless dir dataset
	dstDataset := namelessDirDataset()

	for _, pair := range blobFSToBlobPair() {
		pair := pair
		t.Run(pair.name, func(t *testing.T) {
			t.Parallel()
			srcContainer := uniqueName("c2c-fbnos-src")
			dstContainer := uniqueName("c2c-fbnos-dst")
			defer pair.srcCleanup(t, srcContainer)
			defer pair.dstCleanup(t, dstContainer)

			// Upload source FIRST so it has older LMT
			pair.srcSetup(t, srcContainer, sourceFiles)
			time.Sleep(5 * time.Second)
			// Upload dest SECOND so overlapping files are newer → skipped (no copy)
			pair.dstSetup(t, dstContainer, dstDataset)

			stdout, _, exitCode := runAzCopySync(t, binary,
				pair.srcURL(srcContainer), pair.dstURL(dstContainer),
				mirrorSyncFlags())

			require.Equal(t, 0, exitCode, "azcopy sync failed")

			actual := pair.dstList(t, dstContainer)
			assertDestContains(t, actual, expectedKeys)

			stats := parseSyncStats(stdout)
			assert.Equal(t, 0, stats.CopyFileTransfers,
				"Expected 0 file transfers (source is older)")
		})
	}
}

// ---------------------------------------------------------------------------
// Main Dataset Mirror Delete with Old Source — all C2C pairs
// ---------------------------------------------------------------------------

func TestSync_C2C_MirrorFileDeleteOldSource(t *testing.T) {
	binary := buildAzCopy(t)
	fullDataset := mainDataset()

	// Source: subset of files; dest has the full dataset with newer LMTs
	sourceFiles := map[string][]byte{
		"root_file1.txt":         []byte("root1"),
		"dir1/file_a.txt":        []byte("file-a"),
		"dir1/sub1/deep_file.txt": []byte("deep"),
	}
	expectedKeys := datasetKeys(sourceFiles)

	for _, pair := range allC2CPairs() {
		pair := pair
		t.Run(pair.name, func(t *testing.T) {
			t.Parallel()
			srcContainer := uniqueName("c2c-mfdos-src")
			dstContainer := uniqueName("c2c-mfdos-dst")
			defer pair.srcCleanup(t, srcContainer)
			defer pair.dstCleanup(t, dstContainer)

			// Upload source FIRST so it has older LMT
			pair.srcSetup(t, srcContainer, sourceFiles)
			time.Sleep(5 * time.Second)
			// Upload dest SECOND so overlapping files are newer → skipped (no copy)
			pair.dstSetup(t, dstContainer, fullDataset)

			stdout, _, exitCode := runAzCopySync(t, binary,
				pair.srcURL(srcContainer), pair.dstURL(dstContainer),
				mirrorSyncFlags())

			require.Equal(t, 0, exitCode, "azcopy sync failed")

			actual := pair.dstList(t, dstContainer)
			assertDestContains(t, actual, expectedKeys)

			stats := parseSyncStats(stdout)
			assert.Equal(t, 0, stats.CopyFileTransfers,
				"Expected 0 file transfers (source is older)")
		})
	}
}

// ===========================================================================
// Subpath Sync Tests
// ===========================================================================

// ---------------------------------------------------------------------------
// Subpath Fresh Sync — sync srcContainer/dir1 → dstContainer/dir1
// ---------------------------------------------------------------------------

func TestSync_C2C_SubpathFreshSync(t *testing.T) {
	binary := buildAzCopy(t)
	dataset := mainDataset()
	dir1Keys := filterByPrefix(datasetKeys(dataset), "dir1")
	expectedDst := stripPrefix(dir1Keys, "dir1")

	for _, pair := range allC2CPairs() {
		pair := pair
		t.Run(pair.name, func(t *testing.T) {
			t.Parallel()
			srcContainer := uniqueName("c2c-spfresh-src")
			dstContainer := uniqueName("c2c-spfresh-dst")
			defer pair.srcCleanup(t, srcContainer)
			defer pair.dstCleanup(t, dstContainer)

			pair.srcSetup(t, srcContainer, dataset)
			pair.dstSetup(t, dstContainer, map[string][]byte{})

			stdout, _, exitCode := runAzCopySync(t, binary,
				pair.srcURL(srcContainer)+"/dir1",
				pair.dstURL(dstContainer)+"/dir1",
				defaultSyncFlags())

			require.Equal(t, 0, exitCode, "azcopy sync failed")

			actual := pair.dstList(t, dstContainer)
			// Files should appear under dir1/ prefix at destination
			expectedFull := make([]string, len(expectedDst))
			for i, k := range expectedDst {
				expectedFull[i] = "dir1/" + k
			}
			sort.Strings(expectedFull)
			assertDestContains(t, actual, expectedFull)
			assertNoLeadingSlash(t, actual)

			stats := parseSyncStats(stdout)
			assert.Equal(t, len(dir1Keys)+stats.CopyFolderTransfers, stats.CopyCompleted,
				"Expected %d file transfers", len(dir1Keys))
			assert.Equal(t, 0, stats.CopyFailed, "Expected 0 failed transfers")
		})
	}
}

// ---------------------------------------------------------------------------
// Subpath Incremental Sync — both have full dataset, re-upload dir1/ subset
// ---------------------------------------------------------------------------

func TestSync_C2C_SubpathIncrementalSync(t *testing.T) {
	binary := buildAzCopy(t)
	dataset := mainDataset()
	allKeys := datasetKeys(dataset)

	modifiedFiles := []string{
		"dir1/file_a.txt",
		"dir1/sub1/deep_file.txt",
		"dir1/sub1/sub2/level4.txt",
	}

	for _, pair := range allC2CPairs() {
		pair := pair
		t.Run(pair.name, func(t *testing.T) {
			t.Parallel()
			srcContainer := uniqueName("c2c-spincr-src")
			dstContainer := uniqueName("c2c-spincr-dst")
			defer pair.srcCleanup(t, srcContainer)
			defer pair.dstCleanup(t, dstContainer)

			// Upload full dataset to source
			pair.srcSetup(t, srcContainer, dataset)
			time.Sleep(2 * time.Second)

			// Upload full dataset to destination (dest is newer)
			pair.dstSetup(t, dstContainer, dataset)
			time.Sleep(2 * time.Second)

			// Re-upload dir1/ subset at source (source now newer for these)
			pair.srcReupload(t, srcContainer, modifiedFiles, dataset)

			// Sync only dir1/ subpath
			stdout, _, exitCode := runAzCopySync(t, binary,
				pair.srcURL(srcContainer)+"/dir1",
				pair.dstURL(dstContainer)+"/dir1",
				defaultSyncFlags())

			require.Equal(t, 0, exitCode, "azcopy sync failed")

			// Destination should still have all files (unchanged outside dir1/)
			actual := pair.dstList(t, dstContainer)
			assertDestContains(t, actual, allKeys)

			// Only modified files should transfer
			stats := parseSyncStats(stdout)
			assert.Equal(t, len(modifiedFiles), stats.CopyFileTransfers,
				"Expected %d copy transfers scheduled", len(modifiedFiles))
		})
	}
}

// ---------------------------------------------------------------------------
// Subpath Mirror Delete — only dir1/ scoped, rest untouched
// ---------------------------------------------------------------------------

func TestSync_C2C_SubpathMirrorDelete(t *testing.T) {
	binary := buildAzCopy(t)
	dataset := mainDataset()
	allKeys := datasetKeys(dataset)

	// Source: only a subset of dir1/ files
	srcDir1Files := map[string][]byte{
		"dir1/file_a.txt":         []byte("dir1-a"),
		"dir1/sub1/deep_file.txt": []byte("deep1"),
	}

	for _, pair := range allC2CPairs() {
		pair := pair
		t.Run(pair.name, func(t *testing.T) {
			t.Parallel()
			srcContainer := uniqueName("c2c-spmirdel-src")
			dstContainer := uniqueName("c2c-spmirdel-dst")
			defer pair.srcCleanup(t, srcContainer)
			defer pair.dstCleanup(t, dstContainer)

			// Source: only subset of dir1/
			pair.srcSetup(t, srcContainer, srcDir1Files)
			// Destination: full dataset
			pair.dstSetup(t, dstContainer, dataset)

			// Sync with mirror mode: srcContainer/dir1 → dstContainer/dir1
			_, _, exitCode := runAzCopySync(t, binary,
				pair.srcURL(srcContainer)+"/dir1",
				pair.dstURL(dstContainer)+"/dir1",
				mirrorSyncFlags())

			require.Equal(t, 0, exitCode, "azcopy sync failed")

			actual := pair.dstList(t, dstContainer)

			// dir1/ should only have the source subset
			srcDir1Keys := datasetKeys(srcDir1Files)
			// Files outside dir1/ should be untouched
			outsideDir1 := filterOutPrefix(allKeys, "dir1")
			expected := append(srcDir1Keys, outsideDir1...)
			sort.Strings(expected)

			assertDestContains(t, actual, expected)
			assertNoLeadingSlash(t, actual)
		})
	}
}

// ---------------------------------------------------------------------------
// Subpath Deep Sync — sync srcContainer/dir1/sub1 → dstContainer/dir1/sub1
// ---------------------------------------------------------------------------

func TestSync_C2C_SubpathDeepSync(t *testing.T) {
	binary := buildAzCopy(t)
	dataset := mainDataset()
	sub1Keys := filterByPrefix(datasetKeys(dataset), "dir1/sub1")
	expectedDst := make([]string, len(sub1Keys))
	copy(expectedDst, sub1Keys)
	sort.Strings(expectedDst)

	for _, pair := range allC2CPairs() {
		pair := pair
		t.Run(pair.name, func(t *testing.T) {
			t.Parallel()
			srcContainer := uniqueName("c2c-spdeep-src")
			dstContainer := uniqueName("c2c-spdeep-dst")
			defer pair.srcCleanup(t, srcContainer)
			defer pair.dstCleanup(t, dstContainer)

			pair.srcSetup(t, srcContainer, dataset)
			pair.dstSetup(t, dstContainer, map[string][]byte{})

			stdout, _, exitCode := runAzCopySync(t, binary,
				pair.srcURL(srcContainer)+"/dir1/sub1",
				pair.dstURL(dstContainer)+"/dir1/sub1",
				defaultSyncFlags())

			require.Equal(t, 0, exitCode, "azcopy sync failed")

			actual := pair.dstList(t, dstContainer)
			assertDestContains(t, actual, expectedDst)
			assertNoLeadingSlash(t, actual)

			stats := parseSyncStats(stdout)
			assert.Equal(t, len(sub1Keys)+stats.CopyFolderTransfers, stats.CopyCompleted,
				"Expected %d file transfers", len(sub1Keys))
			assert.Equal(t, 0, stats.CopyFailed, "Expected 0 failed transfers")
		})
	}
}

// ---------------------------------------------------------------------------
// Subpath Prefix Collision — dir1/ must not include dir10/ or dir1_extra.txt
// ---------------------------------------------------------------------------

func TestSync_C2C_SubpathPrefixCollision(t *testing.T) {
	binary := buildAzCopy(t)
	dataset := mainDataset()
	dir1Keys := filterByPrefix(datasetKeys(dataset), "dir1")

	for _, pair := range allC2CPairs() {
		pair := pair
		t.Run(pair.name, func(t *testing.T) {
			t.Parallel()
			srcContainer := uniqueName("c2c-sppfx-src")
			dstContainer := uniqueName("c2c-sppfx-dst")
			defer pair.srcCleanup(t, srcContainer)
			defer pair.dstCleanup(t, dstContainer)

			pair.srcSetup(t, srcContainer, dataset)
			pair.dstSetup(t, dstContainer, map[string][]byte{})

			_, _, exitCode := runAzCopySync(t, binary,
				pair.srcURL(srcContainer)+"/dir1",
				pair.dstURL(dstContainer)+"/dir1",
				defaultSyncFlags())

			require.Equal(t, 0, exitCode, "azcopy sync failed")

			actual := pair.dstList(t, dstContainer)

			// Expected: only dir1/ files at destination
			expectedFull := make([]string, len(dir1Keys))
			copy(expectedFull, dir1Keys)
			sort.Strings(expectedFull)
			assertDestContains(t, actual, expectedFull)

			// Verify dir10/ and dir1_extra.txt are NOT present
			for _, name := range actual {
				assert.False(t, strings.HasPrefix(name, "dir10/"),
					"dir10/ should NOT be synced when syncing dir1/: %s", name)
				assert.NotEqual(t, "dir1_extra.txt", name,
					"dir1_extra.txt should NOT be synced when syncing dir1/")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Subpath Unicode Sync — sync srcContainer/打麻将 → dstContainer/打麻将
// ---------------------------------------------------------------------------

func TestSync_C2C_SubpathUnicodeSync(t *testing.T) {
	binary := buildAzCopy(t)
	dataset := mainDataset()
	unicodeKeys := filterByPrefix(datasetKeys(dataset), "打麻将")

	for _, pair := range allC2CPairs() {
		pair := pair
		t.Run(pair.name, func(t *testing.T) {
			t.Parallel()
			srcContainer := uniqueName("c2c-spuni-src")
			dstContainer := uniqueName("c2c-spuni-dst")
			defer pair.srcCleanup(t, srcContainer)
			defer pair.dstCleanup(t, dstContainer)

			pair.srcSetup(t, srcContainer, dataset)
			pair.dstSetup(t, dstContainer, map[string][]byte{})

			stdout, _, exitCode := runAzCopySync(t, binary,
				pair.srcURL(srcContainer)+"/打麻将",
				pair.dstURL(dstContainer)+"/打麻将",
				defaultSyncFlags())

			require.Equal(t, 0, exitCode, "azcopy sync failed")

			actual := pair.dstList(t, dstContainer)
			assertDestContains(t, actual, unicodeKeys)
			assertNoLeadingSlash(t, actual)

			stats := parseSyncStats(stdout)
			assert.Equal(t, len(unicodeKeys)+stats.CopyFolderTransfers, stats.CopyCompleted,
				"Expected %d file transfers", len(unicodeKeys))
			assert.Equal(t, 0, stats.CopyFailed, "Expected 0 failed transfers")
		})
	}
}

// ---------------------------------------------------------------------------
// Cross-Subpath Sync — sync srcContainer/dir1 → dstContainer/other_dir
// ---------------------------------------------------------------------------

func TestSync_C2C_CrossSubpathSync(t *testing.T) {
	binary := buildAzCopy(t)
	dataset := mainDataset()
	dir1Keys := filterByPrefix(datasetKeys(dataset), "dir1")
	// Files from dir1/ should appear under other_dir/ with dir1/ prefix stripped
	strippedKeys := stripPrefix(dir1Keys, "dir1")
	expectedFull := make([]string, len(strippedKeys))
	for i, k := range strippedKeys {
		expectedFull[i] = "other_dir/" + k
	}
	sort.Strings(expectedFull)

	for _, pair := range allC2CPairs() {
		pair := pair
		t.Run(pair.name, func(t *testing.T) {
			t.Parallel()
			srcContainer := uniqueName("c2c-crosssp-src")
			dstContainer := uniqueName("c2c-crosssp-dst")
			defer pair.srcCleanup(t, srcContainer)
			defer pair.dstCleanup(t, dstContainer)

			pair.srcSetup(t, srcContainer, dataset)
			pair.dstSetup(t, dstContainer, map[string][]byte{})

			stdout, _, exitCode := runAzCopySync(t, binary,
				pair.srcURL(srcContainer)+"/dir1",
				pair.dstURL(dstContainer)+"/other_dir",
				defaultSyncFlags())

			require.Equal(t, 0, exitCode, "azcopy sync failed")

			actual := pair.dstList(t, dstContainer)
			assertDestContains(t, actual, expectedFull)
			assertNoLeadingSlash(t, actual)

			stats := parseSyncStats(stdout)
			assert.Equal(t, len(dir1Keys)+stats.CopyFolderTransfers, stats.CopyCompleted,
				"Expected %d file transfers", len(dir1Keys))
			assert.Equal(t, 0, stats.CopyFailed, "Expected 0 failed transfers")
		})
	}
}

// ---------------------------------------------------------------------------
// Cross-Subpath Mirror Delete — srcContainer/dir1 → dstContainer/other_dir
// Files outside other_dir/ should be untouched.
// ---------------------------------------------------------------------------

func TestSync_C2C_CrossSubpathMirrorDelete(t *testing.T) {
	binary := buildAzCopy(t)

	// Source: subset of dir1/ files
	srcDir1Files := map[string][]byte{
		"dir1/file_a.txt":         []byte("dir1-a"),
		"dir1/sub1/deep_file.txt": []byte("deep1"),
	}
	srcDir1Keys := datasetKeys(srcDir1Files)
	strippedSrcKeys := stripPrefix(srcDir1Keys, "dir1")

	// Destination: pre-populate other_dir/ with more files + files elsewhere
	dstFiles := map[string][]byte{
		"other_dir/file_a.txt":          []byte("old-a"),
		"other_dir/file_b.txt":          []byte("old-b"),
		"other_dir/sub1/deep_file.txt":  []byte("old-deep"),
		"other_dir/sub1/deep_file2.txt": []byte("old-deep2"),
		"outside/untouched.txt":         []byte("untouched"),
		"root_untouched.txt":            []byte("root-untouched"),
	}

	for _, pair := range allC2CPairs() {
		pair := pair
		t.Run(pair.name, func(t *testing.T) {
			t.Parallel()
			srcContainer := uniqueName("c2c-crossmd-src")
			dstContainer := uniqueName("c2c-crossmd-dst")
			defer pair.srcCleanup(t, srcContainer)
			defer pair.dstCleanup(t, dstContainer)

			pair.srcSetup(t, srcContainer, srcDir1Files)
			pair.dstSetup(t, dstContainer, dstFiles)

			_, _, exitCode := runAzCopySync(t, binary,
				pair.srcURL(srcContainer)+"/dir1",
				pair.dstURL(dstContainer)+"/other_dir",
				mirrorSyncFlags())

			require.Equal(t, 0, exitCode, "azcopy sync failed")

			actual := pair.dstList(t, dstContainer)

			// other_dir/ should mirror source dir1/ content
			expectedOtherDir := make([]string, len(strippedSrcKeys))
			for i, k := range strippedSrcKeys {
				expectedOtherDir[i] = "other_dir/" + k
			}
			// Files outside other_dir/ should be untouched
			outsideKeys := filterOutPrefix(datasetKeys(dstFiles), "other_dir")
			expected := append(expectedOtherDir, outsideKeys...)
			sort.Strings(expected)

			assertDestContains(t, actual, expected)
			assertNoLeadingSlash(t, actual)
		})
	}
}
