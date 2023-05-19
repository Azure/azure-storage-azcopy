package azbfs_test

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/azbfs"
)

const (
	fileSystemPrefix = "go"
	directoryPrefix  = "gotestdirectory"
	filePrefix       = "gotestfile"
)

var ctx = context.Background()

func getAccountAndKey() (string, string) {
	name := os.Getenv("ACCOUNT_NAME")
	key := os.Getenv("ACCOUNT_KEY")
	if name == "" || key == "" {
		panic("ACCOUNT_NAME and ACCOUNT_KEY environment vars must be set before running tests")
	}

	return name, key
}

func getBfsServiceURL() azbfs.ServiceURL {
	name, key := getAccountAndKey()
	u, _ := url.Parse(fmt.Sprintf("https://%s.dfs.core.windows.net/", name))

	credential := azbfs.NewSharedKeyCredential(name, key)
	pipeline := azbfs.NewPipeline(credential, azbfs.PipelineOptions{})
	return azbfs.NewServiceURL(*u, pipeline)
}

// This function generates an entity name by concatenating the passed prefix,
// the name of the test requesting the entity name, and the minute, second, and nanoseconds of the call.
// This should make it easy to associate the entities with their test, uniquely identify
// them, and determine the order in which they were created.
// Note that this imposes a restriction on the length of test names
func generateName(prefix string) string {
	// These next lines up through the for loop are obtaining and walking up the stack
	// trace to extract the test name, which is stored in name
	pc := make([]uintptr, 10)
	runtime.Callers(0, pc)
	f := runtime.FuncForPC(pc[0])
	name := f.Name()
	for i := 0; !strings.HasPrefix(name, "Suite"); i++ { // The tests are all scoped to the suite, so this ensures getting the actual test name
		f = runtime.FuncForPC(pc[i])
		name = f.Name()
	}
	funcNameStart := strings.Index(name, "Test")
	name = name[funcNameStart+len("Test"):] // Just get the name of the test and not any of the garbage at the beginning
	name = strings.ToLower(name)            // Ensure it is a valid resource name
	currentTime := time.Now()
	name = fmt.Sprintf("%s%s%d%d%d", prefix, strings.ToLower(name), currentTime.Minute(), currentTime.Second(), currentTime.Nanosecond())
	return name
}

func generateFileSystemName() string {
	return generateName(fileSystemPrefix)
}

func generateDirectoryName() string {
	return generateName(directoryPrefix)
}

func generateFileName() string {
	return generateName(filePrefix)
}

func getFileSystemURL(a *assert.Assertions, fsu azbfs.ServiceURL) (fs azbfs.FileSystemURL, name string) {
	name = generateFileSystemName()
	fs = fsu.NewFileSystemURL(name)

	return fs, name
}

func getDirectoryURLFromFileSystem(a *assert.Assertions, fs azbfs.FileSystemURL) (directory azbfs.DirectoryURL, name string) {
	name = generateDirectoryName()
	directory = fs.NewDirectoryURL(name)
	return directory, name
}

func getDirectoryURLFromDirectory(a *assert.Assertions, parentDirectory azbfs.DirectoryURL) (directory azbfs.DirectoryURL, name string) {
	name = generateDirectoryName()
	directory = parentDirectory.NewDirectoryURL(name)
	return directory, name
}

// This is a convenience method, No public API to create file URL from fileSystem now. This method uses fileSystem's root directory.
func getFileURLFromFileSystem(a *assert.Assertions, fs azbfs.FileSystemURL) (file azbfs.FileURL, name string) {
	name = generateFileName()
	file = fs.NewRootDirectoryURL().NewFileURL(name)

	return file, name
}

func getFileURLFromDirectory(a *assert.Assertions, directory azbfs.DirectoryURL) (file azbfs.FileURL, name string) {
	name = generateFileName()
	file = directory.NewFileURL(name)

	return file, name
}

func createNewFileSystem(a *assert.Assertions, fsu azbfs.ServiceURL) (fs azbfs.FileSystemURL, name string) {
	fs, name = getFileSystemURL(a, fsu)

	cResp, err := fs.Create(ctx)
	a.Nil(err)
	a.Equal(201, cResp.StatusCode())
	return fs, name
}

func createNewDirectoryFromFileSystem(a *assert.Assertions, fileSystem azbfs.FileSystemURL) (dir azbfs.DirectoryURL, name string) {
	dir, name = getDirectoryURLFromFileSystem(a, fileSystem)

	cResp, err := dir.Create(ctx, true)
	a.Nil(err)
	a.Equal(201, cResp.StatusCode())
	return dir, name
}

// This is a convenience method, No public API to create file URL from fileSystem now. This method uses fileSystem's root directory.
func createNewFileFromFileSystem(a *assert.Assertions, fileSystem azbfs.FileSystemURL) (file azbfs.FileURL, name string) {
	dir := fileSystem.NewRootDirectoryURL()

	file, name = getFileURLFromDirectory(a, dir)

	cResp, err := file.Create(ctx, azbfs.BlobFSHTTPHeaders{}, azbfs.BlobFSAccessControl{})
	a.Nil(err)
	a.Equal(201, cResp.StatusCode())

	return file, name
}

func deleteFileSystem(a *assert.Assertions, fs azbfs.FileSystemURL) {
	resp, err := fs.Delete(context.Background())
	a.Nil(err)
	a.Equal(http.StatusAccepted, resp.Response().StatusCode)
}

// deleteDirectory deletes the directory represented by directory Url
func deleteDirectory(a *assert.Assertions, dul azbfs.DirectoryURL) {
	resp, err := dul.Delete(context.Background(), nil, true)
	a.Nil(err)
	a.Equal(http.StatusOK, resp.Response().StatusCode)
}

func deleteFile(a *assert.Assertions, file azbfs.FileURL) {
	resp, err := file.Delete(context.Background())
	a.Nil(err)
	a.Equal(http.StatusOK, resp.Response().StatusCode)
}