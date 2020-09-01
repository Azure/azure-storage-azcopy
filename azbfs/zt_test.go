package azbfs_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/azbfs"
	chk "gopkg.in/check.v1"
)

func Test(t *testing.T) { chk.TestingT(t) }

type aztestsSuite struct{}

var _ = chk.Suite(&aztestsSuite{})

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
	for i := 0; !strings.Contains(name, "Suite"); i++ { // The tests are all scoped to the suite, so this ensures getting the actual test name
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

func getFileSystemURL(c *chk.C, fsu azbfs.ServiceURL) (fs azbfs.FileSystemURL, name string) {
	name = generateFileSystemName()
	fs = fsu.NewFileSystemURL(name)

	return fs, name
}

func getDirectoryURLFromFileSystem(c *chk.C, fs azbfs.FileSystemURL) (directory azbfs.DirectoryURL, name string) {
	name = generateDirectoryName()
	directory = fs.NewDirectoryURL(name)
	return directory, name
}

func getDirectoryURLFromDirectory(c *chk.C, parentDirectory azbfs.DirectoryURL) (directory azbfs.DirectoryURL, name string) {
	name = generateDirectoryName()
	directory = parentDirectory.NewDirectoryURL(name)
	return directory, name
}

// This is a convenience method, No public API to create file URL from fileSystem now. This method uses fileSystem's root directory.
func getFileURLFromFileSystem(c *chk.C, fs azbfs.FileSystemURL) (file azbfs.FileURL, name string) {
	name = generateFileName()
	file = fs.NewRootDirectoryURL().NewFileURL(name)

	return file, name
}

func getFileURLFromDirectory(c *chk.C, directory azbfs.DirectoryURL) (file azbfs.FileURL, name string) {
	name = generateFileName()
	file = directory.NewFileURL(name)

	return file, name
}

func createNewFileSystem(c *chk.C, fsu azbfs.ServiceURL) (fs azbfs.FileSystemURL, name string) {
	fs, name = getFileSystemURL(c, fsu)

	cResp, err := fs.Create(ctx)
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, 201)
	return fs, name
}

func createNewDirectoryFromFileSystem(c *chk.C, fileSystem azbfs.FileSystemURL) (dir azbfs.DirectoryURL, name string) {
	dir, name = getDirectoryURLFromFileSystem(c, fileSystem)

	cResp, err := dir.Create(ctx, true)
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, 201)
	return dir, name
}

// This is a convenience method, No public API to create file URL from fileSystem now. This method uses fileSystem's root directory.
func createNewFileFromFileSystem(c *chk.C, fileSystem azbfs.FileSystemURL) (file azbfs.FileURL, name string) {
	dir := fileSystem.NewRootDirectoryURL()

	file, name = getFileURLFromDirectory(c, dir)

	cResp, err := file.Create(ctx, azbfs.BlobFSHTTPHeaders{})
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, 201)

	return file, name
}
