package azbfs_test

import (
	"context"
	"crypto/md5"
	"fmt"
	"net/url"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-storage-file-go/2017-07-29/azfile"
	"github.com/Azure/azure-storage-azcopy/azbfs"
	chk "gopkg.in/check.v1"
)

func Test(t *testing.T) { chk.TestingT(t) }

type aztestsSuite struct{}

var _ = chk.Suite(&aztestsSuite{})

const (
	fileSystemPrefix         = "go"
	directoryPrefix          = "gotestdirectory"
	filePrefix               = "gotestfile"
	validationErrorSubstring = "validation failed"
	fileDefaultData          = "file default data"
)

var ctx = context.Background()
var basicHeaders = azfile.FileHTTPHeaders{ContentType: "my_type", ContentDisposition: "my_disposition",
	CacheControl: "control", ContentMD5: md5.Sum([]byte("")), ContentLanguage: "my_language", ContentEncoding: "my_encoding"}
var basicMetadata = azfile.Metadata{"foo": "bar"}

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
	u, _ := url.Parse(fmt.Sprintf("http://%s.dfs.core.windows.net/", name))

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
	// trace to extrat the test name, which is stored in name
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
	directory = parentDirectory.NewSubDirectoryUrl(name)
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

//func createNewShareWithPrefix(c *chk.C, fsu azfile.ServiceURL, prefix string) (fileSystem azbfs.FileSystemURL, name string) {
//	name = generateName(prefix)
//	fileSystem = fsu.NewShareURL(name)
//
//	cResp, err := fileSystem.Create(ctx, nil, 0)
//	c.Assert(err, chk.IsNil)
//	c.Assert(cResp.StatusCode(), chk.Equals, 201)
//	return fileSystem, name
//}
//
//func createNewDirectoryWithPrefix(c *chk.C, parentDirectory azfile.DirectoryURL, prefix string) (dir azfile.DirectoryURL, name string) {
//	name = generateName(prefix)
//	dir = parentDirectory.NewDirectoryURL(name)
//
//	cResp, err := dir.Create(ctx, azfile.Metadata{})
//	c.Assert(err, chk.IsNil)
//	c.Assert(cResp.StatusCode(), chk.Equals, 201)
//	return dir, name
//}
//
//func createNewFileWithPrefix(c *chk.C, dir azfile.DirectoryURL, prefix string, size int64) (file azbfs.FileURL, name string) {
//	name = generateName(prefix)
//	file = dir.NewFileURL(name)
//
//	cResp, err := file.Create(ctx, size, azfile.FileHTTPHeaders{}, nil)
//	c.Assert(err, chk.IsNil)
//	c.Assert(cResp.StatusCode(), chk.Equals, 201)
//	return file, name
//}
//
func createNewDirectoryFromFileSystem(c *chk.C, fileSystem azbfs.FileSystemURL) (dir azbfs.DirectoryURL, name string) {
	dir, name = getDirectoryURLFromFileSystem(c, fileSystem)

	cResp, err := dir.Create(ctx)
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, 201)
	return dir, name
}
//
//func createNewDirectoryFromDirectory(c *chk.C, parentDirectory azfile.DirectoryURL) (dir azfile.DirectoryURL, name string) {
//	dir, name = getDirectoryURLFromDirectory(c, parentDirectory)
//
//	cResp, err := dir.Create(ctx, nil)
//	c.Assert(err, chk.IsNil)
//	c.Assert(cResp.StatusCode(), chk.Equals, 201)
//	return dir, name
//}
//
// This is a convenience method, No public API to create file URL from fileSystem now. This method uses fileSystem's root directory.
func createNewFileFromShare(c *chk.C, fileSystem azbfs.FileSystemURL, fileSize int64) (file azbfs.FileURL, name string) {
	dir := fileSystem.NewRootDirectoryURL()

	file, name = getFileURLFromDirectory(c, dir)

	cResp, err := file.Create(ctx, nil)
	c.Assert(err, chk.IsNil)
	c.Assert(cResp.StatusCode(), chk.Equals, 201)

	return file, name
}
//
//// This is a convenience method, No public API to create file URL from fileSystem now. This method uses fileSystem's root directory.
//func createNewFileFromShareWithDefaultData(c *chk.C, fileSystem azbfs.FileSystemURL) (file azbfs.FileURL, name string) {
//	dir := fileSystem.NewRootDirectoryURL()
//
//	file, name = getFileURLFromDirectory(c, dir)
//
//	cResp, err := file.Create(ctx, int64(len(fileDefaultData)), azfile.FileHTTPHeaders{}, nil)
//	c.Assert(err, chk.IsNil)
//	c.Assert(cResp.StatusCode(), chk.Equals, 201)
//
//	_, err = file.UploadRange(ctx, 0, strings.NewReader(fileDefaultData))
//	c.Assert(err, chk.IsNil)
//
//	return file, name
//}
//
//func createNewFileFromDirectory(c *chk.C, directory azfile.DirectoryURL, fileSize int64) (file azbfs.FileURL, name string) {
//	file, name = getFileURLFromDirectory(c, directory)
//
//	cResp, err := file.Create(ctx, fileSize, azfile.FileHTTPHeaders{}, nil)
//	c.Assert(err, chk.IsNil)
//	c.Assert(cResp.StatusCode(), chk.Equals, 201)
//
//	return file, name
//}
//
//func validateStorageError(c *chk.C, err error, code azfile.ServiceCodeType) {
//	c.Assert(err, chk.NotNil)
//
//	serr, _ := err.(azfile.StorageError)
//	c.Assert(serr.ServiceCode(), chk.Equals, code)
//}
