// Copyright © Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package e2etest

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/url"
	"runtime"
	"strings"
	"time"

	chk "gopkg.in/check.v1"

	"github.com/Azure/azure-storage-azcopy/azbfs"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/azure-storage-file-go/azfile"
)

// provide convenient methods to get access to test resources such as accounts, containers/shares, directories
type TestResourceFactory struct{}

func (TestResourceFactory) GetBlobServiceURL(accountType AccountType) azblob.ServiceURL {
	accountName, accountKey := GlobalInputManager{}.GetAccountAndKey(accountType)
	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/", accountName))

	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		panic(err)
	}
	pipeline := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	return azblob.NewServiceURL(*u, pipeline)
}

func (TestResourceFactory) GetFileServiceURL(accountType AccountType) azfile.ServiceURL {
	accountName, accountKey := GlobalInputManager{}.GetAccountAndKey(accountType)
	u, _ := url.Parse(fmt.Sprintf("https://%s.file.core.windows.net/", accountName))

	credential, err := azfile.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		panic(err)
	}
	pipeline := azfile.NewPipeline(credential, azfile.PipelineOptions{})
	return azfile.NewServiceURL(*u, pipeline)
}

func (TestResourceFactory) GetDatalakeServiceURL(accountType AccountType) azbfs.ServiceURL {
	accountName, accountKey := GlobalInputManager{}.GetAccountAndKey(accountType)
	u, _ := url.Parse(fmt.Sprintf("https://%s.dfs.core.windows.net/", accountName))

	cred := azbfs.NewSharedKeyCredential(accountName, accountKey)
	pipeline := azbfs.NewPipeline(cred, azbfs.PipelineOptions{})
	return azbfs.NewServiceURL(*u, pipeline)
}

func (TestResourceFactory) GetBlobServiceURLWithSAS(c asserter, accountType AccountType) azblob.ServiceURL {
	accountName, accountKey := GlobalInputManager{}.GetAccountAndKey(accountType)
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	c.Assert(err, chk.IsNil, chk.Commentf("Error: %s", err))

	sasQueryParams, err := azblob.AccountSASSignatureValues{
		Protocol:      azblob.SASProtocolHTTPS,
		ExpiryTime:    time.Now().Add(48 * time.Hour),
		Permissions:   azfile.AccountSASPermissions{Read: true, List: true, Write: true, Delete: true, Add: true, Create: true, Update: true, Process: true}.String(),
		Services:      azfile.AccountSASServices{File: true, Blob: true, Queue: true}.String(),
		ResourceTypes: azfile.AccountSASResourceTypes{Service: true, Container: true, Object: true}.String(),
	}.NewSASQueryParameters(credential)
	c.Assert(err, chk.IsNil, chk.Commentf("Error: %s", err))

	// construct the url from scratch
	qp := sasQueryParams.Encode()
	rawURL := fmt.Sprintf("https://%s.blob.core.windows.net/?%s",
		credential.AccountName(), qp)

	// convert the raw url and validate it was parsed successfully
	fullURL, err := url.Parse(rawURL)
	c.Assert(err, chk.IsNil, chk.Commentf("Error: %s", err))

	return azblob.NewServiceURL(*fullURL, azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{}))
}

func (TestResourceFactory) GetContainerURLWithSAS(c asserter, accountType AccountType, containerName string) azblob.ContainerURL {
	accountName, accountKey := GlobalInputManager{}.GetAccountAndKey(accountType)
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	c.Assert(err, chk.IsNil, chk.Commentf("Error: %s", err))

	sasQueryParams, err := azblob.BlobSASSignatureValues{
		Protocol:      azblob.SASProtocolHTTPS,
		ExpiryTime:    time.Now().UTC().Add(48 * time.Hour),
		ContainerName: containerName,
		Permissions:   azblob.ContainerSASPermissions{Read: true, Add: true, Write: true, Create: true, Delete: true, List: true}.String(),
	}.NewSASQueryParameters(credential)
	c.Assert(err, chk.IsNil, chk.Commentf("Error: %s", err))

	// construct the url from scratch
	qp := sasQueryParams.Encode()
	rawURL := fmt.Sprintf("https://%s.blob.core.windows.net/%s?%s",
		credential.AccountName(), containerName, qp)

	// convert the raw url and validate it was parsed successfully
	fullURL, err := url.Parse(rawURL)
	c.Assert(err, chk.IsNil, chk.Commentf("Error: %s", err))

	return azblob.NewContainerURL(*fullURL, azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{}))
}

func (TestResourceFactory) GetFileShareULWithSAS(c asserter, accountType AccountType, containerName string) azfile.ShareURL {
	accountName, accountKey := GlobalInputManager{}.GetAccountAndKey(accountType)
	credential, err := azfile.NewSharedKeyCredential(accountName, accountKey)
	c.Assert(err, chk.IsNil, chk.Commentf("Error: %s", err))

	sasQueryParams, err := azfile.FileSASSignatureValues{
		Protocol:    azfile.SASProtocolHTTPS,
		ExpiryTime:  time.Now().UTC().Add(48 * time.Hour),
		ShareName:   containerName,
		Permissions: azfile.ShareSASPermissions{Read: true, Write: true, Create: true, Delete: true, List: true}.String(),
	}.NewSASQueryParameters(credential)
	c.Assert(err, chk.IsNil, chk.Commentf("Error: %s", err))

	// construct the url from scratch
	qp := sasQueryParams.Encode()
	rawURL := fmt.Sprintf("https://%s.file.core.windows.net/%s?%s",
		credential.AccountName(), containerName, qp)

	// convert the raw url and validate it was parsed successfully
	fullURL, err := url.Parse(rawURL)
	c.Assert(err, chk.IsNil, chk.Commentf("Error: %s", err))

	return azfile.NewShareURL(*fullURL, azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{}))
}

func (TestResourceFactory) GetBlobURLWithSAS(c asserter, accountType AccountType, containerName string, blobName string) azblob.BlobURL {
	containerURLWithSAS := TestResourceFactory{}.GetContainerURLWithSAS(c, accountType, containerName)
	blobURLWithSAS := containerURLWithSAS.NewBlobURL(blobName)
	return blobURLWithSAS
}

func (TestResourceFactory) CreateNewContainer(c asserter, accountType AccountType) (container azblob.ContainerURL, name string, rawURL url.URL) {
	name = TestResourceNameGenerator{}.GenerateContainerName(c)
	container = TestResourceFactory{}.GetBlobServiceURL(accountType).NewContainerURL(name)

	cResp, err := container.Create(context.Background(), nil, azblob.PublicAccessNone)
	c.Assert(err, chk.IsNil, chk.Commentf("Error: %s", err))
	c.Assert(cResp.StatusCode(), chk.Equals, 201)
	return container, name, TestResourceFactory{}.GetContainerURLWithSAS(c, accountType, name).URL()
}

const defaultShareQuotaGB = 512

func (TestResourceFactory) CreateNewFileShare(c asserter, accountType AccountType) (fileShare azfile.ShareURL, name string, rawSasURL url.URL) {
	name = TestResourceNameGenerator{}.GenerateContainerName(c)
	fileShare = TestResourceFactory{}.GetFileServiceURL(accountType).NewShareURL(name)

	cResp, err := fileShare.Create(context.Background(), nil, defaultShareQuotaGB)
	c.Assert(err, chk.IsNil, chk.Commentf("Error: %s", err))
	c.Assert(cResp.StatusCode(), chk.Equals, 201)
	return fileShare, name, TestResourceFactory{}.GetFileShareULWithSAS(c, accountType, name).URL()
}

func (TestResourceFactory) CreateLocalDirectory(c asserter) (dstDirName string) {
	dstDirName, err := ioutil.TempDir("", "AzCopyLocalTest")
	c.Assert(err, chk.IsNil, chk.Commentf("Error: %s", err))
	return
}

type TestResourceNameGenerator struct{}

const (
	containerPrefix = "e2e"
	blobPrefix      = "blob"
)

func getTestName() (testSuite, test string) {

	// The following lines step up the stack find the name of the test method
	// Note: the way to do this changed in go 1.12, refer to release notes for more info
	var pcs [10]uintptr
	n := runtime.Callers(1, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	fullName := "(*fooSuite).TestFoo" // default stub "Foo" is used if anything goes wrong with this procedure
	for {
		frame, more := frames.Next()
		if strings.Contains(frame.Func.Name(), "Suite") {
			fullName = frame.Func.Name()
			break
		} else if !more {
			break
		}
	}
	funcNameStart := strings.Index(fullName, "Test")
	suiteNameStart := strings.Index(fullName, ".(")
	suite := strings.Replace(strings.Trim(fullName[suiteNameStart:funcNameStart], "()*."), "_", "-", -1) // for consistency with name, below
	suite = strings.Replace(suite, "Suite", "", -1)
	if len(suite) > 4 {
		suite = suite[:4] // trim the suite name part of it, so that we don't end up with so many names that are too long
	}

	name := fullName[funcNameStart+len("Test"):]                // Just get the name of the test and not any of the garbage at the beginning
	name = strings.Replace(strings.ToLower(name), "_", "-", -1) // Ensure it is a valid resource name (containers don't allow _ but do allow -)

	return suite, name
}

// This function generates an entity name by concatenating the passed prefix,
// the name of the test requesting the entity name, and the minute, second, and nanoseconds of the call.
// This should make it easy to associate the entities with their test, uniquely identify
// them, and determine the order in which they were created.
// Will truncate the end of the test name, if there is not enough room for it, followed by the time-based suffix,
// with a non-zero maxLen.
func generateName(c asserter, prefix string, maxLen int) string {
	name := c.ScenarioName() // don't want to just use test name here, because each test contains multiple scearios with the declarative runner

	textualPortion := fmt.Sprintf("%s-%s", prefix, strings.ToLower(name))
	currentTime := time.Now()
	numericSuffix := fmt.Sprintf("%02d%02d%d", currentTime.Minute(), currentTime.Second(), currentTime.Nanosecond())
	if maxLen > 0 {
		maxTextLen := maxLen - len(numericSuffix)
		if maxTextLen < 1 {
			panic("max len too short")
		}
		if len(textualPortion) > maxTextLen {
			textualPortion = textualPortion[:maxTextLen]
		}
	}
	name = textualPortion + numericSuffix
	return name
}

func (TestResourceNameGenerator) GenerateContainerName(c asserter) string {
	return generateName(c, containerPrefix, 63)
}

func (TestResourceNameGenerator) generateBlobName(c asserter) string {
	return generateName(c, blobPrefix, 0)
}
