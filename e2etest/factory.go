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
	"os"
	"path"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	blobsas "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	blobservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake"
	datalakeservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/service"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	filesas "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/sas"
	fileservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/google/uuid"
)

// provide convenient methods to get access to test resources such as accounts, containers/shares, directories
type TestResourceFactory struct{}

func (TestResourceFactory) GetBlobServiceURL(accountType AccountType) *blobservice.Client {
	accountName, accountKey := GlobalInputManager{}.GetAccountAndKey(accountType)
	var resourceURL string
	if accountName != "devstoreaccount1" {
		resourceURL = fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)
	} else {
		resourceURL = fmt.Sprintf("http://127.0.0.1:10000/%s/", accountName)
	}

	credential, err := blob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		panic(err)
	}

	// Add version policy to client options
	perCallPolicies := []policy.Policy{ste.NewVersionPolicy()}
	options := &blobservice.ClientOptions{ClientOptions: azcore.ClientOptions{PerCallPolicies: perCallPolicies}}

	bsc, err := blobservice.NewClientWithSharedKeyCredential(resourceURL, credential, options)
	if err != nil {
		panic(err)
	}
	return bsc
}

func (TestResourceFactory) GetFileServiceURL(accountType AccountType) *fileservice.Client {
	accountName, accountKey := GlobalInputManager{}.GetAccountAndKey(accountType)
	resourceURL := fmt.Sprintf("https://%s.file.core.windows.net/", accountName)

	credential, err := file.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		panic(err)
	}

	// Add version policy to client options
	perCallPolicies := []policy.Policy{ste.NewVersionPolicy()}
	options := &fileservice.ClientOptions{
		AllowTrailingDot: to.Ptr(true),
		ClientOptions: azcore.ClientOptions{
			PerCallPolicies: perCallPolicies,
		},
	}

	fsc, err := fileservice.NewClientWithSharedKeyCredential(resourceURL, credential, options)
	if err != nil {
		panic(err)
	}
	return fsc
}

func (TestResourceFactory) GetDatalakeServiceURL(accountType AccountType) *datalakeservice.Client {
	accountName, accountKey := GlobalInputManager{}.GetAccountAndKey(accountType)
	resourceURL := fmt.Sprintf("https://%s.dfs.core.windows.net/", accountName)

	credential, err := azdatalake.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		panic(err)
	}

	// Add version policy to client options
	perCallPolicies := []policy.Policy{ste.NewVersionPolicy()}
	options := &datalakeservice.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			PerCallPolicies: perCallPolicies,
		},
	}

	dsc, err := datalakeservice.NewClientWithSharedKeyCredential(resourceURL, credential, options)
	if err != nil {
		panic(err)
	}
	return dsc
}

func (TestResourceFactory) GetBlobServiceURLWithSAS(c asserter, accountType AccountType) *blobservice.Client {
	accountName, accountKey := GlobalInputManager{}.GetAccountAndKey(accountType)
	credential, err := blob.NewSharedKeyCredential(accountName, accountKey)
	c.AssertNoErr(err)
	rawURL := fmt.Sprintf("https://%s.blob.core.windows.net/", credential.AccountName())
	client, err := blobservice.NewClientWithSharedKeyCredential(rawURL, credential, nil)
	c.AssertNoErr(err)

	sasURL, err := client.GetSASURL(
		blobsas.AccountResourceTypes{Service: true, Container: true, Object: true},
		blobsas.AccountPermissions{Read: true, List: true, Write: true, Delete: true, DeletePreviousVersion: true, Add: true, Create: true, Update: true, Process: true, Tag: true, FilterByTags: true},
		time.Now().Add(48*time.Hour),
		nil)
	c.AssertNoErr(err)

	client, err = blobservice.NewClientWithNoCredential(sasURL, nil)
	c.AssertNoErr(err)

	return client
}

func (TestResourceFactory) GetContainerURLWithSAS(c asserter, accountType AccountType, containerName string) *container.Client {
	accountName, accountKey := GlobalInputManager{}.GetAccountAndKey(accountType)
	credential, err := blob.NewSharedKeyCredential(accountName, accountKey)
	c.AssertNoErr(err)
	rawURL := fmt.Sprintf("https://%s.blob.core.windows.net/%s", credential.AccountName(), containerName)
	if accountName == "devstoreaccount1" {
		rawURL = fmt.Sprintf("http://127.0.0.1:10000/%s/%s", credential.AccountName(), containerName)
	}

	permissions := blobsas.ContainerPermissions{Read: true, Add: true, Write: true, Create: true, Delete: true, DeletePreviousVersion: true, List: true, ModifyOwnership: true, ModifyPermissions: true, Tag: true}
	qps, err := blobsas.BlobSignatureValues{
		Version:       blobsas.Version,
		Protocol:      blobsas.ProtocolHTTPSandHTTP,
		ContainerName: containerName,
		Permissions:   permissions.String(),
		StartTime:     time.Time{},
		ExpiryTime:    time.Now().Add(48 * time.Hour).UTC(),
	}.SignWithSharedKey(credential)
	c.AssertNoErr(err)

	sasURL := rawURL + "?" + qps.Encode()

	// Add version policy to client options
	perCallPolicies := []policy.Policy{ste.NewVersionPolicy()}
	options := &container.ClientOptions{ClientOptions: azcore.ClientOptions{
		PerCallPolicies: perCallPolicies}}

	client, err := container.NewClientWithNoCredential(sasURL, options)
	c.AssertNoErr(err)

	return client
}

func (TestResourceFactory) GetFileShareURLWithSAS(c asserter, accountType AccountType, containerName string) *share.Client {
	accountName, accountKey := GlobalInputManager{}.GetAccountAndKey(accountType)
	credential, err := file.NewSharedKeyCredential(accountName, accountKey)
	c.AssertNoErr(err)
	rawURL := fmt.Sprintf("https://%s.file.core.windows.net/%s", credential.AccountName(), containerName)
	client, err := share.NewClientWithSharedKeyCredential(rawURL, credential, nil)
	c.AssertNoErr(err)

	sasURL, err := client.GetSASURL(
		filesas.SharePermissions{Read: true, Write: true, Create: true, Delete: true, List: true},
		time.Now().Add(48*time.Hour),
		nil)
	c.AssertNoErr(err)
	client, err = share.NewClientWithNoCredential(sasURL, &share.ClientOptions{AllowTrailingDot: to.Ptr(true)})
	c.AssertNoErr(err)

	return client
}

func (TestResourceFactory) GetBlobURLWithSAS(c asserter, accountType AccountType, containerName string, blobName string) *blob.Client {
	containerURLWithSAS := TestResourceFactory{}.GetContainerURLWithSAS(c, accountType, containerName)
	blobURLWithSAS := containerURLWithSAS.NewBlobClient(blobName)
	return blobURLWithSAS
}

func (TestResourceFactory) CreateNewContainer(c asserter, publicAccess *container.PublicAccessType, accountType AccountType) (cc *container.Client, name string, rawURL string) {
	name = TestResourceNameGenerator{}.GenerateContainerName(c)
	cc = TestResourceFactory{}.GetBlobServiceURL(accountType).NewContainerClient(name)

	// Pass override key to context
	ctx = context.WithValue(ctx, ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	_, err := cc.Create(ctx, &container.CreateOptions{Access: publicAccess})
	c.AssertNoErr(err)
	return cc, name, TestResourceFactory{}.GetContainerURLWithSAS(c, accountType, name).URL()
}

const defaultShareQuotaGB = int32(512)

func (TestResourceFactory) CreateNewFileShare(c asserter, accountType AccountType) (fileShare *share.Client, name string, rawSasURL string) {
	name = TestResourceNameGenerator{}.GenerateContainerName(c)
	fileShare = TestResourceFactory{}.GetFileServiceURL(accountType).NewShareClient(name)

	_, err := fileShare.Create(context.Background(), &share.CreateOptions{Quota: to.Ptr(defaultShareQuotaGB)})
	c.AssertNoErr(err)
	return fileShare, name, TestResourceFactory{}.GetFileShareURLWithSAS(c, accountType, name).URL()
}

func (TestResourceFactory) CreateNewFileShareSnapshot(c asserter, fileShare *share.Client) (snapshotID string) {
	resp, err := fileShare.CreateSnapshot(context.TODO(), nil)
	c.AssertNoErr(err)
	return *resp.Snapshot
}

func (TestResourceFactory) CreateLocalDirectory(c asserter) (dstDirName string) {
	dstDirName, err := os.MkdirTemp("", "AzCopyLocalTest")
	c.AssertNoErr(err)
	return
}

type TestResourceNameGenerator struct{}

const (
	containerPrefix = "e2e"
	blobPrefix      = "blob"
)

func getTestName(t *testing.T) (pseudoSuite, test string) {

	removeUnderscores := func(s string) string {
		return strings.Replace(s, "_", "-", -1) // necessary if using name as basis for blob container name
	}

	testName := t.Name()

	// Look up the stack to find out more info about the test method
	// Note: the way to do this changed in go 1.12, refer to release notes for more info
	var pcs [10]uintptr
	n := runtime.Callers(1, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	fileName := ""
	for {
		frame, more := frames.Next()
		if strings.HasSuffix(frame.Func.Name(), "."+testName) {
			fileName = frame.File
			break
		} else if !more {
			break
		}
	}

	// When using the basic Testing package, we have adopted a convention that
	// the test name should being with one of the words in the file name, followed by a _ .
	// Try to extract a "pseudo suite" name from the test name according to that rule.
	pseudoSuite = ""
	testName = strings.Replace(testName, "Test", "", 1)
	uscorePos := strings.Index(testName, "_")
	if uscorePos >= 0 && uscorePos < len(testName)-1 {
		beforeUnderscore := strings.ToLower(testName[:uscorePos])

		fileWords := strings.ReplaceAll(
			strings.TrimSuffix(strings.TrimPrefix(path.Base(fileName), "zt_"), "_test.go"), "_", "")

		if strings.Contains(fileWords, beforeUnderscore) {
			pseudoSuite = beforeUnderscore
			testName = testName[uscorePos+1:]
		}
		// fileWords := strings.Split(strings.Replace(strings.ToLower(filepath.Base(fileName)), "_test.go", "", -1), "_")
		// for _, w := range fileWords {
		// 	if beforeUnderscore == w {
		// 		pseudoSuite = beforeUnderscore
		// 		testName = testName[uscorePos+1:]
		// 		break
		// 	}
		// }
	}

	return pseudoSuite, removeUnderscores(testName)
}

// This function generates an entity name by concatenating the passed prefix,
// the name of the test requesting the entity name, and the minute, second, and nanoseconds of the call.
// This should make it easy to associate the entities with their test, uniquely identify
// them, and determine the order in which they were created.
// Will truncate the end of the test name, if there is not enough room for it, followed by the time-based suffix,
// with a non-zero maxLen.
func generateName(c asserter, prefix string, maxLen int) string {
	name := c.CompactScenarioName() // don't want to just use test name here, because each test contains multiple scenarios with the declarative runner

	textualPortion := fmt.Sprintf("%s-%s", prefix, strings.ToLower(name))
	// GUIDs are less prone to overlap than times.
	guidSuffix := uuid.New().String()
	if maxLen > 0 {
		maxTextLen := maxLen - len(guidSuffix)
		if maxTextLen < 1 {
			panic("max len too short")
		}
		if len(textualPortion) > maxTextLen {
			textualPortion = textualPortion[:maxTextLen]
		}
	}
	name = textualPortion + guidSuffix
	return name
}

func (TestResourceNameGenerator) GenerateContainerName(c asserter) string {
	// return generateName(c, containerPrefix, 63)
	return uuid.New().String()
}

func (TestResourceNameGenerator) generateBlobName(c asserter) string {
	return generateName(c, blobPrefix, 0)
}
