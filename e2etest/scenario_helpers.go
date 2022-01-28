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

// TODO this file was forked from the cmd package, it needs to cleaned to keep only the necessary part

package e2etest

import (
	"context"
	"crypto/md5"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/azbfs"
	"github.com/minio/minio-go"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/azure-storage-file-go/azfile"
)

const defaultFileSize = 1024
const defaultStringFileSize = "1k"

type scenarioHelper struct{}

//nolint
var specialNames = []string{
	"打麻将.txt",
	"wow such space so much space",
	"打%%#%@#%麻将.txt",
	//"saywut.pdf?yo=bla&WUWUWU=foo&sig=yyy", // TODO this breaks on windows, figure out a way to add it only for tests on Unix
	"coração",
	"আপনার নাম কি",
	"%4509%4254$85140&",
	"Donaudampfschifffahrtselektrizitätenhauptbetriebswerkbauunterbeamtengesellschaft",
	"お名前は何ですか",
	"Adın ne",
	"як вас звати",
}

// note: this is to emulate the list-of-files flag
//nolint
func (scenarioHelper) generateListOfFiles(c asserter, fileList []string) (path string) {
	parentDirName, err := ioutil.TempDir("", "AzCopyLocalTest")
	c.AssertNoErr(err)

	// create the file
	path = common.GenerateFullPath(parentDirName, generateName(c, "listy", 0))
	err = os.MkdirAll(filepath.Dir(path), os.ModePerm)
	c.AssertNoErr(err)

	// pipe content into it
	content := strings.Join(fileList, "\n")
	err = ioutil.WriteFile(path, []byte(content), common.DEFAULT_FILE_PERM)
	c.AssertNoErr(err)
	return
}

//nolint
func (scenarioHelper) generateLocalDirectory(c asserter) (dstDirName string) {
	dstDirName, err := ioutil.TempDir("", "AzCopyLocalTest")
	c.AssertNoErr(err)
	return
}

// create a test file
func (scenarioHelper) generateLocalFile(filePath string, fileSize int) ([]byte, error) {
	// generate random data
	_, bigBuff := getRandomDataAndReader(fileSize)

	// create all parent directories
	err := os.MkdirAll(filepath.Dir(filePath), os.ModePerm)
	if err != nil {
		return nil, err
	}

	// write to file and return the data
	err = ioutil.WriteFile(filePath, bigBuff, common.DEFAULT_FILE_PERM)
	return bigBuff, err
}

type generateLocalFilesFromList struct {
	dirPath string
	generateFromListOptions
}

func (s scenarioHelper) generateLocalFilesFromList(c asserter, options *generateLocalFilesFromList) {
	for _, file := range options.fs {
		var err error
		if file.isFolder() {
			err = os.MkdirAll(filepath.Join(options.dirPath, file.name), os.ModePerm)
			c.AssertNoErr(err)
			// TODO: You'll need to set up things like attributes, and other relevant things from
			//   file.creationProperties here. (Use all the properties of file.creationProperties that are supported
			//			//   by local files. E.g. not contentHeaders or metadata).

			if file.creationProperties.smbPermissionsSddl != nil {
				osScenarioHelper{}.setFileSDDLString(c, filepath.Join(options.dirPath, file.name), *file.creationProperties.smbPermissionsSddl)
			}
		} else {
			sourceData, err := s.generateLocalFile(
				filepath.Join(options.dirPath, file.name),
				file.creationProperties.sizeBytes(c, options.defaultSize))
			contentMD5 := md5.Sum(sourceData)
			if file.creationProperties.contentHeaders == nil {
				file.creationProperties.contentHeaders = &contentHeaders{}
			}
			file.creationProperties.contentHeaders.contentMD5 = contentMD5[:]

			c.AssertNoErr(err)
			// TODO: You'll need to set up things like attributes, and other relevant things from
			//   file.creationProperties here. (Use all the properties of file.creationProperties that are supported
			//   by local files. E.g. not contentHeaders or metadata).

			if file.creationProperties.smbPermissionsSddl != nil {
				osScenarioHelper{}.setFileSDDLString(c, filepath.Join(options.dirPath, file.name), *file.creationProperties.smbPermissionsSddl)
			}
		}
	}

	// sleep a bit so that the files' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
}

// Enumerates all local files and their properties, with the given dirpath
func (s scenarioHelper) enumerateLocalProperties(a asserter, dirpath string) map[string]*objectProperties {
	result := make(map[string]*objectProperties)
	err := filepath.Walk(dirpath, func(fullpath string, info os.FileInfo, err error) error {
		a.AssertNoErr(err) // we don't expect any errors walking the local file system
		relPath := strings.Replace(fullpath, dirpath, "", 1)
		if runtime.GOOS == "windows" {
			// For windows based system
			relPath = strings.TrimPrefix(relPath, "\\")
		} else {
			// For Linux based system
			relPath = strings.TrimPrefix(relPath, "/")
		}

		size := info.Size()
		lastWriteTime := info.ModTime()
		var pCreationTime *time.Time
		var pSmbAttributes *uint32
		var pSmbPermissionsSddl *string
		if runtime.GOOS == "windows" {
			var creationTime time.Time
			lastWriteTime, creationTime = osScenarioHelper{}.getFileDates(a, fullpath)
			pCreationTime = &creationTime
			pSmbAttributes = osScenarioHelper{}.getFileAttrs(a, fullpath)
			pSmbPermissionsSddl = osScenarioHelper{}.getFileSDDLString(a, fullpath)
		}
		props := objectProperties{
			isFolder:           info.IsDir(),
			size:               &size,
			creationTime:       pCreationTime,
			lastWriteTime:      &lastWriteTime,
			smbAttributes:      pSmbAttributes,
			smbPermissionsSddl: pSmbPermissionsSddl,
			//contentHeaders don't exist on local file system
			//nameValueMetadata doesn't exist on local file system
		}

		result[relPath] = &props
		return nil
	})
	a.AssertNoErr(err)
	return result
}

//nolint
func (s scenarioHelper) generateCommonRemoteScenarioForLocal(c asserter, dirPath string, prefix string) (fileList []string) {
	fileList = make([]string, 50)
	for i := 0; i < 10; i++ {
		batch := []string{
			generateName(c, prefix+"top", 0),
			generateName(c, prefix+"sub1/", 0),
			generateName(c, prefix+"sub2/", 0),
			generateName(c, prefix+"sub1/sub3/sub5/", 0),
			generateName(c, prefix+specialNames[i], 0),
		}

		for j, name := range batch {
			fileList[5*i+j] = name
			_, err := s.generateLocalFile(filepath.Join(dirPath, name), defaultFileSize)
			c.AssertNoErr(err)
		}
	}

	// sleep a bit so that the files' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
	return
}

//nolint
func (scenarioHelper) generateCommonRemoteScenarioForBlob(c asserter, containerURL azblob.ContainerURL, prefix string) (blobList []string) {
	// make 50 blobs with random names
	// 10 of them at the top level
	// 10 of them in sub dir "sub1"
	// 10 of them in sub dir "sub2"
	// 10 of them in deeper sub dir "sub1/sub3/sub5"
	// 10 of them with special characters
	blobList = make([]string, 50)

	for i := 0; i < 10; i++ {
		_, blobName1 := createNewBlockBlob(c, containerURL, prefix+"top")
		_, blobName2 := createNewBlockBlob(c, containerURL, prefix+"sub1/")
		_, blobName3 := createNewBlockBlob(c, containerURL, prefix+"sub2/")
		_, blobName4 := createNewBlockBlob(c, containerURL, prefix+"sub1/sub3/sub5/")
		_, blobName5 := createNewBlockBlob(c, containerURL, prefix+specialNames[i])

		blobList[5*i] = blobName1
		blobList[5*i+1] = blobName2
		blobList[5*i+2] = blobName3
		blobList[5*i+3] = blobName4
		blobList[5*i+4] = blobName5
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
	return
}

//nolint
func (scenarioHelper) generateCommonRemoteScenarioForBlobFS(c asserter, filesystemURL azbfs.FileSystemURL, prefix string) (pathList []string) {
	pathList = make([]string, 50)

	for i := 0; i < 10; i++ {
		_, pathName1 := createNewBfsFile(c, filesystemURL, prefix+"top")
		_, pathName2 := createNewBfsFile(c, filesystemURL, prefix+"sub1/")
		_, pathName3 := createNewBfsFile(c, filesystemURL, prefix+"sub2/")
		_, pathName4 := createNewBfsFile(c, filesystemURL, prefix+"sub1/sub3/sub5")
		_, pathName5 := createNewBfsFile(c, filesystemURL, prefix+specialNames[i])

		pathList[5*i] = pathName1
		pathList[5*i+1] = pathName2
		pathList[5*i+2] = pathName3
		pathList[5*i+3] = pathName4
		pathList[5*i+4] = pathName5
	}

	// sleep a bit so that the paths' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1500)
	return
}

//nolint
func (scenarioHelper) generateCommonRemoteScenarioForAzureFile(c asserter, shareURL azfile.ShareURL, prefix string) (fileList []string) {
	fileList = make([]string, 50)

	for i := 0; i < 10; i++ {
		_, fileName1 := createNewAzureFile(c, shareURL, prefix+"top")
		_, fileName2 := createNewAzureFile(c, shareURL, prefix+"sub1/")
		_, fileName3 := createNewAzureFile(c, shareURL, prefix+"sub2/")
		_, fileName4 := createNewAzureFile(c, shareURL, prefix+"sub1/sub3/sub5/")
		_, fileName5 := createNewAzureFile(c, shareURL, prefix+specialNames[i])

		fileList[5*i] = fileName1
		fileList[5*i+1] = fileName2
		fileList[5*i+2] = fileName3
		fileList[5*i+3] = fileName4
		fileList[5*i+4] = fileName5
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
	return
}

//nolint
func (s scenarioHelper) generateBlobContainersAndBlobsFromLists(c asserter, serviceURL azblob.ServiceURL, containerList []string, blobList []*testObject) {
	for _, containerName := range containerList {
		curl := serviceURL.NewContainerURL(containerName)
		_, err := curl.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
		c.AssertNoErr(err)
		s.generateBlobsFromList(c, &generateBlobFromListOptions{
			containerURL: curl,
			generateFromListOptions: generateFromListOptions{
				fs:          blobList,
				defaultSize: defaultStringFileSize,
			},
		})
	}
}

//nolint
func (s scenarioHelper) generateFileSharesAndFilesFromLists(c asserter, serviceURL azfile.ServiceURL, shareList []string, fileList []*testObject) {
	for _, shareName := range shareList {
		sURL := serviceURL.NewShareURL(shareName)
		_, err := sURL.Create(ctx, azfile.Metadata{}, 0)
		c.AssertNoErr(err)

		s.generateAzureFilesFromList(c, &generateAzureFilesFromListOptions{
			shareURL:    sURL,
			fileList:    fileList,
			defaultSize: defaultStringFileSize,
		})
	}
}

//nolint
func (s scenarioHelper) generateFilesystemsAndFilesFromLists(c asserter, serviceURL azbfs.ServiceURL, fsList []string, fileList []string, data string) {
	for _, filesystemName := range fsList {
		fsURL := serviceURL.NewFileSystemURL(filesystemName)
		_, err := fsURL.Create(ctx)
		c.AssertNoErr(err)

		s.generateBFSPathsFromList(c, fsURL, fileList)
	}
}

//nolint
func (s scenarioHelper) generateS3BucketsAndObjectsFromLists(c asserter, s3Client *minio.Client, bucketList []string, objectList []string, data string) {
	for _, bucketName := range bucketList {
		err := s3Client.MakeBucket(bucketName, "")
		c.AssertNoErr(err)

		s.generateObjects(c, s3Client, bucketName, objectList)
	}
}

type generateFromListOptions struct {
	fs          []*testObject
	defaultSize string
	accountType AccountType
}

type generateBlobFromListOptions struct {
	rawSASURL    url.URL
	containerURL azblob.ContainerURL
	cpkInfo      common.CpkInfo
	cpkScopeInfo common.CpkScopeInfo
	accessTier   azblob.AccessTierType
	generateFromListOptions
}

// create the demanded blobs
func (scenarioHelper) generateBlobsFromList(c asserter, options *generateBlobFromListOptions) {
	for _, b := range options.fs {
		if b.isFolder() {
			continue // no real folders in blob
		}
		ad := blobResourceAdapter{b}
		reader, sourceData := getRandomDataAndReader(b.creationProperties.sizeBytes(c, options.defaultSize))

		// Setting content MD5
		contentMD5 := md5.Sum(sourceData)
		if ad.obj.creationProperties.contentHeaders == nil {
			b.creationProperties.contentHeaders = &contentHeaders{}
		}
		ad.obj.creationProperties.contentHeaders.contentMD5 = contentMD5[:]

		tags := ad.toBlobTags()

		if options.accountType == EAccountType.HierarchicalNamespaceEnabled() {
			tags = nil
		}

		headers := ad.toHeaders()
		headers.ContentMD5 = contentMD5[:]

		var err error

		switch b.creationProperties.blobType {
		case common.EBlobType.BlockBlob(), common.EBlobType.Detect():
			bb := options.containerURL.NewBlockBlobURL(b.name)

			if options.accessTier == "" {
				options.accessTier = azblob.DefaultAccessTier
			}

			cResp, err := bb.Upload(ctx,
				reader,
				headers,
				ad.toMetadata(),
				azblob.BlobAccessConditions{},
				options.accessTier,
				tags,
				common.ToClientProvidedKeyOptions(options.cpkInfo, options.cpkScopeInfo),
			)

			c.AssertNoErr(err)
			c.Assert(cResp.StatusCode(), equals(), 201)
		case common.EBlobType.PageBlob():
			pb := options.containerURL.NewPageBlobURL(b.name)
			cResp, err := pb.Create(ctx, reader.Size(), 0, headers, ad.toMetadata(), azblob.BlobAccessConditions{}, azblob.DefaultPremiumBlobAccessTier, tags, common.ToClientProvidedKeyOptions(options.cpkInfo, options.cpkScopeInfo))
			c.AssertNoErr(err)
			c.Assert(cResp.StatusCode(), equals(), 201)

			pbUpResp, err := pb.UploadPages(ctx, 0, reader, azblob.PageBlobAccessConditions{}, nil, common.ToClientProvidedKeyOptions(options.cpkInfo, options.cpkScopeInfo))
			c.AssertNoErr(err)
			c.Assert(pbUpResp.StatusCode(), equals(), 201)
		case common.EBlobType.AppendBlob():
			ab := options.containerURL.NewAppendBlobURL(b.name)
			cResp, err := ab.Create(ctx, headers, ad.toMetadata(), azblob.BlobAccessConditions{}, tags, common.ToClientProvidedKeyOptions(options.cpkInfo, options.cpkScopeInfo))
			c.AssertNoErr(err)
			c.Assert(cResp.StatusCode(), equals(), 201)

			abUpResp, err := ab.AppendBlock(ctx, reader, azblob.AppendBlobAccessConditions{}, nil, common.ToClientProvidedKeyOptions(options.cpkInfo, options.cpkScopeInfo))
			c.AssertNoErr(err)
			c.Assert(abUpResp.StatusCode(), equals(), 201)
		}

		if b.creationProperties.adlsPermissionsACL != nil {
			bfsURLParts := azbfs.NewBfsURLParts(options.rawSASURL)
			bfsURLParts.Host = strings.Replace(bfsURLParts.Host, ".blob", ".dfs", 1)

			bfsContainer := azbfs.NewFileSystemURL(bfsURLParts.URL(), azbfs.NewPipeline(azbfs.NewAnonymousCredential(), azbfs.PipelineOptions{}))

			var updateResp *azbfs.PathUpdateResponse
			if b.isFolder() {
				dirURL := bfsContainer.NewDirectoryURL(b.name)

				updateResp, err = dirURL.SetAccessControl(ctx, azbfs.BlobFSAccessControl{
					ACL: *b.creationProperties.adlsPermissionsACL,
				})
			} else {
				d, f := path.Split(b.name)
				dirURL := bfsContainer.NewDirectoryURL(d)
				fileURL := dirURL.NewFileURL(f)

				updateResp, err = fileURL.SetAccessControl(ctx, azbfs.BlobFSAccessControl{
					ACL: *b.creationProperties.adlsPermissionsACL,
				})
			}

			c.AssertNoErr(err)
			c.Assert(updateResp.StatusCode(), equals(), 200)
		}
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	// TODO: can we make it so that this sleeping only happens when we really need it to?
	time.Sleep(time.Millisecond * 1050)
}

func (s scenarioHelper) enumerateContainerBlobProperties(a asserter, containerURL azblob.ContainerURL) map[string]*objectProperties {
	result := make(map[string]*objectProperties)

	for marker := (azblob.Marker{}); marker.NotDone(); {

		listBlob, err := containerURL.ListBlobsFlatSegment(context.TODO(), marker, azblob.ListBlobsSegmentOptions{Details: azblob.BlobListingDetails{Metadata: true, Tags: true}})
		a.AssertNoErr(err)

		for _, blobInfo := range listBlob.Segment.BlobItems {

			relativePath := blobInfo.Name // need to change this when we support working on virtual directories down inside containers
			bp := blobInfo.Properties

			h := contentHeaders{
				cacheControl:       bp.CacheControl,
				contentDisposition: bp.ContentDisposition,
				contentEncoding:    bp.ContentEncoding,
				contentLanguage:    bp.ContentLanguage,
				contentType:        bp.ContentType,
				contentMD5:         bp.ContentMD5,
			}
			md := map[string]string(blobInfo.Metadata)

			props := objectProperties{
				isFolder:           false, // no folders in Blob
				size:               bp.ContentLength,
				contentHeaders:     &h,
				nameValueMetadata:  md,
				creationTime:       bp.CreationTime,
				lastWriteTime:      &bp.LastModified,
				cpkInfo:            &common.CpkInfo{EncryptionKeySha256: bp.CustomerProvidedKeySha256},
				cpkScopeInfo:       &common.CpkScopeInfo{EncryptionScope: bp.EncryptionScope},
				adlsPermissionsACL: bp.ACL,
				// smbAttributes and smbPermissions don't exist in blob
			}

			if blobInfo.BlobTags != nil {
				blobTagsMap := common.BlobTags{}
				for _, blobTag := range blobInfo.BlobTags.BlobTagSet {
					blobTagsMap[url.QueryEscape(blobTag.Key)] = url.QueryEscape(blobTag.Value)
				}
				props.blobTags = blobTagsMap
			}

			props.blobType = common.FromAzBlobType(blobInfo.Properties.BlobType)

			result[relativePath] = &props
		}

		marker = listBlob.NextMarker
	}

	return result
}

func (s scenarioHelper) downloadBlobContent(a asserter, options downloadContentOptions) []byte {
	blobURL := options.containerURL.NewBlobURL(options.resourceRelPath)
	cpk := common.ToClientProvidedKeyOptions(options.cpkInfo, options.cpkScopeInfo)
	downloadResp, err := blobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false, cpk)
	a.AssertNoErr(err)

	retryReader := downloadResp.Body(azblob.RetryReaderOptions{})
	defer retryReader.Close()

	destData, err := ioutil.ReadAll(retryReader)
	a.AssertNoErr(err)
	return destData[:]
}

//nolint
func (scenarioHelper) generatePageBlobsFromList(c asserter, containerURL azblob.ContainerURL, blobList []string, data string) {
	for _, blobName := range blobList {
		//Create the blob (PUT blob)
		blob := containerURL.NewPageBlobURL(blobName)
		cResp, err := blob.Create(ctx,
			int64(len(data)),
			0,
			azblob.BlobHTTPHeaders{
				ContentType: "text/random",
			},
			azblob.Metadata{},
			azblob.BlobAccessConditions{},
			azblob.DefaultPremiumBlobAccessTier,
			nil,
			azblob.ClientProvidedKeyOptions{},
		)
		c.AssertNoErr(err)
		c.Assert(cResp.StatusCode(), equals(), 201)

		//Create the page (PUT page)
		uResp, err := blob.UploadPages(ctx,
			0,
			strings.NewReader(data),
			azblob.PageBlobAccessConditions{},
			nil,
			azblob.ClientProvidedKeyOptions{},
		)
		c.AssertNoErr(err)
		c.Assert(uResp.StatusCode(), equals(), 201)
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
}

//nolint
func (scenarioHelper) generateAppendBlobsFromList(c asserter, containerURL azblob.ContainerURL, blobList []string, data string) {
	for _, blobName := range blobList {
		//Create the blob (PUT blob)
		blob := containerURL.NewAppendBlobURL(blobName)
		cResp, err := blob.Create(ctx,
			azblob.BlobHTTPHeaders{
				ContentType: "text/random",
			},
			azblob.Metadata{},
			azblob.BlobAccessConditions{},
			nil,
			azblob.ClientProvidedKeyOptions{},
		)
		c.AssertNoErr(err)
		c.Assert(cResp.StatusCode(), equals(), 201)

		//Append a block (PUT block)
		uResp, err := blob.AppendBlock(ctx,
			strings.NewReader(data),
			azblob.AppendBlobAccessConditions{},
			nil, azblob.ClientProvidedKeyOptions{})
		c.AssertNoErr(err)
		c.Assert(uResp.StatusCode(), equals(), 201)
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
}

//nolint
func (scenarioHelper) generateBlockBlobWithAccessTier(c asserter, containerURL azblob.ContainerURL, blobName string, accessTier azblob.AccessTierType) {
	blob := containerURL.NewBlockBlobURL(blobName)
	cResp, err := blob.Upload(ctx, strings.NewReader(blockBlobDefaultData), azblob.BlobHTTPHeaders{},
		nil, azblob.BlobAccessConditions{}, accessTier, nil, azblob.ClientProvidedKeyOptions{})
	c.AssertNoErr(err)
	c.Assert(cResp.StatusCode(), equals(), 201)
}

// create the demanded objects
//nolint
func (scenarioHelper) generateObjects(c asserter, client *minio.Client, bucketName string, objectList []string) {
	size := int64(len(objectDefaultData))
	for _, objectName := range objectList {
		n, err := client.PutObjectWithContext(ctx, bucketName, objectName, strings.NewReader(objectDefaultData), size, minio.PutObjectOptions{})
		c.AssertNoErr(err)
		c.Assert(n, equals(), size)
	}
}

// create the demanded files
//nolint
func (scenarioHelper) generateFlatFiles(c asserter, shareURL azfile.ShareURL, fileList []string) {
	for _, fileName := range fileList {
		file := shareURL.NewRootDirectoryURL().NewFileURL(fileName)
		err := azfile.UploadBufferToAzureFile(ctx, []byte(fileDefaultData), file, azfile.UploadToAzureFileOptions{})
		c.AssertNoErr(err)
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
}

//nolint
func (scenarioHelper) generateCommonRemoteScenarioForS3(c asserter, client *minio.Client, bucketName string, prefix string, returnObjectListWithBucketName bool) (objectList []string) {
	// make 50 objects with random names
	// 10 of them at the top level
	// 10 of them in sub dir "sub1"
	// 10 of them in sub dir "sub2"
	// 10 of them in deeper sub dir "sub1/sub3/sub5"
	// 10 of them with special characters
	objectList = make([]string, 50)

	for i := 0; i < 10; i++ {
		objectName1 := createNewObject(c, client, bucketName, prefix+"top")
		objectName2 := createNewObject(c, client, bucketName, prefix+"sub1/")
		objectName3 := createNewObject(c, client, bucketName, prefix+"sub2/")
		objectName4 := createNewObject(c, client, bucketName, prefix+"sub1/sub3/sub5/")
		objectName5 := createNewObject(c, client, bucketName, prefix+specialNames[i])

		// Note: common.AZCOPY_PATH_SEPARATOR_STRING is added before bucket or objectName, as in the change minimize JobPartPlan file size,
		// transfer.Source & transfer.Destination(after trimed the SourceRoot and DestinationRoot) are with AZCOPY_PATH_SEPARATOR_STRING suffix,
		// when user provided source & destination are without / suffix, which is the case for scenarioHelper generated URL.

		bucketPath := ""
		if returnObjectListWithBucketName {
			bucketPath = common.AZCOPY_PATH_SEPARATOR_STRING + bucketName
		}

		objectList[5*i] = bucketPath + common.AZCOPY_PATH_SEPARATOR_STRING + objectName1
		objectList[5*i+1] = bucketPath + common.AZCOPY_PATH_SEPARATOR_STRING + objectName2
		objectList[5*i+2] = bucketPath + common.AZCOPY_PATH_SEPARATOR_STRING + objectName3
		objectList[5*i+3] = bucketPath + common.AZCOPY_PATH_SEPARATOR_STRING + objectName4
		objectList[5*i+4] = bucketPath + common.AZCOPY_PATH_SEPARATOR_STRING + objectName5
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
	return
}

type generateAzureFilesFromListOptions struct {
	shareURL    azfile.ShareURL
	fileList    []*testObject
	defaultSize string
}

// create the demanded azure files
func (scenarioHelper) generateAzureFilesFromList(c asserter, options *generateAzureFilesFromListOptions) {
	for _, f := range options.fileList {
		ad := filesResourceAdapter{f}
		if f.isFolder() {
			// make sure the dir exists
			file := options.shareURL.NewRootDirectoryURL().NewFileURL(path.Join(f.name, "dummyChild"))
			generateParentsForAzureFile(c, file)

			dir := options.shareURL.NewRootDirectoryURL().NewDirectoryURL(f.name)

			// set its metadata if any
			if f.creationProperties.nameValueMetadata != nil {
				_, err := dir.SetMetadata(context.TODO(), ad.toMetadata())
				c.AssertNoErr(err)
			}

			if f.creationProperties.smbPermissionsSddl != nil || f.creationProperties.smbAttributes != nil {
				_, err := dir.SetProperties(ctx, ad.toHeaders(c, options.shareURL).SMBProperties)
				c.AssertNoErr(err)
			}

			// set other properties
			// TODO: do we need a SetProperties method on dir...?  Discuss with zezha-msft
			if f.creationProperties.creationTime != nil {
				panic("setting these properties isn't implmented yet for folders in the test harnesss")
				// TODO: nakulkar-msft the attributes stuff will need to be implemented here before attributes can be tested on Azure Files
			}

			// TODO: I'm pretty sure we don't prserve lastWritetime or contentProperties (headers) for folders, so the above if statement doesn't test those
			//    Is that the correct decision?
		} else {
			file := options.shareURL.NewRootDirectoryURL().NewFileURL(f.name)

			// create parents first
			generateParentsForAzureFile(c, file)

			// create the file itself
			fileSize := int64(f.creationProperties.sizeBytes(c, options.defaultSize))
			contentR, contentD := getRandomDataAndReader(int(fileSize))
			contentMD5 := md5.Sum(contentD)
			if f.creationProperties.contentHeaders == nil {
				f.creationProperties.contentHeaders = &contentHeaders{}
			}
			f.creationProperties.contentHeaders.contentMD5 = contentMD5[:]

			//if f.verificationProperties.contentHeaders == nil {
			//	f.verificationProperties.contentHeaders = &contentHeaders{}
			//}
			//f.verificationProperties.contentHeaders.contentMD5 = contentMD5[:]
			headers := ad.toHeaders(c, options.shareURL)
			headers.ContentMD5 = contentMD5[:]

			cResp, err := file.Create(ctx, fileSize, headers, ad.toMetadata())
			c.AssertNoErr(err)
			c.Assert(cResp.StatusCode(), equals(), 201)

			_, err = file.UploadRange(context.Background(), 0, contentR, nil)
			if err == nil {
				c.Failed()
			}

			// TODO: do we want to put some random content into it?
		}
	}

	// sleep a bit so that the files' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
}

func (s scenarioHelper) enumerateShareFileProperties(a asserter, shareURL azfile.ShareURL) map[string]*objectProperties {
	var dirQ []azfile.DirectoryURL
	result := make(map[string]*objectProperties)

	root := shareURL.NewRootDirectoryURL()
	dirQ = append(dirQ, root)
	for i := 0; i < len(dirQ); i++ {
		currentDirURL := dirQ[i]
		for marker := (azfile.Marker{}); marker.NotDone(); {
			lResp, err := currentDirURL.ListFilesAndDirectoriesSegment(context.TODO(), marker, azfile.ListFilesAndDirectoriesOptions{})
			a.AssertNoErr(err)

			// Process the files and folders we listed
			for _, fileInfo := range lResp.FileItems {
				fileURL := currentDirURL.NewFileURL(fileInfo.Name)
				fProps, err := fileURL.GetProperties(context.TODO())
				a.AssertNoErr(err)

				// Construct the properties object
				fileSize := fProps.ContentLength()
				creationTime, err := time.Parse(azfile.ISO8601, fProps.FileCreationTime())
				a.AssertNoErr(err)
				lastWriteTime, err := time.Parse(azfile.ISO8601, fProps.FileLastWriteTime())
				a.AssertNoErr(err)
				contentHeader := fProps.NewHTTPHeaders()
				h := contentHeaders{
					cacheControl:       &contentHeader.CacheControl,
					contentDisposition: &contentHeader.ContentDisposition,
					contentEncoding:    &contentHeader.ContentEncoding,
					contentLanguage:    &contentHeader.ContentLanguage,
					contentType:        &contentHeader.ContentType,
					contentMD5:         contentHeader.ContentMD5,
				}
				fileAttrs := uint32(azfile.ParseFileAttributeFlagsString(fProps.FileAttributes()))
				permissionKey := fProps.FilePermissionKey()

				var perm string
				if permissionKey != "" {
					sharePerm, err := shareURL.GetPermission(ctx, permissionKey)
					a.AssertNoErr(err, "Failed to get permissions from key")

					perm = sharePerm.Permission
				}

				props := objectProperties{
					isFolder:           false, // no folders in Blob
					size:               &fileSize,
					nameValueMetadata:  fProps.NewMetadata(),
					contentHeaders:     &h,
					creationTime:       &creationTime,
					lastWriteTime:      &lastWriteTime,
					smbAttributes:      &fileAttrs,
					smbPermissionsSddl: &perm,
				}

				relativePath := lResp.DirectoryPath + "/"
				if relativePath == "/" {
					relativePath = ""
				}
				result[relativePath+fileInfo.Name] = &props
			}

			for _, dirInfo := range lResp.DirectoryItems {
				dirURL := currentDirURL.NewDirectoryURL(dirInfo.Name)
				dProps, err := dirURL.GetProperties(context.TODO())
				a.AssertNoErr(err)

				// Construct the properties object
				creationTime, err := time.Parse(azfile.ISO8601, dProps.FileCreationTime())
				a.AssertNoErr(err)
				lastWriteTime, err := time.Parse(azfile.ISO8601, dProps.FileLastWriteTime())
				a.AssertNoErr(err)

				// Grab the permissions
				permKey := dProps.FilePermissionKey()

				var perm string
				if permKey != "" {
					permResp, err := shareURL.GetPermission(ctx, permKey)
					a.AssertNoErr(err, "Failed to get permissions from key")

					perm = permResp.Permission
				}

				// Set up properties
				props := objectProperties{
					isFolder:           true,
					nameValueMetadata:  dProps.NewMetadata(),
					creationTime:       &creationTime,
					lastWriteTime:      &lastWriteTime,
					smbPermissionsSddl: &perm,
				}

				// get the directory name properly
				relativePath := lResp.DirectoryPath + "/"
				if relativePath == "/" {
					relativePath = ""
				}
				result[relativePath+dirInfo.Name] = &props

				dirQ = append(dirQ, dirURL)
			}

			marker = lResp.NextMarker
		}
	}

	return result
}

func (s scenarioHelper) downloadFileContent(a asserter, options downloadContentOptions) []byte {
	fileURL := options.shareURL.NewRootDirectoryURL().NewFileURL(options.resourceRelPath)
	downloadResp, err := fileURL.Download(ctx, 0, azfile.CountToEnd, false)
	a.AssertNoErr(err)

	retryReader := downloadResp.Body(azfile.RetryReaderOptions{})
	defer retryReader.Close() // The client must close the response body when finished with it

	destData, err := ioutil.ReadAll(retryReader)
	a.AssertNoErr(err)
	downloadResp.Body(azfile.RetryReaderOptions{})
	return destData
}

//nolint
func (scenarioHelper) generateBFSPathsFromList(c asserter, filesystemURL azbfs.FileSystemURL, fileList []string) {
	for _, bfsPath := range fileList {
		file := filesystemURL.NewRootDirectoryURL().NewFileURL(bfsPath)

		// Create the file
		cResp, err := file.Create(ctx, azbfs.BlobFSHTTPHeaders{}, azbfs.BlobFSAccessControl{})
		c.AssertNoErr(err)
		c.Assert(cResp.StatusCode(), equals(), 201)

		aResp, err := file.AppendData(ctx, 0, strings.NewReader(string(make([]byte, defaultBlobFSFileSizeInBytes))))
		c.AssertNoErr(err)
		c.Assert(aResp.StatusCode(), equals(), 202)

		fResp, err := file.FlushData(ctx, defaultBlobFSFileSizeInBytes, nil, azbfs.BlobFSHTTPHeaders{}, false, true)
		c.AssertNoErr(err)
		c.Assert(fResp.StatusCode(), equals(), 200)

	}
}

// Golang does not have sets, so we have to use a map to fulfill the same functionality
func (scenarioHelper) convertListToMap(list []*testObject, converter func(*testObject) string) map[string]int {
	lookupMap := make(map[string]int)
	for _, entry := range list {
		entryName := converter(entry)
		lookupMap[entryName] = 0
	}

	return lookupMap
}

//nolint
func (scenarioHelper) shaveOffPrefix(list []string, prefix string) []string {
	cleanList := make([]string, len(list))
	for i, item := range list {
		cleanList[i] = strings.TrimPrefix(item, prefix)
	}
	return cleanList
}

//nolint
func (scenarioHelper) addPrefix(list []string, prefix string) []string {
	modifiedList := make([]string, len(list))
	for i, item := range list {
		modifiedList[i] = prefix + item
	}
	return modifiedList
}

//nolint
func (scenarioHelper) getRawContainerURLWithSAS(c asserter, containerName string) url.URL {
	accountName, accountKey := GlobalInputManager{}.GetAccountAndKey(EAccountType.Standard())
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	c.AssertNoErr(err)
	containerURLWithSAS := getContainerURLWithSAS(c, *credential, containerName)
	return containerURLWithSAS.URL()
}

//nolint
func (scenarioHelper) getRawBlobURLWithSAS(c asserter, containerName string, blobName string) url.URL {
	accountName, accountKey := GlobalInputManager{}.GetAccountAndKey(EAccountType.Standard())
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	c.AssertNoErr(err)
	containerURLWithSAS := getContainerURLWithSAS(c, *credential, containerName)
	blobURLWithSAS := containerURLWithSAS.NewBlockBlobURL(blobName)
	return blobURLWithSAS.URL()
}

//nolint
func (scenarioHelper) getRawBlobServiceURLWithSAS(c asserter) url.URL {
	accountName, accountKey := GlobalInputManager{}.GetAccountAndKey(EAccountType.Standard())
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	c.AssertNoErr(err)

	return getBlobServiceURLWithSAS(c, *credential).URL()
}

//nolint
func (scenarioHelper) getRawFileServiceURLWithSAS(c asserter) url.URL {
	accountName, accountKey := GlobalInputManager{}.GetAccountAndKey(EAccountType.Standard())
	credential, err := azfile.NewSharedKeyCredential(accountName, accountKey)
	c.AssertNoErr(err)

	return getFileServiceURLWithSAS(c, *credential).URL()
}

//nolint
func (scenarioHelper) getRawAdlsServiceURLWithSAS(c asserter) azbfs.ServiceURL {
	accountName, accountKey := GlobalInputManager{}.GetAccountAndKey(EAccountType.Standard())
	credential := azbfs.NewSharedKeyCredential(accountName, accountKey)

	return getAdlsServiceURLWithSAS(c, *credential)
}

//nolint
func (scenarioHelper) getBlobServiceURL(c asserter) azblob.ServiceURL {
	accountName, accountKey := GlobalInputManager{}.GetAccountAndKey(EAccountType.Standard())
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	c.AssertNoErr(err)
	rawURL := fmt.Sprintf("https://%s.blob.core.windows.net", credential.AccountName())

	// convert the raw url and validate it was parsed successfully
	fullURL, err := url.Parse(rawURL)
	c.AssertNoErr(err)

	return azblob.NewServiceURL(*fullURL, azblob.NewPipeline(credential, azblob.PipelineOptions{}))
}

//nolint
func (s scenarioHelper) getContainerURL(c asserter, containerName string) azblob.ContainerURL {
	serviceURL := s.getBlobServiceURL(c)
	containerURL := serviceURL.NewContainerURL(containerName)

	return containerURL
}

//nolint
func (scenarioHelper) getRawS3AccountURL(c asserter, region string) url.URL {
	rawURL := fmt.Sprintf("https://s3%s.amazonaws.com", common.IffString(region == "", "", "-"+region))

	fullURL, err := url.Parse(rawURL)
	c.AssertNoErr(err)

	return *fullURL
}

// TODO: Possibly add virtual-hosted-style and dual stack support. Currently use path style for testing.
//nolint
func (scenarioHelper) getRawS3BucketURL(c asserter, region string, bucketName string) url.URL {
	rawURL := fmt.Sprintf("https://s3%s.amazonaws.com/%s", common.IffString(region == "", "", "-"+region), bucketName)

	fullURL, err := url.Parse(rawURL)
	c.AssertNoErr(err)

	return *fullURL
}

//nolint
func (scenarioHelper) getRawS3ObjectURL(c asserter, region string, bucketName string, objectName string) url.URL {
	rawURL := fmt.Sprintf("https://s3%s.amazonaws.com/%s/%s", common.IffString(region == "", "", "-"+region), bucketName, objectName)

	fullURL, err := url.Parse(rawURL)
	c.AssertNoErr(err)

	return *fullURL
}

//nolint
func (scenarioHelper) getRawFileURLWithSAS(c asserter, shareName string, fileName string) url.URL {
	credential, err := getGenericCredentialForFile("")
	c.AssertNoErr(err)
	shareURLWithSAS := getShareURLWithSAS(c, *credential, shareName)
	fileURLWithSAS := shareURLWithSAS.NewRootDirectoryURL().NewFileURL(fileName)
	return fileURLWithSAS.URL()
}

//nolint
func (scenarioHelper) getRawShareURLWithSAS(c asserter, shareName string) url.URL {
	accountName, accountKey := GlobalInputManager{}.GetAccountAndKey(EAccountType.Standard())
	credential, err := azfile.NewSharedKeyCredential(accountName, accountKey)
	c.AssertNoErr(err)
	shareURLWithSAS := getShareURLWithSAS(c, *credential, shareName)
	return shareURLWithSAS.URL()
}
