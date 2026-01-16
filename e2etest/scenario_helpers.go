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
	"bytes"
	"compress/gzip"
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/appendblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/pageblob"
	blobservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/datalakeerror"
	datalakedirectory "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/directory"
	datalakefile "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/file"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/filesystem"
	datalakeservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/service"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/directory"
	sharefile "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	fileservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/google/uuid"

	"github.com/Azure/azure-storage-azcopy/v10/sddl"
	"github.com/minio/minio-go/v7"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

const defaultFileSize = 1024
const defaultStringFileSize = "1k"

type scenarioHelper struct{}

var specialNames = []string{
	"打麻将.txt",
	"wow such space so much space",
	"打%%#%@#%麻将.txt",
	// "saywut.pdf?yo=bla&WUWUWU=foo&sig=yyy", // TODO this breaks on windows, figure out a way to add it only for tests on Unix
	"coração",
	"আপনার নাম কি",
	"%4509%4254$85140&",
	"Donaudampfschifffahrtselektrizitätenhauptbetriebswerkbauunterbeamtengesellschaft",
	"お名前は何ですか",
	"Adın ne",
	"як вас звати",
}

// note: this is to emulate the list-of-files flag
func (scenarioHelper) generateListOfFiles(c asserter, fileList []string) (path string) {
	parentDirName, err := os.MkdirTemp("", "AzCopyLocalTest")
	c.AssertNoErr(err)

	// create the file
	path = common.GenerateFullPath(parentDirName, generateName(c, "listy", 0))
	err = os.MkdirAll(filepath.Dir(path), os.ModePerm)
	c.AssertNoErr(err)

	// pipe content into it
	content := strings.Join(fileList, "\n")
	err = os.WriteFile(path, []byte(content), common.DEFAULT_FILE_PERM)
	c.AssertNoErr(err)
	return
}

func (scenarioHelper) generateLocalDirectory(c asserter) (dstDirName string) {
	dstDirName, err := os.MkdirTemp("", "AzCopyLocalTest")
	c.AssertNoErr(err)
	return
}

// create a test file
func (scenarioHelper) generateLocalFile(filePath string, fileSize int, body []byte) ([]byte, error) {
	if body == nil {
		// generate random data
		_, body = getRandomDataAndReader(fileSize)
	}

	// create all parent directories
	err := os.MkdirAll(filepath.Dir(filePath), os.ModePerm)
	if err != nil {
		return nil, err
	}

	// write to file and return the data
	err = os.WriteFile(filePath, body, common.DEFAULT_FILE_PERM)
	return body, err
}

type generateLocalFilesFromList struct {
	dirPath string
	generateFromListOptions
}

func (s scenarioHelper) generateLocalFilesFromList(c asserter, options *generateLocalFilesFromList) {
	for _, file := range options.fs {
		var err error
		destFile := filepath.Join(options.dirPath, file.name)

		if file.isFolder() {
			err = os.MkdirAll(destFile, os.ModePerm)
			c.AssertNoErr(err)
			// TODO: You'll need to set up things like attributes, and other relevant things from
			//   file.creationProperties here. (Use all the properties of file.creationProperties that are supported
			//			//   by local files. E.g. not contentHeaders or metadata).

			if file.creationProperties.smbPermissionsSddl != nil {
				osScenarioHelper{}.setFileSDDLString(c, filepath.Join(options.dirPath, file.name), *file.creationProperties.smbPermissionsSddl)
			}
			if file.creationProperties.lastWriteTime != nil {
				c.AssertNoErr(os.Chtimes(destFile, time.Now(), *file.creationProperties.lastWriteTime), "set times")
			}
		} else if file.creationProperties.entityType == common.EEntityType.File() {
			var mode uint32
			if file.creationProperties.posixProperties != nil && file.creationProperties.posixProperties.mode != nil {
				mode = *file.creationProperties.posixProperties.mode
			}
			switch {
			case mode&common.S_IFIFO == common.S_IFIFO || mode&common.S_IFSOCK == common.S_IFSOCK:
				osScenarioHelper{}.Mknod(c, destFile, mode, 0)
			default:
				sourceData, err := s.generateLocalFile(
					destFile,
					file.creationProperties.sizeBytes(c, options.defaultSize), file.body)
				if file.creationProperties.contentHeaders == nil {
					file.creationProperties.contentHeaders = &contentHeaders{}
				}

				if file.creationProperties.contentHeaders.contentMD5 == nil {
					contentMD5 := md5.Sum(sourceData)
					file.creationProperties.contentHeaders.contentMD5 = contentMD5[:]
				}

				c.AssertNoErr(err)
			}
			// TODO: You'll need to set up things like attributes, and other relevant things from
			//   file.creationProperties here. (Use all the properties of file.creationProperties that are supported
			//   by local files. E.g. not contentHeaders or metadata).

			if file.creationProperties.smbPermissionsSddl != nil {
				osScenarioHelper{}.setFileSDDLString(c, destFile, *file.creationProperties.smbPermissionsSddl)
			}
			if file.creationProperties.lastWriteTime != nil {
				c.AssertNoErr(os.Chtimes(destFile, time.Now(), *file.creationProperties.lastWriteTime), "set times")
			} else if file.creationProperties.posixProperties.HasTimes() {
				aTime, mTime := time.Now(), time.Now()
				props := file.creationProperties.posixProperties

				if props.modTime != nil {
					mTime = *props.modTime
				}

				if props.accessTime != nil {
					aTime = *props.accessTime
				}

				c.AssertNoErr(os.Chtimes(destFile, aTime, mTime), "set times")
			}
		} else if file.creationProperties.entityType == common.EEntityType.Symlink() {
			c.Assert(file.creationProperties.symlinkTarget, notEquals(), nil)
			oldName := filepath.Join(options.dirPath, *file.creationProperties.symlinkTarget)
			c.AssertNoErr(os.Symlink(oldName, destFile))
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
		entityType := common.EEntityType.File()
		if info.IsDir() {
			entityType = common.EEntityType.Folder()
		} else if info.Mode()&os.ModeSymlink == os.ModeSymlink {
			entityType = common.EEntityType.Symlink()
		}

		props := objectProperties{
			entityType:         entityType,
			size:               &size,
			creationTime:       pCreationTime,
			lastWriteTime:      &lastWriteTime,
			smbAttributes:      pSmbAttributes,
			smbPermissionsSddl: pSmbPermissionsSddl,
			// contentHeaders don't exist on local file system
			// nameValueMetadata doesn't exist on local file system
		}

		result[relPath] = &props
		return nil
	})
	a.AssertNoErr(err)
	return result
}

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
			_, err := s.generateLocalFile(filepath.Join(dirPath, name), defaultFileSize, nil)
			c.AssertNoErr(err)
		}
	}

	// sleep a bit so that the files' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
	return
}

func (scenarioHelper) generateCommonRemoteScenarioForBlob(c asserter, containerClient *container.Client, prefix string) (blobList []string) {
	// make 50 blobs with random names
	// 10 of them at the top level
	// 10 of them in sub dir "sub1"
	// 10 of them in sub dir "sub2"
	// 10 of them in deeper sub dir "sub1/sub3/sub5"
	// 10 of them with special characters
	blobList = make([]string, 50)

	for i := 0; i < 10; i++ {
		_, blobName1 := createNewBlockBlob(c, containerClient, prefix+"top")
		_, blobName2 := createNewBlockBlob(c, containerClient, prefix+"sub1/")
		_, blobName3 := createNewBlockBlob(c, containerClient, prefix+"sub2/")
		_, blobName4 := createNewBlockBlob(c, containerClient, prefix+"sub1/sub3/sub5/")
		_, blobName5 := createNewBlockBlob(c, containerClient, prefix+specialNames[i])

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

func (scenarioHelper) generateCommonRemoteScenarioForBlobFS(c asserter, fsc *filesystem.Client, prefix string) (pathList []string) {
	pathList = make([]string, 50)

	for i := 0; i < 10; i++ {
		_, pathName1 := createNewBfsFile(c, fsc, prefix+"top")
		_, pathName2 := createNewBfsFile(c, fsc, prefix+"sub1/")
		_, pathName3 := createNewBfsFile(c, fsc, prefix+"sub2/")
		_, pathName4 := createNewBfsFile(c, fsc, prefix+"sub1/sub3/sub5")
		_, pathName5 := createNewBfsFile(c, fsc, prefix+specialNames[i])

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

func (scenarioHelper) generateCommonRemoteScenarioForAzureFile(c asserter, shareClient *share.Client, prefix string) (fileList []string) {
	fileList = make([]string, 50)

	for i := 0; i < 10; i++ {
		_, fileName1 := createNewAzureFile(c, shareClient, prefix+"top")
		_, fileName2 := createNewAzureFile(c, shareClient, prefix+"sub1/")
		_, fileName3 := createNewAzureFile(c, shareClient, prefix+"sub2/")
		_, fileName4 := createNewAzureFile(c, shareClient, prefix+"sub1/sub3/sub5/")
		_, fileName5 := createNewAzureFile(c, shareClient, prefix+specialNames[i])

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

func (s scenarioHelper) generateBlobContainersAndBlobsFromLists(c asserter, serviceClient *blobservice.Client, containerList []string, blobList []*testObject) {
	for _, containerName := range containerList {
		curl := serviceClient.NewContainerClient(containerName)
		_, err := curl.Create(ctx, nil)
		c.AssertNoErr(err)
		s.generateBlobsFromList(c, &generateBlobFromListOptions{
			containerClient: curl,
			generateFromListOptions: generateFromListOptions{
				fs:          blobList,
				defaultSize: defaultStringFileSize,
			},
		})
	}
}

func (s scenarioHelper) generateFileSharesAndFilesFromLists(c asserter, serviceClient *fileservice.Client, shareList []string, fileList []*testObject) {
	for _, shareName := range shareList {
		sURL := serviceClient.NewShareClient(shareName)
		_, err := sURL.Create(ctx, nil)
		c.AssertNoErr(err)

		s.generateAzureFilesFromList(c, &generateAzureFilesFromListOptions{
			shareClient: sURL,
			fileList:    fileList,
			defaultSize: defaultStringFileSize,
		})
	}
}

func (s scenarioHelper) generateFilesystemsAndFilesFromLists(c asserter, dsc *datalakeservice.Client, fsList []string, fileList []string, data string) {
	for _, filesystemName := range fsList {
		fsc := dsc.NewFileSystemClient(filesystemName)
		_, err := fsc.Create(ctx, nil)
		c.AssertNoErr(err)

		s.generateBFSPathsFromList(c, fsc, fileList)
	}
}

func (s scenarioHelper) generateS3BucketsAndObjectsFromLists(c asserter, s3Client *minio.Client, bucketList []string, objectList []string, data string) {
	for _, bucketName := range bucketList {
		err := s3Client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{Region: ""})
		c.AssertNoErr(err)

		s.generateObjects(c, s3Client, bucketName, objectList)
	}
}

type generateFromListOptions struct {
	fs                      []*testObject
	defaultSize             string
	preservePosixProperties bool
	accountType             AccountType
}

type generateBlobFromListOptions struct {
	rawSASURL       url.URL
	containerClient *container.Client
	cpkInfo         *blob.CPKInfo
	cpkScopeInfo    *blob.CPKScopeInfo
	accessTier      *blob.AccessTier
	compressToGZ    bool
	generateFromListOptions
}

// create the demanded blobs
func (scenarioHelper) generateBlobsFromList(c asserter, options *generateBlobFromListOptions) {
	for _, b := range options.fs {
		switch b.creationProperties.entityType {
		case common.EEntityType.Folder(): // it's fine to create folders even when we're not explicitly testing them, UNLESS we're testing CPK-- AzCopy can't properly pick that up!
			if options.cpkInfo != nil || b.name == "" {
				continue // can't write root, and can't handle dirs with CPK
			}

			if b.creationProperties.nameValueMetadata == nil {
				b.creationProperties.nameValueMetadata = map[string]*string{}
			}

			b.body = make([]byte, 0)
			b.creationProperties.nameValueMetadata[common.POSIXFolderMeta] = to.Ptr("true")
			mode := uint64(os.FileMode(common.DEFAULT_FILE_PERM) | os.ModeDir)
			b.creationProperties.nameValueMetadata[common.POSIXModeMeta] = to.Ptr(strconv.FormatUint(mode, 10))
			b.creationProperties.posixProperties.AddToMetadata(b.creationProperties.nameValueMetadata)
		case common.EEntityType.Symlink():
			if b.creationProperties.nameValueMetadata == nil {
				b.creationProperties.nameValueMetadata = map[string]*string{}
			}

			b.body = []byte(*b.creationProperties.symlinkTarget)
			b.creationProperties.nameValueMetadata[common.POSIXSymlinkMeta] = to.Ptr("true")
			mode := uint64(os.FileMode(common.DEFAULT_FILE_PERM) | os.ModeSymlink)
			b.creationProperties.nameValueMetadata[common.POSIXModeMeta] = to.Ptr(strconv.FormatUint(mode, 10))
			b.creationProperties.posixProperties.AddToMetadata(b.creationProperties.nameValueMetadata)
		default:
			if b.creationProperties.nameValueMetadata == nil {
				b.creationProperties.nameValueMetadata = map[string]*string{}
			}

			b.creationProperties.posixProperties.AddToMetadata(b.creationProperties.nameValueMetadata)

			if b.creationProperties.posixProperties != nil && b.creationProperties.posixProperties.mode != nil {
				mode := *b.creationProperties.posixProperties.mode

				// todo: support for device rep files may be difficult in a testing environment.
				if mode&common.S_IFSOCK == common.S_IFSOCK || mode&common.S_IFIFO == common.S_IFIFO {
					b.body = make([]byte, 0)
				}
			}
		}

		blobHadBody := b.body != nil
		versionsRequested := common.IffNotNil[uint](b.creationProperties.blobVersions, 1)
		versionsCreated := uint(0)

		for versionsCreated < versionsRequested {
			versionsCreated++

			ad := blobResourceAdapter{b}
			var reader io.ReadSeekCloser
			var size int
			var sourceData []byte
			if b.body != nil && blobHadBody {
				reader = streaming.NopCloser(bytes.NewReader(b.body))
				sourceData = b.body
				size = len(b.body)
			} else {
				reader, sourceData = getRandomDataAndReader(b.creationProperties.sizeBytes(c, options.defaultSize))
				b.body = sourceData // set body
				size = len(b.body)
			}

			if options.compressToGZ {
				var buff bytes.Buffer
				gz := gzip.NewWriter(&buff)
				if _, err := gz.Write([]byte(sourceData)); err != nil {
					c.AssertNoErr(err)
				}
				if err := gz.Close(); err != nil {
					c.AssertNoErr(err)
				}
				if ad.obj.creationProperties.contentHeaders == nil {
					ad.obj.creationProperties.contentHeaders = &contentHeaders{}
				}
				contentEncoding := "gzip"
				ad.obj.creationProperties.contentHeaders.contentEncoding = &contentEncoding
				b.body = buff.Bytes()
				b.name += ".gz"
			}

			// Setting content MD5
			if ad.obj.creationProperties.contentHeaders == nil {
				b.creationProperties.contentHeaders = &contentHeaders{}
			}
			// only set MD5 when we're on the last version
			if ad.obj.creationProperties.contentHeaders.contentMD5 == nil && versionsCreated == versionsRequested {
				contentMD5 := md5.Sum(sourceData)
				ad.obj.creationProperties.contentHeaders.contentMD5 = contentMD5[:]
			}

			tags := ad.obj.creationProperties.blobTags
			metadata := ad.obj.creationProperties.nameValueMetadata

			if options.accountType == EAccountType.HierarchicalNamespaceEnabled() {
				tags = nil
			}

			headers := ad.toHeaders()

			var err error

			switch b.creationProperties.blobType {
			case common.EBlobType.BlockBlob(), common.EBlobType.Detect():
				bb := options.containerClient.NewBlockBlobClient(b.name)

				if size > 0 {
					// to prevent the service from erroring out with an improper MD5, we opt to commit a block, then the list.
					blockID := base64.StdEncoding.EncodeToString([]byte(uuid.NewString()))
					_, err = bb.StageBlock(ctx, blockID, reader,
						&blockblob.StageBlockOptions{
							CPKInfo:      options.cpkInfo,
							CPKScopeInfo: options.cpkScopeInfo,
						})

					c.AssertNoErr(err)

					// Commit block list will generate a new version.
					_, err = bb.CommitBlockList(ctx,
						[]string{blockID},
						&blockblob.CommitBlockListOptions{
							HTTPHeaders:  headers,
							Metadata:     metadata,
							Tier:         options.accessTier,
							Tags:         tags,
							CPKInfo:      options.cpkInfo,
							CPKScopeInfo: options.cpkScopeInfo,
						})

					c.AssertNoErr(err)
				} else { // todo: invalid MD5 on empty blob is impossible like this, but it's doubtful we'll need to support it.
					// handle empty blobs
					_, err := bb.Upload(ctx, reader,
						&blockblob.UploadOptions{
							HTTPHeaders:  headers,
							Metadata:     metadata,
							Tier:         options.accessTier,
							Tags:         tags,
							CPKInfo:      options.cpkInfo,
							CPKScopeInfo: options.cpkScopeInfo,
						})

					c.AssertNoErr(err)
				}
			case common.EBlobType.PageBlob():
				// A create call will generate a new version
				pb := options.containerClient.NewPageBlobClient(b.name)
				_, err := pb.Create(ctx, int64(size),
					&pageblob.CreateOptions{
						SequenceNumber: to.Ptr(int64(0)),
						HTTPHeaders:    headers,
						Metadata:       metadata,
						Tags:           tags,
						CPKInfo:        options.cpkInfo,
						CPKScopeInfo:   options.cpkScopeInfo,
					})
				c.AssertNoErr(err)

				_, err = pb.UploadPages(ctx, reader, blob.HTTPRange{Offset: 0, Count: int64(size)},
					&pageblob.UploadPagesOptions{
						CPKInfo:      options.cpkInfo,
						CPKScopeInfo: options.cpkScopeInfo,
					})
				c.AssertNoErr(err)
			case common.EBlobType.AppendBlob():
				// A create call will generate a new version
				ab := options.containerClient.NewAppendBlobClient(b.name)
				_, err := ab.Create(ctx,
					&appendblob.CreateOptions{
						HTTPHeaders:  headers,
						Metadata:     metadata,
						Tags:         tags,
						CPKInfo:      options.cpkInfo,
						CPKScopeInfo: options.cpkScopeInfo,
					})
				c.AssertNoErr(err)

				_, err = ab.AppendBlock(ctx, reader,
					&appendblob.AppendBlockOptions{
						CPKInfo:      options.cpkInfo,
						CPKScopeInfo: options.cpkScopeInfo,
					})
				c.AssertNoErr(err)
			}
		}

		if b.creationProperties.adlsPermissionsACL != nil {
			bfsURLParts, err := azdatalake.ParseURL(options.rawSASURL.String())
			c.AssertNoErr(err)
			bfsURLParts.Host = strings.Replace(bfsURLParts.Host, ".blob", ".dfs", 1)

			fsc, err := filesystem.NewClientWithNoCredential(bfsURLParts.String(), nil)
			c.AssertNoErr(err)

			if b.isFolder() {
				dc := fsc.NewDirectoryClient(b.name)

				_, err = dc.SetAccessControl(ctx,
					&datalakedirectory.SetAccessControlOptions{ACL: b.creationProperties.adlsPermissionsACL})
			} else {
				d, f := path.Split(b.name)
				dc := fsc.NewDirectoryClient(d)
				fc, err := dc.NewFileClient(f)
				c.AssertNoErr(err)

				_, err = fc.SetAccessControl(ctx,
					&datalakefile.SetAccessControlOptions{ACL: b.creationProperties.adlsPermissionsACL})
			}

			c.AssertNoErr(err)
		}
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	// TODO: can we make it so that this sleeping only happens when we really need it to?
	time.Sleep(time.Millisecond * 1050)
}

func (s scenarioHelper) enumerateContainerBlobProperties(a asserter, containerClient *container.Client, fileSystemURL *filesystem.Client) map[string]*objectProperties {
	result := make(map[string]*objectProperties)

	pager := containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{Include: container.ListBlobsInclude{Metadata: true, Tags: true}})
	for pager.More() {
		listBlob, err := pager.NextPage(context.TODO())
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
			md := blobInfo.Metadata

			var acl *string
			if fileSystemURL != nil {
				fURL := fileSystemURL.NewFileClient(*relativePath)
				accessControl, err := fURL.GetAccessControl(ctx, nil)

				if datalakeerror.HasCode(err, "FilesystemNotFound") {
					err = nil
					acl = nil
				} else {
					a.AssertNoErr(err, "getting ACLs")
					acl = accessControl.ACL
				}
			}

			props := objectProperties{
				entityType:        common.EEntityType.File(), // todo: posix properties includes folders
				size:              bp.ContentLength,
				contentHeaders:    &h,
				nameValueMetadata: md,
				creationTime:      bp.CreationTime,
				lastWriteTime:     bp.LastModified,
				cpkInfo:           &blob.CPKInfo{EncryptionKeySHA256: bp.CustomerProvidedKeySHA256},
				cpkScopeInfo:      &blob.CPKScopeInfo{EncryptionScope: bp.EncryptionScope},
				// TODO : Return ACL in list
				adlsPermissionsACL: acl,
				// smbAttributes and smbPermissions don't exist in blob
			}

			if blobInfo.BlobTags != nil {
				blobTagsMap := common.BlobTags{}
				for _, blobTag := range blobInfo.BlobTags.BlobTagSet {
					blobTagsMap[url.QueryEscape(*blobTag.Key)] = url.QueryEscape(*blobTag.Value)
				}
				props.blobTags = blobTagsMap
			}

			switch *blobInfo.Properties.BlobType {
			case blob.BlobTypeBlockBlob:
				props.blobType = common.EBlobType.BlockBlob()
			case blob.BlobTypePageBlob:
				props.blobType = common.EBlobType.PageBlob()
			case blob.BlobTypeAppendBlob:
				props.blobType = common.EBlobType.AppendBlob()
			default:
				props.blobType = common.EBlobType.Detect()
			}

			result[*relativePath] = &props
		}
	}

	return result
}

func (s scenarioHelper) downloadBlobContent(a asserter, options downloadContentOptions) []byte {
	blobClient := options.containerClient.NewBlobClient(options.resourceRelPath)
	downloadResp, err := blobClient.DownloadStream(ctx, &blob.DownloadStreamOptions{CPKInfo: options.cpkInfo, CPKScopeInfo: options.cpkScopeInfo})
	a.AssertNoErr(err)

	destData, err := io.ReadAll(downloadResp.Body)
	defer downloadResp.Body.Close()
	a.AssertNoErr(err)
	return destData
}

func (scenarioHelper) generatePageBlobsFromList(c asserter, containerClient *container.Client, blobList []string, data string) {
	for _, blobName := range blobList {
		// Create the blob (PUT blob)
		bc := containerClient.NewPageBlobClient(blobName)
		_, err := bc.Create(ctx,
			int64(len(data)),
			&pageblob.CreateOptions{
				SequenceNumber: to.Ptr(int64(0)),
				HTTPHeaders:    &blob.HTTPHeaders{BlobContentType: to.Ptr("text/random")},
			})
		c.AssertNoErr(err)

		// Create the page (PUT page)
		_, err = bc.UploadPages(ctx,
			streaming.NopCloser(strings.NewReader(data)),
			blob.HTTPRange{Offset: 0, Count: int64(len(data))},
			nil)
		c.AssertNoErr(err)
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
}

func (scenarioHelper) generateAppendBlobsFromList(c asserter, containerClient *container.Client, blobList []string, data string) {
	for _, blobName := range blobList {
		// Create the blob (PUT blob)
		bc := containerClient.NewAppendBlobClient(blobName)
		_, err := bc.Create(ctx,
			&appendblob.CreateOptions{
				HTTPHeaders: &blob.HTTPHeaders{BlobContentType: to.Ptr("text/random")},
			})
		c.AssertNoErr(err)

		// Append a block (PUT block)
		_, err = bc.AppendBlock(ctx, streaming.NopCloser(strings.NewReader(data)), nil)
		c.AssertNoErr(err)
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
}

func (scenarioHelper) generateBlockBlobWithAccessTier(c asserter, containerClient *container.Client, blobName string, accessTier *blob.AccessTier) {
	bc := containerClient.NewBlockBlobClient(blobName)
	_, err := bc.Upload(ctx, streaming.NopCloser(strings.NewReader(blockBlobDefaultData)),
		&blockblob.UploadOptions{
			Tier: accessTier,
		})
	c.AssertNoErr(err)
}

// create the demanded objects
func (scenarioHelper) generateObjects(c asserter, client *minio.Client, bucketName string, objectList []string) {
	size := int64(len(objectDefaultData))
	for _, objectName := range objectList {
		n, err := client.PutObject(ctx, bucketName, objectName, strings.NewReader(objectDefaultData), size, minio.PutObjectOptions{})
		c.AssertNoErr(err)
		c.Assert(n, equals(), size)
	}
}

// create the demanded files
func (scenarioHelper) generateFlatFiles(c asserter, shareClient *share.Client, fileList []string) {
	for _, fileName := range fileList {
		fileClient := shareClient.NewRootDirectoryClient().NewFileClient(fileName)
		err := fileClient.UploadBuffer(ctx, []byte(fileDefaultData), nil)
		c.AssertNoErr(err)
	}

	// sleep a bit so that the blobs' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
}

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
		// transfer.Source & transfer.Destination(after trimming the SourceRoot and DestinationRoot) are with AZCOPY_PATH_SEPARATOR_STRING suffix,
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
	shareClient *share.Client
	fileList    []*testObject
	defaultSize string
}

// create the demanded azure files
func (scenarioHelper) generateAzureFilesFromList(c asserter, options *generateAzureFilesFromListOptions) {
	for _, f := range options.fileList {
		ad := filesResourceAdapter{f}
		if f.isFolder() {
			// make sure the dir exists
			file := options.shareClient.NewRootDirectoryClient().NewFileClient(path.Join(f.name, "dummyChild"))
			generateParentsForAzureFile(c, file, options.shareClient)

			dir := options.shareClient.NewRootDirectoryClient().NewSubdirectoryClient(f.name)

			// set its metadata if any
			if f.creationProperties.nameValueMetadata != nil {
				_, err := dir.SetMetadata(context.TODO(), &directory.SetMetadataOptions{Metadata: ad.obj.creationProperties.nameValueMetadata})
				c.AssertNoErr(err)
			}

			if f.creationProperties.smbPermissionsSddl != nil || f.creationProperties.smbAttributes != nil || f.creationProperties.lastWriteTime != nil {
				_, err := dir.SetProperties(ctx, &directory.SetPropertiesOptions{
					FileSMBProperties: ad.toSMBProperties(c),
					FilePermissions:   ad.toPermissions(c, options.shareClient),
				})
				c.AssertNoErr(err)

				if f.creationProperties.smbPermissionsSddl != nil {
					prop, err := dir.GetProperties(ctx, nil)
					c.AssertNoErr(err)

					perm, err := options.shareClient.GetPermission(ctx, *prop.FilePermissionKey, nil)
					c.AssertNoErr(err)

					dest, _ := sddl.ParseSDDL(*perm.Permission)
					source, _ := sddl.ParseSDDL(*f.creationProperties.smbPermissionsSddl)

					c.Assert(dest.Compare(source), equals(), true)
				}
			}

			// set other properties
			// TODO: do we need a SetProperties method on dir...?  Discuss with zezha-msft
			if f.creationProperties.creationTime != nil {
				panic("setting these properties isn't implemented yet for folders in the test harness")
				// TODO: nakulkar-msft the attributes stuff will need to be implemented here before attributes can be tested on Azure Files
			}

			// TODO: I'm pretty sure we don't prserve lastWritetime or contentProperties (headers) for folders, so the above if statement doesn't test those
			//    Is that the correct decision?
		} else if f.creationProperties.entityType == common.EEntityType.File() {
			fileClient := options.shareClient.NewRootDirectoryClient().NewFileClient(f.name)

			// create parents first
			generateParentsForAzureFile(c, fileClient, options.shareClient)

			// create the file itself
			fileSize := int64(f.creationProperties.sizeBytes(c, options.defaultSize))
			var contentR io.ReadSeekCloser
			var contentD []byte
			if f.body != nil {
				contentR = streaming.NopCloser(bytes.NewReader(f.body))
				contentD = f.body
				fileSize = int64(len(f.body))
			} else {
				contentR, contentD = getRandomDataAndReader(int(fileSize))
				f.body = contentD
				fileSize = int64(len(f.body))
			}
			if f.creationProperties.contentHeaders == nil {
				f.creationProperties.contentHeaders = &contentHeaders{}
			}
			if f.creationProperties.contentHeaders.contentMD5 == nil {
				contentMD5 := md5.Sum(contentD)
				f.creationProperties.contentHeaders.contentMD5 = contentMD5[:]
			}

			_, err := fileClient.Create(ctx, fileSize, &sharefile.CreateOptions{
				SMBProperties: ad.toSMBProperties(c),
				Permissions:   ad.toPermissions(c, options.shareClient),
				HTTPHeaders:   ad.toHeaders(),
				Metadata:      ad.obj.creationProperties.nameValueMetadata,
			})
			c.AssertNoErr(err)

			_, err = fileClient.UploadRange(context.Background(), 0, contentR, nil)
			if err == nil {
				c.Failed()
			}

			if f.creationProperties.smbPermissionsSddl != nil || f.creationProperties.smbAttributes != nil || f.creationProperties.lastWriteTime != nil {
				/*
					via Jason Shay:
					Providing securityKey/SDDL during 'PUT File' and 'PUT Properties' can and will provide different results/semantics.
					This is true for the REST PUT commands, as well as locally when providing a SECURITY_DESCRIPTOR in the SECURITY_ATTRIBUTES structure in the CreateFile() call.
					In both cases of file creation (CreateFile() and REST PUT File), the actual security descriptor applied to the file can undergo some changes as compared to the input.

					SetProperties() (and NtSetSecurityObject) use update semantics, so it should store what you provide it (with a couple exceptions).
					And on the cloud share, you would need 'Set Properties' to be called as a final step, to save the final ACLs with 'update' semantics.


				*/

				_, err := fileClient.SetHTTPHeaders(ctx, &sharefile.SetHTTPHeadersOptions{
					HTTPHeaders:   ad.toHeaders(),
					SMBProperties: ad.toSMBProperties(c),
					Permissions:   ad.toPermissions(c, options.shareClient),
				})
				c.AssertNoErr(err)

				if f.creationProperties.smbPermissionsSddl != nil {
					prop, err := fileClient.GetProperties(ctx, nil)
					c.AssertNoErr(err)

					perm, err := options.shareClient.GetPermission(ctx, *prop.FilePermissionKey, nil)
					c.AssertNoErr(err)

					dest, _ := sddl.ParseSDDL(*perm.Permission)
					source, _ := sddl.ParseSDDL(*f.creationProperties.smbPermissionsSddl)

					c.Assert(dest.Compare(source), equals(), true)
				}
			}

			// TODO: do we want to put some random content into it?
		} else {
			panic(fmt.Sprintf("file %s unsupported entity type %s", f.name, f.creationProperties.entityType.String()))
		}
	}

	// sleep a bit so that the files' lmts are guaranteed to be in the past
	time.Sleep(time.Millisecond * 1050)
}

func (s scenarioHelper) enumerateShareFileProperties(a asserter, sc *share.Client) map[string]*objectProperties {
	var dirQ []*directory.Client
	result := make(map[string]*objectProperties)

	root := sc.NewRootDirectoryClient()
	rootProps, err := root.GetProperties(ctx, nil)
	a.AssertNoErr(err)
	rootAttr, err := sharefile.ParseNTFSFileAttributes(rootProps.FileAttributes)
	a.AssertNoErr(err)
	var rootPerm *string
	if permKey := rootProps.FilePermissionKey; permKey != nil {
		sharePerm, err := sc.GetPermission(ctx, *permKey, nil)
		a.AssertNoErr(err, "Failed to get permissions from key")

		rootPerm = sharePerm.Permission
	}
	result[""] = &objectProperties{
		entityType:         common.EEntityType.Folder(),
		smbPermissionsSddl: rootPerm,
		smbAttributes:      to.Ptr(ste.FileAttributesToUint32(*rootAttr)),
	}

	dirQ = append(dirQ, root)
	for i := 0; i < len(dirQ); i++ {
		currentDirURL := dirQ[i]
		pager := currentDirURL.NewListFilesAndDirectoriesPager(nil)
		for pager.More() {
			lResp, err := pager.NextPage(context.TODO())
			a.AssertNoErr(err)

			// Process the files and folders we listed
			for _, fileInfo := range lResp.Segment.Files {
				fileURL := currentDirURL.NewFileClient(*fileInfo.Name)
				fProps, err := fileURL.GetProperties(context.TODO(), nil)
				a.AssertNoErr(err)

				// Construct the properties object
				h := contentHeaders{
					cacheControl:       fProps.CacheControl,
					contentDisposition: fProps.ContentDisposition,
					contentEncoding:    fProps.ContentEncoding,
					contentLanguage:    fProps.ContentLanguage,
					contentType:        fProps.ContentType,
					contentMD5:         fProps.ContentMD5,
				}
				attr, err := sharefile.ParseNTFSFileAttributes(fProps.FileAttributes)
				a.AssertNoErr(err)
				fileAttrs := ste.FileAttributesToUint32(*attr)
				permissionKey := fProps.FilePermissionKey

				var perm string
				if permissionKey != nil {
					sharePerm, err := sc.GetPermission(ctx, *permissionKey, nil)
					a.AssertNoErr(err, "Failed to get permissions from key")

					perm = *sharePerm.Permission
				}

				props := objectProperties{
					entityType:         common.EEntityType.File(), // only enumerating files in list call
					size:               fProps.ContentLength,
					nameValueMetadata:  fProps.Metadata,
					contentHeaders:     &h,
					creationTime:       fProps.FileCreationTime,
					lastWriteTime:      fProps.FileLastWriteTime,
					smbAttributes:      &fileAttrs,
					smbPermissionsSddl: &perm,
				}

				relativePath := *lResp.DirectoryPath + "/"
				if relativePath == "/" {
					relativePath = ""
				}
				result[relativePath+*fileInfo.Name] = &props
			}

			for _, dirInfo := range lResp.Segment.Directories {
				dirURL := currentDirURL.NewSubdirectoryClient(*dirInfo.Name)
				dProps, err := dirURL.GetProperties(context.TODO(), nil)
				a.AssertNoErr(err)

				// Construct the properties object
				// Grab the permissions
				permissionKey := dProps.FilePermissionKey

				var perm string
				if permissionKey != nil {
					sharePerm, err := sc.GetPermission(ctx, *permissionKey, nil)
					a.AssertNoErr(err, "Failed to get permissions from key")

					perm = *sharePerm.Permission
				}

				// Set up properties
				props := objectProperties{
					entityType:         common.EEntityType.Folder(), // Only enumerating directories in list call
					nameValueMetadata:  dProps.Metadata,
					creationTime:       dProps.FileCreationTime,
					lastWriteTime:      dProps.FileLastWriteTime,
					smbPermissionsSddl: &perm,
				}

				// get the directory name properly
				relativePath := *lResp.DirectoryPath + "/"
				if relativePath == "/" {
					relativePath = ""
				}
				result[relativePath+*dirInfo.Name] = &props

				dirQ = append(dirQ, dirURL)
			}
		}
	}

	return result
}

func (s scenarioHelper) downloadFileContent(a asserter, options downloadContentOptions) []byte {
	fileURL := options.shareClient.NewRootDirectoryClient().NewFileClient(options.resourceRelPath)
	downloadResp, err := fileURL.DownloadStream(ctx, nil)
	a.AssertNoErr(err)

	destData, err := io.ReadAll(downloadResp.Body)
	defer downloadResp.Body.Close()
	a.AssertNoErr(err)
	return destData
}

func (scenarioHelper) generateBFSPathsFromList(c asserter, fsc *filesystem.Client, fileList []string) {
	for _, bfsPath := range fileList {
		fc := fsc.NewFileClient(bfsPath)

		// Create the file
		_, err := fc.Create(ctx, nil)
		c.AssertNoErr(err)

		_, err = fc.AppendData(ctx, 0, streaming.NopCloser(strings.NewReader(string(make([]byte, defaultBlobFSFileSizeInBytes)))), nil)
		c.AssertNoErr(err)

		_, err = fc.FlushData(ctx, defaultBlobFSFileSizeInBytes, &datalakefile.FlushDataOptions{Close: to.Ptr(true)})
		c.AssertNoErr(err)

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

func (scenarioHelper) shaveOffPrefix(list []string, prefix string) []string {
	cleanList := make([]string, len(list))
	for i, item := range list {
		cleanList[i] = strings.TrimPrefix(item, prefix)
	}
	return cleanList
}

func (scenarioHelper) addPrefix(list []string, prefix string) []string {
	modifiedList := make([]string, len(list))
	for i, item := range list {
		modifiedList[i] = prefix + item
	}
	return modifiedList
}

func (scenarioHelper) getRawContainerURLWithSAS(c asserter, containerName string) string {
	accountName, accountKey := GlobalInputManager{}.GetAccountAndKey(EAccountType.Standard())
	credential, err := blob.NewSharedKeyCredential(accountName, accountKey)
	c.AssertNoErr(err)
	containerURLWithSAS := getContainerURLWithSAS(c, credential, containerName)
	return containerURLWithSAS.URL()
}

func (scenarioHelper) getRawBlobURLWithSAS(c asserter, containerName string, blobName string) string {
	accountName, accountKey := GlobalInputManager{}.GetAccountAndKey(EAccountType.Standard())
	credential, err := blob.NewSharedKeyCredential(accountName, accountKey)
	c.AssertNoErr(err)
	containerURLWithSAS := getContainerURLWithSAS(c, credential, containerName)
	blobURLWithSAS := containerURLWithSAS.NewBlockBlobClient(blobName)
	return blobURLWithSAS.URL()
}

func (scenarioHelper) getRawBlobServiceURLWithSAS(c asserter) string {
	accountName, accountKey := GlobalInputManager{}.GetAccountAndKey(EAccountType.Standard())
	credential, err := blob.NewSharedKeyCredential(accountName, accountKey)
	c.AssertNoErr(err)

	return getBlobServiceURLWithSAS(c, credential).URL()
}

func (scenarioHelper) getRawFileServiceURLWithSAS(c asserter) string {
	accountName, accountKey := GlobalInputManager{}.GetAccountAndKey(EAccountType.Standard())
	credential, err := sharefile.NewSharedKeyCredential(accountName, accountKey)
	c.AssertNoErr(err)

	return getFileServiceURLWithSAS(c, credential).URL()
}

func (scenarioHelper) getRawAdlsServiceURLWithSAS(c asserter) *datalakeservice.Client {
	accountName, accountKey := GlobalInputManager{}.GetAccountAndKey(EAccountType.Standard())
	credential, err := azdatalake.NewSharedKeyCredential(accountName, accountKey)
	c.AssertNoErr(err)

	return getAdlsServiceURLWithSAS(c, credential)
}

func (scenarioHelper) getBlobServiceURL(c asserter) *blobservice.Client {
	accountName, accountKey := GlobalInputManager{}.GetAccountAndKey(EAccountType.Standard())
	credential, err := blob.NewSharedKeyCredential(accountName, accountKey)
	c.AssertNoErr(err)
	rawURL := fmt.Sprintf("https://%s.blob.core.windows.net", credential.AccountName())

	client, err := blobservice.NewClientWithSharedKeyCredential(rawURL, credential, nil)
	c.AssertNoErr(err)

	return client
}

func (s scenarioHelper) getContainerURL(c asserter, containerName string) *container.Client {
	serviceURL := s.getBlobServiceURL(c)
	containerURL := serviceURL.NewContainerClient(containerName)

	return containerURL
}

func (scenarioHelper) getRawS3AccountURL(c asserter, region string) url.URL {
	rawURL := fmt.Sprintf("https://s3%s.amazonaws.com", common.Iff(region == "", "", "-"+region))

	fullURL, err := url.Parse(rawURL)
	c.AssertNoErr(err)

	return *fullURL
}

// TODO: Possibly add virtual-hosted-style and dual stack support. Currently use path style for testing.
func (scenarioHelper) getRawS3BucketURL(c asserter, region string, bucketName string) url.URL {
	rawURL := fmt.Sprintf("https://s3%s.amazonaws.com/%s", common.Iff(region == "", "", "-"+region), bucketName)

	fullURL, err := url.Parse(rawURL)
	c.AssertNoErr(err)

	return *fullURL
}

func (scenarioHelper) getRawS3ObjectURL(c asserter, region string, bucketName string, objectName string) url.URL {
	rawURL := fmt.Sprintf("https://s3%s.amazonaws.com/%s/%s", common.Iff(region == "", "", "-"+region), bucketName, objectName)

	fullURL, err := url.Parse(rawURL)
	c.AssertNoErr(err)

	return *fullURL
}
