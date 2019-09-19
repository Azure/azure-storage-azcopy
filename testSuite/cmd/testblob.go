package cmd

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/spf13/cobra"
)

// TestBlobCommand represents the struct to get command
// for validating azcopy operations.

// defaultServiceApiVersion is the default value of service api version that is set as value to the ServiceAPIVersionOverride in every Job's context.
const defaultServiceApiVersion = "2017-04-17"

// todo check the number of contents uploaded while verifying.

type TestBlobCommand struct {
	// object is the resource which needs to be validated against a resource on container.
	Object string
	//Subject is the remote resource against which object needs to be validated.
	Subject string
	// IsObjectDirectory defines if the object is a directory or not.
	// If the object is directory, then validation goes through another path.
	IsObjectDirectory bool
	// Metadata of the blob to be validated.
	MetaData string
	// NoGuessMimeType represent the azcopy NoGuessMimeType flag set while uploading the blob.
	NoGuessMimeType bool
	// Represents the flag to determine whether number of blocks or pages needs
	// to be verified or not.
	// todo always set this to true
	VerifyBlockOrPageSize bool
	// BlobType of the resource to be validated.
	BlobType string
	// access tier for block blobs
	BlobTier string
	// Number of Blocks or Pages Expected from the blob.
	NumberOfBlocksOrPages uint64
	// todo : numberofblockorpages can be an array with offset : end url.
	//todo consecutive page ranges get squashed.
	// PreserveLastModifiedTime represents the azcopy PreserveLastModifiedTime flag while downloading the blob.
	PreserveLastModifiedTime bool

	// Property of the blob to be validated.
	ContentType        string
	ContentEncoding    string
	ContentDisposition string
	ContentLanguage    string
	CacheControl       string
	CheckContentMD5    bool
	CheckContentType   bool
}

// initializes the testblob command, its aliases and description.
// also adds the possible flags that can be supplied with testBlob command.
func init() {
	cmdInput := TestBlobCommand{}
	testBlobCmd := &cobra.Command{
		Use:     "testBlob",
		Aliases: []string{"tBlob"},
		Short:   "tests the blob created using AZCopy v2",

		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 2 {
				return fmt.Errorf("invalid arguments for test blob command")
			}
			// first argument is the resource name.
			cmdInput.Object = args[0]

			// second argument is the test directory.
			cmdInput.Subject = args[1]

			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			verifyBlob(cmdInput)
		},
	}
	rootCmd.AddCommand(testBlobCmd)
	// add flags.
	testBlobCmd.PersistentFlags().StringVar(&cmdInput.MetaData, "metadata", "", "metadata expected from the blob in the container")
	testBlobCmd.PersistentFlags().StringVar(&cmdInput.ContentType, "content-type", "", "content type expected from the blob in the container")
	testBlobCmd.PersistentFlags().StringVar(&cmdInput.ContentEncoding, "content-encoding", "", "Validate content encoding.")
	testBlobCmd.PersistentFlags().StringVar(&cmdInput.ContentDisposition, "content-disposition", "", "Validate content disposition.")
	testBlobCmd.PersistentFlags().StringVar(&cmdInput.ContentLanguage, "content-language", "", "Validate content language.")
	testBlobCmd.PersistentFlags().StringVar(&cmdInput.CacheControl, "cache-control", "", "Validate cache control.")
	testBlobCmd.PersistentFlags().BoolVar(&cmdInput.CheckContentMD5, "check-content-md5", false, "Validate content MD5.")
	testBlobCmd.PersistentFlags().BoolVar(&cmdInput.IsObjectDirectory, "is-object-dir", false, "set the type of object to verify against the subject")
	testBlobCmd.PersistentFlags().Uint64Var(&cmdInput.NumberOfBlocksOrPages, "number-blocks-or-pages", 0, "Use this block size to verify the number of blocks uploaded")
	testBlobCmd.PersistentFlags().BoolVar(&cmdInput.VerifyBlockOrPageSize, "verify-block-size", false, "this flag verify the block size by determining the number of blocks")
	testBlobCmd.PersistentFlags().BoolVar(&cmdInput.NoGuessMimeType, "no-guess-mime-type", false, "This sets the content-type based on the extension of the file.")
	testBlobCmd.PersistentFlags().StringVar(&cmdInput.BlobType, "blob-type", "BlockBlob", "Upload to Azure Storage using this blob type.")
	testBlobCmd.PersistentFlags().StringVar(&cmdInput.BlobTier, "blob-tier", string(azblob.AccessTierNone), "access tier type for the block blob")
	testBlobCmd.PersistentFlags().BoolVar(&cmdInput.PreserveLastModifiedTime, "preserve-last-modified-time", false, "Only available when destination is file system.")
	testBlobCmd.PersistentFlags().BoolVar(&cmdInput.CheckContentType, "check-content-type", false, "Validate content type.")
}

func verifyBlobType(url url.URL, ctx context.Context, p pipeline.Pipeline, intendedBlobType string) (bool, error) {
	bURL := azblob.NewBlobURL(url, p)
	pResp, err := bURL.GetProperties(ctx, azblob.BlobAccessConditions{})

	if err != nil {
		return false, err
	}

	if string(pResp.BlobType()) != intendedBlobType {
		return false, fmt.Errorf("blob URL is not intended blob type %s, but instead %s", intendedBlobType, pResp.BlobType())
	}

	return true, nil
}

// verify the blob downloaded or uploaded.
func verifyBlob(testBlobCmd TestBlobCommand) {
	if testBlobCmd.BlobType == "PageBlob" {
		verifySinglePageBlobUpload(testBlobCmd)
	} else if testBlobCmd.BlobType == "AppendBlob" {
		verifySingleAppendBlob(testBlobCmd)
	} else {
		if testBlobCmd.IsObjectDirectory {
			verifyBlockBlobDirUpload(testBlobCmd)
		} else {
			verifySingleBlockBlob(testBlobCmd)
		}
	}
}

// verifyBlockBlobDirUpload verifies the directory recursively uploaded to the container.
func verifyBlockBlobDirUpload(testBlobCmd TestBlobCommand) {
	// parse the subject url.
	sasUrl, err := url.Parse(testBlobCmd.Subject)
	if err != nil {
		fmt.Println("error parsing the container sas ", testBlobCmd.Subject)
		os.Exit(1)
	}

	containerName := strings.SplitAfterN(sasUrl.Path[1:], "/", 2)[0]
	sasUrl.Path = "/" + containerName

	// Create Pipeline to Get the Blob Properties or List Blob Segment
	p := ste.NewBlobPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{
		Telemetry: azblob.TelemetryOptions{
			Value: common.UserAgent,
		},
	},
		ste.XferRetryOptions{
			Policy:        0,
			MaxTries:      ste.UploadMaxTries,
			TryTimeout:    10 * time.Minute,
			RetryDelay:    ste.UploadRetryDelay,
			MaxRetryDelay: ste.UploadMaxRetryDelay},
		nil,
		ste.NewAzcopyHTTPClient(0),
		nil)
	containerUrl := azblob.NewContainerURL(*sasUrl, p)

	testCtx := context.WithValue(context.Background(), ste.ServiceAPIVersionOverride, defaultServiceApiVersion)
	// perform a list blob with search prefix "dirname/"
	dirName := strings.Split(testBlobCmd.Object, "/")
	searchPrefix := dirName[len(dirName)-1] + "/"
	for marker := (azblob.Marker{}); marker.NotDone(); {
		// look for all blobs that start with the prefix, so that if a blob is under the virtual directory, it will show up
		listBlob, err := containerUrl.ListBlobsFlatSegment(testCtx, marker, azblob.ListBlobsSegmentOptions{Prefix: searchPrefix})
		if err != nil {
			fmt.Println("error listing blobs inside the container. Please check the container sas")
			os.Exit(1)
		}

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Segment.BlobItems {
			// get the blob
			size := blobInfo.Properties.ContentLength
			get, err := containerUrl.NewBlobURL(blobInfo.Name).Download(testCtx,
				0, *size, azblob.BlobAccessConditions{}, false)

			if err != nil {
				fmt.Println(fmt.Sprintf("error downloading the blob %s", blobInfo.Name))
				os.Exit(1)
			}

			// read all bytes.
			blobBytesDownloaded, err := ioutil.ReadAll(get.Body(azblob.RetryReaderOptions{}))
			if err != nil {
				fmt.Println(fmt.Sprintf("error reading the body of blob %s downloaded and failed with error %s", blobInfo.Name, err.Error()))
				os.Exit(1)
			}
			// remove the search prefix from the blob name
			blobName := strings.Replace(blobInfo.Name, searchPrefix, "", 1)
			// blob path on local disk.
			objectLocalPath := testBlobCmd.Object + string(os.PathSeparator) + blobName
			// opening the file locally and memory mapping it.
			sFileInfo, err := os.Stat(objectLocalPath)
			if err != nil {
				fmt.Println("error geting the subject blob file info on local disk ")
				os.Exit(1)
			}

			sFile, err := os.Open(objectLocalPath)
			if err != nil {
				fmt.Println("error opening file ", sFile)
				os.Exit(1)
			}
			sMap, err := NewMMF(sFile, false, 0, int64(sFileInfo.Size()))
			if err != nil {
				fmt.Println("error memory mapping the file ", sFileInfo.Name())
			}

			// calculating the md5 of blob on container.
			actualMd5 := md5.Sum(blobBytesDownloaded)
			// calculating md5 of resource locally.
			expectedMd5 := md5.Sum(sMap)

			if actualMd5 != expectedMd5 {
				fmt.Println("the upload blob md5 is not equal to the md5 of actual blob on disk for blob ", blobInfo.Name)
				os.Exit(1)
			}
		}
		marker = listBlob.NextMarker
	}

}

// validateMetadata compares the meta data provided while
// uploading and metadata with blob in the container.
func validateMetadata(expectedMetaDataString string, actualMetaData azblob.Metadata) bool {
	if len(expectedMetaDataString) > 0 {
		// split the meta data string to get the map of key value pair
		// metadata string is in format key1=value1;key2=value2;key3=value3
		expectedMetaData := azblob.Metadata{}
		// split the metadata to get individual keyvalue pair in format key1=value1
		keyValuePair := strings.Split(expectedMetaDataString, ";")
		for index := 0; index < len(keyValuePair); index++ {
			// split the individual key value pair to get key and value
			keyValue := strings.Split(keyValuePair[index], "=")
			expectedMetaData[keyValue[0]] = keyValue[1]
		}
		// if number of metadata provided while uploading
		// doesn't match the metadata with blob on the container
		if len(expectedMetaData) != len(actualMetaData) {
			fmt.Println("number of user given key value pair of the actual metadata differs from key value pair of expected metaData")
			return false
		}
		// iterating through each key value pair of actual metaData and comparing the key value pair in expected metadata
		for key, value := range actualMetaData {
			if expectedMetaData[key] != value {
				fmt.Println(fmt.Sprintf("value of user given key %s is %s in actual data while it is %s in expected metadata", key, value, expectedMetaData[key]))
				return false
			}
		}
	} else {
		if len(actualMetaData) > 0 {
			return false
		}
	}
	return true
}

// verifySinglePageBlobUpload verifies the pageblob uploaded or downloaded
// against the blob locally.
func verifySinglePageBlobUpload(testBlobCmd TestBlobCommand) {

	fileInfo, err := os.Stat(testBlobCmd.Object)
	if err != nil {
		fmt.Println("error opening the destination blob on local disk ")
		os.Exit(1)
	}
	file, err := os.Open(testBlobCmd.Object)
	if err != nil {
		fmt.Println("error opening the file ", testBlobCmd.Object)
	}

	// getting the shared access signature of the resource.
	sourceURL, err := url.Parse(testBlobCmd.Subject)
	if err != nil {
		fmt.Println("Error parsing the blob url source")
		os.Exit(1)
	}

	// creating the page blob url of the resource on container.
	// Create Pipeline to Get the Blob Properties or List Blob Segment
	p := ste.NewBlobPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{
		Telemetry: azblob.TelemetryOptions{
			Value: common.UserAgent,
		},
	},
		ste.XferRetryOptions{
			Policy:        0,
			MaxTries:      ste.UploadMaxTries,
			TryTimeout:    10 * time.Minute,
			RetryDelay:    ste.UploadRetryDelay,
			MaxRetryDelay: ste.UploadMaxRetryDelay},
		nil,
		ste.NewAzcopyHTTPClient(0),
		nil)

	testCtx := context.WithValue(context.Background(), ste.ServiceAPIVersionOverride, defaultServiceApiVersion)

	isPage, err := verifyBlobType(*sourceURL, testCtx, p, "PageBlob")

	if !isPage || err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	pageBlobUrl := azblob.NewPageBlobURL(*sourceURL, p)

	// get the blob properties and check the blob tier.
	if azblob.AccessTierType(testBlobCmd.BlobTier) != azblob.AccessTierNone {
		blobProperties, err := pageBlobUrl.GetProperties(testCtx, azblob.BlobAccessConditions{})
		if err != nil {
			fmt.Println(fmt.Sprintf("error getting the properties of the blob. failed with error %s", err.Error()))
			os.Exit(1)
		}
		// If the blob tier does not match the expected blob tier.
		if !strings.EqualFold(blobProperties.AccessTier(), testBlobCmd.BlobTier) {
			fmt.Println(fmt.Sprintf("Access blob tier type %s does not match the expected %s tier type", blobProperties.AccessTier(), testBlobCmd.BlobTier))
			os.Exit(1)
		}
		// Closing the blobProperties response body.
		if blobProperties.Response() != nil {
			io.Copy(ioutil.Discard, blobProperties.Response().Body)
			blobProperties.Response().Body.Close()
		}
	}

	get, err := pageBlobUrl.Download(testCtx, 0, fileInfo.Size(), azblob.BlobAccessConditions{}, false)
	if err != nil {
		fmt.Println("unable to get blob properties ", err.Error())
		os.Exit(1)
	}
	// reading all the bytes downloaded.
	blobBytesDownloaded, err := ioutil.ReadAll(get.Body(azblob.RetryReaderOptions{}))
	if get.Response().Body != nil {
		get.Response().Body.Close()
	}
	if err != nil {
		fmt.Println("error reading the byes from response and failed with error ", err.Error())
		os.Exit(1)
	}

	expectedContentType := ""

	if testBlobCmd.NoGuessMimeType {
		expectedContentType = testBlobCmd.ContentType
	}

	if len(blobBytesDownloaded) != 0 {
		// memory mapping the resource on local path.
		mmap, err := NewMMF(file, false, 0, fileInfo.Size())
		if err != nil {
			fmt.Println("error mapping the destination blob file ", err.Error())
			os.Exit(1)
		}

		// calculating and verify the md5 of the resource
		// both locally and on the container.
		actualMd5 := md5.Sum(mmap)
		expectedMd5 := md5.Sum(blobBytesDownloaded)
		if actualMd5 != expectedMd5 {
			fmt.Println("the uploaded blob's md5 doesn't matches the actual blob's md5 for blob ", testBlobCmd.Object)
			os.Exit(1)
		}

		if !testBlobCmd.NoGuessMimeType {
			expectedContentType = http.DetectContentType(mmap)
		}

		mmap.Unmap()
	}

	// verify the content-type
	if testBlobCmd.CheckContentType && !validateString(expectedContentType, get.ContentType()) {
		fmt.Printf(
			"mismatch content type between actual and user given blob content type, expected %q, actually %q\n",
			expectedContentType,
			get.ContentType())
		os.Exit(1)
	}

	// verify the user given metadata supplied while uploading the blob against the metadata actually present in the blob
	if !validateMetadata(testBlobCmd.MetaData, get.NewMetadata()) {
		fmt.Println("meta data does not match between the actual and uploaded blob.")
		os.Exit(1)
	}

	//verify the content-encoding
	if !validateString(testBlobCmd.ContentEncoding, get.ContentEncoding()) {
		fmt.Println("mismatch ContentEncoding between actual and user given blob")
		os.Exit(1)
	}

	if !validateString(testBlobCmd.CacheControl, get.CacheControl()) {
		fmt.Println("mismatch CacheControl between actual and user given blob")
		os.Exit(1)
	}

	if !validateString(testBlobCmd.ContentDisposition, get.ContentDisposition()) {
		fmt.Println("mismatch ContentDisposition between actual and user given blob")
		os.Exit(1)
	}

	if !validateString(testBlobCmd.ContentLanguage, get.ContentLanguage()) {
		fmt.Println("mismatch ContentLanguage between actual and user given blob")
		os.Exit(1)
	}

	if testBlobCmd.CheckContentMD5 && (get.ContentMD5() == nil || len(get.ContentMD5()) == 0) {
		fmt.Println("ContentMD5 should not be empty")
		os.Exit(1)
	}
	file.Close()

	// verify the number of pageranges.
	// this verifies the page-size and azcopy pageblob implementation.
	if testBlobCmd.VerifyBlockOrPageSize {
		numberOfPages := int(testBlobCmd.NumberOfBlocksOrPages)
		resp, err := pageBlobUrl.GetPageRanges(testCtx, 0, 0, azblob.BlobAccessConditions{})
		if err != nil {
			fmt.Println("error getting the block blob list ", err.Error())
			os.Exit(1)
		}
		if numberOfPages != (len(resp.PageRange)) {
			fmt.Println("number of blocks to be uploaded is different from the number of expected to be uploaded")
			os.Exit(1)
		}
	}
}

// verifySingleBlockBlob verifies the blockblob uploaded or downloaded
// against the blob locally.

// todo close the file as soon as possible.
func verifySingleBlockBlob(testBlobCmd TestBlobCommand) {
	// opening the resource on local path in test directory.
	objectLocalPath := testBlobCmd.Object
	fileInfo, err := os.Stat(objectLocalPath)
	if err != nil {
		fmt.Println("error opening the destination blob on local disk ")
		os.Exit(1)
	}
	file, err := os.Open(objectLocalPath)
	if err != nil {
		fmt.Println("error opening the file ", objectLocalPath)
	}

	// getting the shared access signature of the resource.
	sourceSas := testBlobCmd.Subject
	sourceURL, err := url.Parse(sourceSas)
	if err != nil {
		fmt.Println(fmt.Sprintf("Error parsing the blob url source %s", testBlobCmd.Object))
		os.Exit(1)
	}

	// creating the blockblob url of the resource on container.
	// Create Pipeline to Get the Blob Properties or List Blob Segment
	p := ste.NewBlobPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{
		Telemetry: azblob.TelemetryOptions{
			Value: common.UserAgent,
		},
	},
		ste.XferRetryOptions{
			Policy:        0,
			MaxTries:      ste.UploadMaxTries,
			TryTimeout:    10 * time.Minute,
			RetryDelay:    ste.UploadRetryDelay,
			MaxRetryDelay: ste.UploadMaxRetryDelay},
		nil,
		ste.NewAzcopyHTTPClient(0),
		nil)

	testCtx := context.WithValue(context.Background(), ste.ServiceAPIVersionOverride, defaultServiceApiVersion)

	isBlock, err := verifyBlobType(*sourceURL, testCtx, p, "BlockBlob")

	if !isBlock || err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	blobUrl := azblob.NewBlobURL(*sourceURL, p)

	// check for access tier type
	// get the blob properties and get the Access Tier Type.
	if azblob.AccessTierType(testBlobCmd.BlobTier) != azblob.AccessTierNone {
		blobProperties, err := blobUrl.GetProperties(testCtx, azblob.BlobAccessConditions{})
		if err != nil {
			fmt.Println(fmt.Sprintf("error getting the blob properties. Failed with error %s", err.Error()))
			os.Exit(1)
		}
		// Match the Access Tier Type with Expected Tier Type.
		if !strings.EqualFold(blobProperties.AccessTier(), testBlobCmd.BlobTier) {
			fmt.Println(fmt.Sprintf("block blob access tier %s does not matches the expected tier %s", blobProperties.AccessTier(), testBlobCmd.BlobTier))
			os.Exit(1)
		}
		// Closing the blobProperties response.
		if blobProperties.Response() != nil {
			io.Copy(ioutil.Discard, blobProperties.Response().Body)
			blobProperties.Response().Body.Close()
		}
		// If the access tier type of blob is set to Archive, then the blob is offline and reading the blob is not allowed,
		// so exit the test.
		if azblob.AccessTierType(testBlobCmd.BlobTier) == azblob.AccessTierArchive {
			os.Exit(0)
		}
	}

	get, err := blobUrl.Download(testCtx, 0, fileInfo.Size(), azblob.BlobAccessConditions{}, false)
	if err != nil {
		fmt.Println("unable to get blob properties ", err.Error())
		os.Exit(1)
	}
	// reading all the blob bytes.
	blobBytesDownloaded, err := ioutil.ReadAll(get.Body(azblob.RetryReaderOptions{}))
	if get.Response().Body != nil {
		get.Response().Body.Close()
	}
	if err != nil {
		fmt.Println("error reading the byes from response and failed with error ", err.Error())
		os.Exit(1)
	}
	if fileInfo.Size() == 0 {
		// If the fileSize is 0 and the len of downloaded bytes is not 0
		// validation fails
		if len(blobBytesDownloaded) != 0 {
			fmt.Println(fmt.Sprintf("validation failed since the actual file size %d differs from the downloaded file size %d", fileInfo.Size(), len(blobBytesDownloaded)))
			os.Exit(1)
		}
		// If both the actual and downloaded file size is 0,
		// validation is successful, no need to match the md5
		os.Exit(0)
	}
	// memory mapping the resource on local path.
	mmap, err := NewMMF(file, false, 0, fileInfo.Size())
	if err != nil {
		fmt.Println("error mapping the destination blob file ", err.Error())
		os.Exit(1)
	}

	// calculating and verify the md5 of the resource
	// both locally and on the container.
	actualMd5 := md5.Sum(mmap)
	expectedMd5 := md5.Sum(blobBytesDownloaded)

	if actualMd5 != expectedMd5 {
		fmt.Println("the uploaded blob's md5 doesn't matches the actual blob's md5")
		os.Exit(1)
	}
	// verify the user given metadata supplied while uploading the blob against the metadata actually present in the blob
	if !validateMetadata(testBlobCmd.MetaData, get.NewMetadata()) {
		fmt.Println("meta data does not match between the actual and uploaded blob.")
		os.Exit(1)
	}

	// verify the content-type
	expectedContentType := ""
	if testBlobCmd.NoGuessMimeType {
		expectedContentType = testBlobCmd.ContentType
	} else {
		expectedContentType = http.DetectContentType(mmap)
	}
	if testBlobCmd.CheckContentType && !validateString(expectedContentType, get.ContentType()) {
		fmt.Printf(
			"mismatch content type between actual and user given blob content type, expected %q, actually %q\n",
			expectedContentType,
			get.ContentType())
		os.Exit(1)
	}

	//verify the content-encoding
	if !validateString(testBlobCmd.ContentEncoding, get.ContentEncoding()) {
		fmt.Println("mismatch content encoding between actual and user given blob content encoding")
		os.Exit(1)
	}

	if testBlobCmd.PreserveLastModifiedTime {
		if fileInfo.ModTime().Unix() != get.LastModified().Unix() {
			fmt.Println("modified time of downloaded and actual blob does not match")
			os.Exit(1)
		}
	}

	// unmap and closing the memory map file.
	mmap.Unmap()
	err = file.Close()
	if err != nil {
		fmt.Println(fmt.Sprintf("error closing the file %s and failed with error %s. Error could be while validating the blob.", file.Name(), err.Error()))
		os.Exit(1)
	}

	// verify the block size
	if testBlobCmd.VerifyBlockOrPageSize {
		blockBlobUrl := azblob.NewBlockBlobURL(*sourceURL, p)
		numberOfBlocks := int(testBlobCmd.NumberOfBlocksOrPages)
		resp, err := blockBlobUrl.GetBlockList(testCtx, azblob.BlockListNone, azblob.LeaseAccessConditions{})
		if err != nil {
			fmt.Println("error getting the block blob list")
			os.Exit(1)
		}
		// todo only commited blocks
		if numberOfBlocks != (len(resp.CommittedBlocks)) {
			fmt.Println("number of blocks to be uploaded is different from the number of expected to be uploaded")
			os.Exit(1)
		}
	}
}

func verifySingleAppendBlob(testBlobCmd TestBlobCommand) {

	fileInfo, err := os.Stat(testBlobCmd.Object)
	if err != nil {
		fmt.Println("error opening the destination blob on local disk ")
		os.Exit(1)
	}
	file, err := os.Open(testBlobCmd.Object)
	if err != nil {
		fmt.Println("error opening the file ", testBlobCmd.Object)
	}

	// getting the shared access signature of the resource.
	sourceURL, err := url.Parse(testBlobCmd.Subject)
	if err != nil {
		fmt.Println(fmt.Sprintf("Error parsing the blob url source %s", testBlobCmd.Object))
		os.Exit(1)
	}

	p := ste.NewBlobPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{
		Telemetry: azblob.TelemetryOptions{
			Value: common.UserAgent,
		},
	},
		ste.XferRetryOptions{
			Policy:        0,
			MaxTries:      ste.UploadMaxTries,
			TryTimeout:    10 * time.Minute,
			RetryDelay:    ste.UploadRetryDelay,
			MaxRetryDelay: ste.UploadMaxRetryDelay},
		nil,
		ste.NewAzcopyHTTPClient(0),
		nil)

	testCtx := context.WithValue(context.Background(), ste.ServiceAPIVersionOverride, defaultServiceApiVersion)

	isAppend, err := verifyBlobType(*sourceURL, testCtx, p, "AppendBlob")

	if !isAppend || err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	appendBlobURL := azblob.NewAppendBlobURL(*sourceURL, p)

	// get the blob properties and check the blob tier.
	if azblob.AccessTierType(testBlobCmd.BlobTier) != azblob.AccessTierNone {
		blobProperties, err := appendBlobURL.GetProperties(testCtx, azblob.BlobAccessConditions{})
		if err != nil {
			fmt.Println(fmt.Sprintf("error getting the properties of the blob. failed with error %s", err.Error()))
			os.Exit(1)
		}
		// If the blob tier does not match the expected blob tier.
		if !strings.EqualFold(blobProperties.AccessTier(), testBlobCmd.BlobTier) {
			fmt.Println(fmt.Sprintf("Access blob tier type %s does not match the expected %s tier type", blobProperties.AccessTier(), testBlobCmd.BlobTier))
			os.Exit(1)
		}
		// Closing the blobProperties response body.
		if blobProperties.Response() != nil {
			io.Copy(ioutil.Discard, blobProperties.Response().Body)
			blobProperties.Response().Body.Close()
		}
	}

	get, err := appendBlobURL.Download(testCtx, 0, fileInfo.Size(), azblob.BlobAccessConditions{}, false)
	if err != nil {
		fmt.Println("unable to get blob properties ", err.Error())
		os.Exit(1)
	}
	// reading all the bytes downloaded.
	blobBytesDownloaded, err := ioutil.ReadAll(get.Body(azblob.RetryReaderOptions{}))
	if get.Response().Body != nil {
		get.Response().Body.Close()
	}
	if err != nil {
		fmt.Println("error reading the byes from response and failed with error ", err.Error())
		os.Exit(1)
	}

	// verify the content-type
	expectedContentType := ""

	if testBlobCmd.NoGuessMimeType {
		expectedContentType = testBlobCmd.ContentType
	}

	if len(blobBytesDownloaded) != 0 {
		// memory mapping the resource on local path.
		mmap, err := NewMMF(file, false, 0, fileInfo.Size())
		if err != nil {
			fmt.Println("error mapping the destination blob file ", err.Error())
			os.Exit(1)
		}

		// calculating and verify the md5 of the resource
		// both locally and on the container.
		actualMd5 := md5.Sum(mmap)
		expectedMd5 := md5.Sum(blobBytesDownloaded)
		if actualMd5 != expectedMd5 {
			fmt.Println("the uploaded blob's md5 doesn't matches the actual blob's md5 for blob ", testBlobCmd.Object)
			os.Exit(1)
		}

		if !testBlobCmd.NoGuessMimeType {
			expectedContentType = http.DetectContentType(mmap)
		}

		mmap.Unmap()
	}

	// verify the user given metadata supplied while uploading the blob against the metadata actually present in the blob
	if !validateMetadata(testBlobCmd.MetaData, get.NewMetadata()) {
		fmt.Println("meta data does not match between the actual and uploaded blob.")
		os.Exit(1)
	}

	if testBlobCmd.CheckContentType && !validateString(expectedContentType, get.ContentType()) {
		fmt.Printf(
			"mismatch content type between actual and user given blob content type, expected %q, actually %q\n",
			expectedContentType,
			get.ContentType())
		os.Exit(1)
	}

	//verify the content-encoding
	if !validateString(testBlobCmd.ContentEncoding, get.ContentEncoding()) {
		fmt.Println("mismatch ContentEncoding between actual and user given blob")
		os.Exit(1)
	}

	if !validateString(testBlobCmd.CacheControl, get.CacheControl()) {
		fmt.Println("mismatch CacheControl between actual and user given blob")
		os.Exit(1)
	}

	if !validateString(testBlobCmd.ContentDisposition, get.ContentDisposition()) {
		fmt.Println("mismatch ContentDisposition between actual and user given blob")
		os.Exit(1)
	}

	if !validateString(testBlobCmd.ContentLanguage, get.ContentLanguage()) {
		fmt.Println("mismatch ContentLanguage between actual and user given blob")
		os.Exit(1)
	}

	if testBlobCmd.CheckContentMD5 && (get.ContentMD5() == nil || len(get.ContentMD5()) == 0) {
		fmt.Println("ContentMD5 should not be empty")
		os.Exit(1)
	}

	file.Close()
}
