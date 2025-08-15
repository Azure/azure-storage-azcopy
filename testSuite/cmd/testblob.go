package cmd

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/appendblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/pageblob"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"

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
	Metadata string
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
	testBlobCmd.PersistentFlags().StringVar(&cmdInput.Metadata, "metadata", "", "metadata expected from the blob in the container")
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
	testBlobCmd.PersistentFlags().StringVar(&cmdInput.BlobTier, "blob-tier", "", "access tier type for the block blob")
	testBlobCmd.PersistentFlags().BoolVar(&cmdInput.PreserveLastModifiedTime, "preserve-last-modified-time", false, "Only available when destination is file system.")
	testBlobCmd.PersistentFlags().BoolVar(&cmdInput.CheckContentType, "check-content-type", false, "Validate content type.")
}

func verifyBlobType(resourceURL string, ctx context.Context, intendedBlobType string) (bool, error) {
	blobClient, err := blob.NewClientWithNoCredential(resourceURL, nil)
	if err != nil {
		return false, err
	}
	pResp, err := blobClient.GetProperties(ctx, nil)

	if err != nil {
		return false, err
	}

	if string(common.IffNotNil(pResp.BlobType, "")) != intendedBlobType {
		return false, fmt.Errorf("blob URL is not intended blob type %s, but instead %s", intendedBlobType, common.IffNotNil(pResp.BlobType, ""))
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
	containerClient, err := container.NewClientWithNoCredential(sasUrl.String(), &container.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Telemetry: policy.TelemetryOptions{ApplicationID: common.UserAgent},
			Retry: policy.RetryOptions{
				MaxRetries:    ste.UploadMaxTries,
				TryTimeout:    10 * time.Minute,
				RetryDelay:    ste.UploadRetryDelay,
				MaxRetryDelay: ste.UploadMaxRetryDelay,
			},
			Transport: ste.NewAzcopyHTTPClient(0),
		}})
	if err != nil {
		fmt.Printf("error creating container client. failed with error %s\n", err.Error())
		os.Exit(1)
	}

	testCtx := context.WithValue(context.Background(), ste.ServiceAPIVersionOverride, defaultServiceApiVersion)
	// perform a list blob with search prefix "dirname/"
	dirName := strings.Split(testBlobCmd.Object, "/")
	searchPrefix := dirName[len(dirName)-1] + "/"
	pager := containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{Prefix: &searchPrefix})
	for pager.More() {
		// look for all blobs that start with the prefix, so that if a blob is under the virtual directory, it will show up
		listBlob, err := pager.NextPage(testCtx)
		if err != nil {
			fmt.Println("error listing blobs inside the container. Please check the container sas")
			os.Exit(1)
		}

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Segment.BlobItems {
			// get the blob
			get, err := containerClient.NewBlobClient(*blobInfo.Name).DownloadStream(testCtx, nil)

			if err != nil {
				fmt.Printf("error downloading the blob %s\n", *blobInfo.Name)
				os.Exit(1)
			}

			// read all bytes.
			blobBytesDownloaded, err := io.ReadAll(get.Body)
			if err != nil {
				fmt.Printf("error reading the body of blob %s downloaded and failed with error %s\n", *blobInfo.Name, err.Error())
				os.Exit(1)
			}
			// remove the search prefix from the blob name
			blobName := strings.Replace(*blobInfo.Name, searchPrefix, "", 1)
			// blob path on local disk.
			objectLocalPath := testBlobCmd.Object + string(os.PathSeparator) + blobName
			// opening the file locally and memory mapping it.
			sFileInfo, err := os.Stat(objectLocalPath)
			if err != nil {
				fmt.Println("error getting the subject blob file info on local disk ")
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
	}

}

// validateMetadata compares the metadata provided while
// uploading and metadata with blob in the container.
func validateMetadata(expectedMetadataString string, actualMetadata map[string]*string) bool {
	if len(expectedMetadataString) > 0 {
		// split the metadata string to get the map of key value pair
		// metadata string is in format key1=value1;key2=value2;key3=value3
		expectedMetadata := map[string]*string{}
		// split the metadata to get individual keyvalue pair in format key1=value1
		keyValuePair := strings.Split(expectedMetadataString, ";")
		for index := 0; index < len(keyValuePair); index++ {
			// split the individual key value pair to get key and value
			keyValue := strings.Split(keyValuePair[index], "=")
			expectedMetadata[keyValue[0]] = to.Ptr(keyValue[1])
		}
		// if number of metadata provided while uploading
		// doesn't match the metadata with blob on the container
		if len(expectedMetadata) != len(actualMetadata) {
			fmt.Println("number of user given key value pair of the actual metadata differs from key value pair of expected metaData")
			return false
		}
		// iterating through each key value pair of actual metaData and comparing the key value pair in expected metadata
		for key, value := range actualMetadata {
			if *expectedMetadata[strings.ToLower(key)] != *value {
				fmt.Printf("value of user given key %s is %s in actual data while it is %s in expected metadata\n", key, *value, *expectedMetadata[key])
				return false
			}
		}
	} else {
		if len(actualMetadata) > 0 {
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

	testCtx := context.WithValue(context.Background(), ste.ServiceAPIVersionOverride, defaultServiceApiVersion)

	isPage, err := verifyBlobType(testBlobCmd.Subject, testCtx, "PageBlob")

	if !isPage || err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	pageBlobClient, err := pageblob.NewClientWithNoCredential(testBlobCmd.Subject, &pageblob.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Telemetry: policy.TelemetryOptions{ApplicationID: common.UserAgent},
			Retry: policy.RetryOptions{
				MaxRetries:    ste.UploadMaxTries,
				TryTimeout:    10 * time.Minute,
				RetryDelay:    ste.UploadRetryDelay,
				MaxRetryDelay: ste.UploadMaxRetryDelay,
			},
			Transport: ste.NewAzcopyHTTPClient(0),
		}})
	if err != nil {
		fmt.Printf("error creating page blob client. failed with error %s\n", err.Error())
		os.Exit(1)
	}

	// get the blob properties and check the blob tier.
	if testBlobCmd.BlobTier != "" {
		blobProperties, err := pageBlobClient.GetProperties(testCtx, nil)
		if err != nil {
			fmt.Printf("error getting the properties of the blob. failed with error %s\n", err.Error())
			os.Exit(1)
		}
		// If the blob tier does not match the expected blob tier.
		if !strings.EqualFold(common.IffNotNil(blobProperties.AccessTier, ""), testBlobCmd.BlobTier) {
			fmt.Printf("Access blob tier type %s does not match the expected %s tier type\n", common.IffNotNil(blobProperties.AccessTier, ""), testBlobCmd.BlobTier)
			os.Exit(1)
		}
	}

	get, err := pageBlobClient.DownloadStream(testCtx, nil)
	if err != nil {
		fmt.Println("unable to get blob properties ", err.Error())
		os.Exit(1)
	}
	// reading all the bytes downloaded.
	blobBytesDownloaded, err := io.ReadAll(get.Body)
	if get.Body != nil {
		get.Body.Close()
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
			expectedContentType = strings.Split(http.DetectContentType(mmap), ";")[0]
		}

		mmap.Unmap()
	}

	// verify the content-type
	if testBlobCmd.CheckContentType && !validateString(expectedContentType, common.IffNotNil(get.ContentType, "")) {
		fmt.Printf(
			"mismatch content type between actual and user given blob content type, expected %q, actually %q\n",
			expectedContentType,
			common.IffNotNil(get.ContentType, ""))
		os.Exit(1)
	}

	// verify the user given metadata supplied while uploading the blob against the metadata actually present in the blob
	if !validateMetadata(testBlobCmd.Metadata, get.Metadata) {
		fmt.Println("meta data does not match between the actual and uploaded blob.")
		os.Exit(1)
	}

	//verify the content-encoding
	if !validateString(testBlobCmd.ContentEncoding, common.IffNotNil(get.ContentEncoding, "")) {
		fmt.Println("mismatch ContentEncoding between actual and user given blob")
		os.Exit(1)
	}

	if !validateString(testBlobCmd.CacheControl, common.IffNotNil(get.CacheControl, "")) {
		fmt.Println("mismatch CacheControl between actual and user given blob")
		os.Exit(1)
	}

	if !validateString(testBlobCmd.ContentDisposition, common.IffNotNil(get.ContentDisposition, "")) {
		fmt.Println("mismatch ContentDisposition between actual and user given blob")
		os.Exit(1)
	}

	if !validateString(testBlobCmd.ContentLanguage, common.IffNotNil(get.ContentLanguage, "")) {
		fmt.Println("mismatch ContentLanguage between actual and user given blob")
		os.Exit(1)
	}

	if testBlobCmd.CheckContentMD5 && len(get.ContentMD5) == 0 {
		fmt.Println("ContentMD5 should not be empty")
		os.Exit(1)
	}
	file.Close()

	// verify the number of pageranges.
	// this verifies the page-size and azcopy pageblob implementation.
	if testBlobCmd.VerifyBlockOrPageSize {
		numberOfPages := int(testBlobCmd.NumberOfBlocksOrPages)
		pager := pageBlobClient.NewGetPageRangesPager(nil)
		pageRanges := 0
		for pager.More() {
			resp, err := pager.NextPage(testCtx)
			if err != nil {
				fmt.Println("error getting the page blob list ", err.Error())
				os.Exit(1)
			}
			pageRanges += len(resp.PageRange)
		}
		if numberOfPages != (pageRanges) {
			fmt.Printf("number of blocks to be uploaded (%d) is different from the number of expected to be uploaded (%d)\n", pageRanges, numberOfPages)
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

	testCtx := context.WithValue(context.Background(), ste.ServiceAPIVersionOverride, defaultServiceApiVersion)

	isBlock, err := verifyBlobType(testBlobCmd.Subject, testCtx, "BlockBlob")

	if !isBlock || err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	blockBlobClient, err := blockblob.NewClientWithNoCredential(testBlobCmd.Subject, &blockblob.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Telemetry: policy.TelemetryOptions{ApplicationID: common.UserAgent},
			Retry: policy.RetryOptions{
				MaxRetries:    ste.UploadMaxTries,
				TryTimeout:    10 * time.Minute,
				RetryDelay:    ste.UploadRetryDelay,
				MaxRetryDelay: ste.UploadMaxRetryDelay,
			},
			Transport: ste.NewAzcopyHTTPClient(0),
		}})
	if err != nil {
		fmt.Printf("error creating block blob client. failed with error %s\n", err.Error())
		os.Exit(1)
	}

	// check for access tier type
	// get the blob properties and get the Access Tier Type.
	if testBlobCmd.BlobTier != "" {
		blobProperties, err := blockBlobClient.GetProperties(testCtx, nil)
		if err != nil {
			fmt.Printf("error getting the blob properties. Failed with error %s\n", err.Error())
			os.Exit(1)
		}
		// Match the Access Tier Type with Expected Tier Type.
		if !strings.EqualFold(common.IffNotNil(blobProperties.AccessTier, ""), testBlobCmd.BlobTier) {
			fmt.Printf("block blob access tier %s does not matches the expected tier %s\n", common.IffNotNil(blobProperties.AccessTier, ""), testBlobCmd.BlobTier)
			os.Exit(1)
		}
		// If the access tier type of blob is set to Archive, then the blob is offline and reading the blob is not allowed,
		// so exit the test.
		if blob.AccessTier(testBlobCmd.BlobTier) == blob.AccessTierArchive {
			os.Exit(0)
		}
	}

	get, err := blockBlobClient.DownloadStream(testCtx, nil)
	if err != nil {
		fmt.Println("unable to get blob properties ", err.Error())
		os.Exit(1)
	}
	// reading all the blob bytes.
	blobBytesDownloaded, err := io.ReadAll(get.Body)
	if get.Body != nil {
		get.Body.Close()
	}
	if err != nil {
		fmt.Println("error reading the byes from response and failed with error ", err.Error())
		os.Exit(1)
	}
	if fileInfo.Size() == 0 {
		// If the fileSize is 0 and the len of downloaded bytes is not 0
		// validation fails
		if len(blobBytesDownloaded) != 0 {
			fmt.Printf("validation failed since the actual file size %d differs from the downloaded file size %d\n", fileInfo.Size(), len(blobBytesDownloaded))
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
	if !validateMetadata(testBlobCmd.Metadata, get.Metadata) {
		fmt.Println("meta data does not match between the actual and uploaded blob.")
		os.Exit(1)
	}

	// verify the content-type
	expectedContentType := ""
	if testBlobCmd.NoGuessMimeType {
		expectedContentType = testBlobCmd.ContentType
	} else {
		expectedContentType = strings.Split(http.DetectContentType(mmap), ";")[0]
	}
	if testBlobCmd.CheckContentType && !validateString(expectedContentType, common.IffNotNil(get.ContentType, "")) {
		fmt.Printf(
			"mismatch content type between actual and user given blob content type, expected %q, actually %q\n",
			expectedContentType,
			common.IffNotNil(get.ContentType, ""))
		os.Exit(1)
	}

	//verify the content-encoding
	if !validateString(testBlobCmd.ContentEncoding, common.IffNotNil(get.ContentEncoding, "")) {
		fmt.Println("mismatch content encoding between actual and user given blob content encoding")
		os.Exit(1)
	}

	if testBlobCmd.PreserveLastModifiedTime {
		if fileInfo.ModTime().Unix() != (common.IffNotNil(get.LastModified, time.Time{})).Unix() {
			fmt.Println("modified time of downloaded and actual blob does not match")
			os.Exit(1)
		}
	}

	// unmap and closing the memory map file.
	mmap.Unmap()
	err = file.Close()
	if err != nil {
		fmt.Printf("error closing the file %s and failed with error %s. OnError could be while validating the blob.\n", file.Name(), err.Error())
		os.Exit(1)
	}

	// verify the block size
	if testBlobCmd.VerifyBlockOrPageSize {
		numberOfBlocks := int(testBlobCmd.NumberOfBlocksOrPages)
		resp, err := blockBlobClient.GetBlockList(testCtx, blockblob.BlockListTypeCommitted, nil)
		if err != nil {
			fmt.Println("error getting the block blob list")
			os.Exit(1)
		}
		// todo only committed blocks
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

	testCtx := context.WithValue(context.Background(), ste.ServiceAPIVersionOverride, defaultServiceApiVersion)

	isAppend, err := verifyBlobType(testBlobCmd.Subject, testCtx, "AppendBlob")

	if !isAppend || err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	appendBlobClient, err := appendblob.NewClientWithNoCredential(testBlobCmd.Subject, &appendblob.ClientOptions{
		ClientOptions: azcore.ClientOptions{
			Telemetry: policy.TelemetryOptions{ApplicationID: common.UserAgent},
			Retry: policy.RetryOptions{
				MaxRetries:    ste.UploadMaxTries,
				TryTimeout:    10 * time.Minute,
				RetryDelay:    ste.UploadRetryDelay,
				MaxRetryDelay: ste.UploadMaxRetryDelay,
			},
			Transport: ste.NewAzcopyHTTPClient(0),
		}})
	if err != nil {
		fmt.Printf("error creating append blob client. failed with error %s\n", err.Error())
		os.Exit(1)
	}

	// get the blob properties and check the blob tier.
	if testBlobCmd.BlobTier != "" {
		blobProperties, err := appendBlobClient.GetProperties(testCtx, nil)
		if err != nil {
			fmt.Printf("error getting the properties of the blob. failed with error %s\n", err.Error())
			os.Exit(1)
		}
		// If the blob tier does not match the expected blob tier.
		if !strings.EqualFold(common.IffNotNil(blobProperties.AccessTier, ""), testBlobCmd.BlobTier) {
			fmt.Printf("Access blob tier type %s does not match the expected %s tier type\n", common.IffNotNil(blobProperties.AccessTier, ""), testBlobCmd.BlobTier)
			os.Exit(1)
		}
	}

	get, err := appendBlobClient.DownloadStream(testCtx, nil)
	if err != nil {
		fmt.Println("unable to get blob properties ", err.Error())
		os.Exit(1)
	}
	// reading all the bytes downloaded.
	blobBytesDownloaded, err := io.ReadAll(get.Body)
	if get.Body != nil {
		get.Body.Close()
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
			expectedContentType = strings.Split(http.DetectContentType(mmap), ";")[0]
		}

		mmap.Unmap()
	}

	// verify the user given metadata supplied while uploading the blob against the metadata actually present in the blob
	if !validateMetadata(testBlobCmd.Metadata, get.Metadata) {
		fmt.Println("meta data does not match between the actual and uploaded blob.")
		os.Exit(1)
	}

	if testBlobCmd.CheckContentType && !validateString(expectedContentType, common.IffNotNil(get.ContentType, "")) {
		fmt.Printf(
			"mismatch content type between actual and user given blob content type, expected %q, actually %q\n",
			expectedContentType,
			common.IffNotNil(get.ContentType, ""))
		os.Exit(1)
	}

	//verify the content-encoding
	if !validateString(testBlobCmd.ContentEncoding, common.IffNotNil(get.ContentEncoding, "")) {
		fmt.Println("mismatch ContentEncoding between actual and user given blob")
		os.Exit(1)
	}

	if !validateString(testBlobCmd.CacheControl, common.IffNotNil(get.CacheControl, "")) {
		fmt.Println("mismatch CacheControl between actual and user given blob")
		os.Exit(1)
	}

	if !validateString(testBlobCmd.ContentDisposition, common.IffNotNil(get.ContentDisposition, "")) {
		fmt.Println("mismatch ContentDisposition between actual and user given blob")
		os.Exit(1)
	}

	if !validateString(testBlobCmd.ContentLanguage, common.IffNotNil(get.ContentLanguage, "")) {
		fmt.Println("mismatch ContentLanguage between actual and user given blob")
		os.Exit(1)
	}

	if testBlobCmd.CheckContentMD5 && len(get.ContentMD5) == 0 {
		fmt.Println("ContentMD5 should not be empty")
		os.Exit(1)
	}

	file.Close()
}
