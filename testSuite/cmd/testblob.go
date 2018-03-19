package cmd

import (
	"github.com/spf13/cobra"
	"fmt"
	"github.com/Azure/azure-storage-blob-go/2016-05-31/azblob"
	"net/url"
	"os"
	"context"
	"crypto/md5"
	"strings"
	"path"
	"io/ioutil"
)

type TestBlobCommand struct{
	Object          string
	TestDirPath       string
	ContainerUrl      string
	IsObjectDirectory bool
	MetaData        string
	ContentType     string
	ContentEncoding string
	VerifyBlockSize bool
	BlockSize		uint64
}

// initializes the testblob command, its aliases and description.
// also adds the possible flags that can be supplied with testBlob command.
func init(){
	cmdInput := TestBlobCommand{}
	testBlobCmd := &cobra.Command{
		Use:    "testBlob",
		Aliases: []string{"tBlob",},
		Short:   "tests the blob created using AZCopy v2",

		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) <= 2 {
				return fmt.Errorf("invalid arguments for test blob command")
			}
			cmdInput.Object = args[0]
			cmdInput.TestDirPath = args[1]
			cmdInput.ContainerUrl = args[2]
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			verifyBlobUpload(cmdInput)
		},
	}
	rootCmd.AddCommand(testBlobCmd)
	testBlobCmd.PersistentFlags().StringVar(&cmdInput.MetaData, "metadata", "", "metadata expected from the blob in the container")
	testBlobCmd.PersistentFlags().StringVar(&cmdInput.ContentType, "contentType", "", "content type expected from the blob in the container")
	testBlobCmd.PersistentFlags().BoolVar(&cmdInput.IsObjectDirectory, "is-object-dir", false, "set the type of object to verify against the subject")
	testBlobCmd.PersistentFlags().Uint64Var(&cmdInput.BlockSize, "block-size", 100*1024*1024, "Use this block size to verify the number of blocks uploaded")
	testBlobCmd.PersistentFlags().BoolVar(&cmdInput.VerifyBlockSize, "verify-block-size", false, "this flag verify the block size by determining the number of blocks")
}

// Verify Blob gets the
func verifyBlobUpload(testBlobCmd TestBlobCommand){
	// check whether destination provided is valid or not.

	if testBlobCmd.IsObjectDirectory{
		verifyBlobDirUpload(testBlobCmd)
	}else{
		verifySingleBlobUpload(testBlobCmd)
	}

}

func verifyBlobDirUpload(testBlobCmd TestBlobCommand)  {
	sasUrl, err := url.Parse(testBlobCmd.ContainerUrl)

	if err != nil{
		fmt.Println("error parsing the container sas ", testBlobCmd.ContainerUrl)
		os.Exit(1)
	}

	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})
	containerUrl := azblob.NewContainerURL(*sasUrl, p)

	// perform a list blob
	for marker := (azblob.Marker{}); marker.NotDone(); {
		// look for all blobs that start with the prefix, so that if a blob is under the virtual directory, it will show up
		listBlob, err := containerUrl.ListBlobs(context.Background(), marker, azblob.ListBlobsOptions{})
		if err != nil {
			fmt.Println("error listing blobs inside the container. Please check the container sas")
			os.Exit(1)
		}

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Blobs.Blob {
			size := blobInfo.Properties.ContentLength
			get, err := containerUrl.NewBlobURL(blobInfo.Name).GetBlob(context.Background(),
				azblob.BlobRange{0, *size}, azblob.BlobAccessConditions{}, false)

			if err != nil{
				fmt.Println(fmt.Sprintf("error downloading the blob %s", blobInfo.Name))
				os.Exit(1)
			}

			blobBytesDownloaded, err := ioutil.ReadAll(get.Body())
			if err != nil {
				fmt.Println(fmt.Sprintf("error reading the body of blob %s downloaded and failed with error %s", blobInfo.Name, err.Error()))
				os.Exit(1)
			}


			objectLocalPath := testBlobCmd.TestDirPath + "/" + blobInfo.Name

			fmt.Println("subject path ", objectLocalPath)
			sFileInfo, err := os.Stat(objectLocalPath)
			if err != nil{
				fmt.Println("error geting the subject blob file info on local disk ")
				os.Exit(1)
			}

			sFile, err := os.Open(objectLocalPath)
			if err != nil{
				fmt.Println("error opening file ", sFile)
				os.Exit(1)
			}
			sMap, err := Map(sFile, false, 0, int(sFileInfo.Size()))
			if err != nil{
				fmt.Println("error memory mapping the file ", sFileInfo.Name())
			}
			actualMd5 := md5.Sum(blobBytesDownloaded)

			expectedMd5 := md5.Sum(sMap)

			if actualMd5 != expectedMd5 {
				fmt.Println("the upload blob md5 is not equal to the md5 of actual blob on disk")
				os.Exit(1)
			}
		}
		marker = listBlob.NextMarker
	}

}

func getResourceSas(container_sas string, resource string) (string){
	parts := strings.Split(container_sas, "?")
	return parts[0] + "/" + resource + "?" + parts[1]
}


func validateMetadata(expectedMetaDataString string, actualMetaData azblob.Metadata) (bool){
	if len(expectedMetaDataString) > 0{
		// split the meta data string to get the map of key value pair
		// metadata string is in format key1=value1;key2=value2;key3=value3
		expectedMetaData := azblob.Metadata{}
		// split the metadata to get individual keyvalue pair in format key1=value1
		keyValuePair := strings.Split(expectedMetaDataString, ";")
		for index := 0; index < len(keyValuePair); index++{
			// split the individual key value pair to get key and value
			keyValue := strings.Split(keyValuePair[index], "=")
			expectedMetaData[keyValue[0]] = keyValue[1]
		}

		if len(expectedMetaData) != len(actualMetaData){
			fmt.Println("number of user given key value pair of the actual metadata differs from key value pair of expected metaData")
			return false
		}
		// iterating through each key value pair of actual metaData and comparing the key value pair in expected metadata
		for key, value := range actualMetaData{
			if expectedMetaData[key] != value {
				fmt.Println(fmt.Sprintf("value of user given key %s is %s in actual data while it is %s in expected metadata", key, value, expectedMetaData[key]))
				return false
			}
		}
	}else{
		fmt.Println("len of actual meta data ", len(actualMetaData), "  " , actualMetaData)
		if len(actualMetaData) > 0{
			return false
		}
	}
	fmt.Println("return true")
	return true
}

func validateString(expected string, actual string) (bool){
	if len(expected) == 0{
		return true
	}
	if strings.Compare(expected, actual) != 0{
		return false
	}
	return true
}

func verifySingleBlobUpload(testBlobCmd TestBlobCommand){

	objectLocalPath := path.Join(testBlobCmd.TestDirPath, testBlobCmd.Object)
	fileInfo, err := os.Stat(objectLocalPath)
	if err != nil{
		fmt.Println("error opening the destination blob on local disk ")
		os.Exit(1)
	}
	file, err := os.Open(objectLocalPath)
	if err != nil{
		fmt.Println("error opening the file ", objectLocalPath)
	}

	sourceSas := getResourceSas(testBlobCmd.ContainerUrl, testBlobCmd.Object)
	fmt.Println("source sas ", sourceSas)
	sourceURL, err := url.Parse(sourceSas)
	if err != nil{
		fmt.Println(fmt.Sprintf("Error parsing the blob url source %s", testBlobCmd.Object))
		os.Exit(1)
	}
	fmt.Println("source sas ", sourceURL)
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})

	blobUrl := azblob.NewBlobURL(*sourceURL, p)
	get, err := blobUrl.GetBlob(context.Background(), azblob.BlobRange{Offset:0, Count:fileInfo.Size()}, azblob.BlobAccessConditions{}, false)
	if err != nil{
		fmt.Println("unable to get blob properties ", err.Error())
		os.Exit(1)
	}
	blobsize := int(get.ContentLength())

	blobBytesDownloaded, err := ioutil.ReadAll(get.Body())
	if err != nil{
		fmt.Println("error reading the byes from response and failed with error ", err.Error())
	}

	if get.Response().Body != nil{
		get.Response().Body.Close()
	}

	mmap, err := Map(file, false, 0, int(fileInfo.Size()))
	if err != nil{
		fmt.Println("error mapping the destination blob file ", err.Error())
		os.Exit(1)
	}

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
	if !validateString(testBlobCmd.ContentType, get.ContentType()) {
		fmt.Println("mismatch content type between actual and user given blob content type")
		os.Exit(1)
	}

	//verify the content-encoding
	if !validateString(testBlobCmd.ContentEncoding, get.ContentEncoding()) {
		fmt.Println("mismatch content encoding between actual and user given blob content encoding")
		os.Exit(1)
	}
	mmap.Unmap()
	file.Close()

	// verify the block size
	if testBlobCmd.VerifyBlockSize {
		blockBlobUrl := azblob.NewBlockBlobURL(*sourceURL, p)
		numberOfBlocks := int(0)
		if (blobsize % int(testBlobCmd.BlockSize)) == 0{
			numberOfBlocks = blobsize / int(testBlobCmd.BlockSize)
		}else{
			numberOfBlocks = blobsize / int(testBlobCmd.BlockSize) + 1
		}
		resp, err := blockBlobUrl.GetBlockList(context.Background(), azblob.BlockListNone, azblob.LeaseAccessConditions{})
		if err != nil{
			fmt.Println("error getting the block blob list")
			os.Exit(1)
		}
		if numberOfBlocks != (len(resp.CommittedBlocks) + len(resp.UncommittedBlocks)){
			fmt.Println("number of blocks to be uploaded is different from the number of expected to be uploaded")
			os.Exit(1)
		}
	}
}