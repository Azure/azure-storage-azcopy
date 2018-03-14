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
)

type TestBlobCommand struct{
	Object          string
	Subject         string
	CheckProperties bool
	MetaData        string
	ContentType     string
	ContentEncoding string
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
			if len(args) != 2 {
				return fmt.Errorf("invalid arguments for test blob command")
			}
			cmdInput.Object = args[0]
			cmdInput.Subject = args[1]
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			verifyBlob(cmdInput)
		},
	}
	rootCmd.AddCommand(testBlobCmd)
	testBlobCmd.PersistentFlags().BoolVar(&cmdInput.CheckProperties, "check-properties", false, "use this flag to test the properties of blob uploaded")
	testBlobCmd.PersistentFlags().StringVar(&cmdInput.MetaData, "metadata", "", "metadata expected from the blob in the container")
	testBlobCmd.PersistentFlags().StringVar(&cmdInput.ContentType, "contentType", "", "content type expected from the blob in the container")
}

// Verify Blob gets the
func verifyBlob(testBlobCmd TestBlobCommand){
	sourceURL, err := url.Parse(testBlobCmd.Object)
	if err != nil{
		fmt.Println(fmt.Sprintf("Error parsing the blob url source %s", testBlobCmd.Object))
		os.Exit(1)
	}
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})
	blobUrl := azblob.NewBlobURL(*sourceURL, p)
	blobProperties, err := blobUrl.GetPropertiesAndMetadata(context.Background(), azblob.BlobAccessConditions{})
	if err != nil{
		fmt.Println("unable to get blob properties ", blobProperties)
		os.Exit(1)
	}

	// check whether destination provided is valid or not.
	fileInfo, err := os.Stat(testBlobCmd.Subject)
	if err != nil{
		fmt.Println("error opening the destination blob on local disk ")
		os.Exit(1)
	}
	file, err := os.Open(testBlobCmd.Subject)
	if err != nil{
		fmt.Println("error opening the file ", testBlobCmd.Subject)
	}
	fmt.Println("destination file size ", fileInfo.Size())
	mmap, err := Map(file, false, 0, int(fileInfo.Size()))
	if err != nil{
		fmt.Println("error mapping the destination blob file ", err.Error())
		os.Exit(1)
	}
	actualMd5 := md5.Sum(mmap)
	expectedMd5 := blobProperties.ContentMD5()
	fmt.Println("actual md5 ", (actualMd5))
	if actualMd5 != expectedMd5 {
		fmt.Println("the uploaded blob's md5 doesn't matches the actual blob's md5")
		os.Exit(1)
	}
	if testBlobCmd.CheckProperties {
		// verify the user given metadata supplied while uploading the blob against the metadata actually present in the blob
		if len(testBlobCmd.MetaData) > 0{
			// split the meta data string to get the map of key value pair
			// metadata string is in format key1=value1;key2=value2;key3=value3
			expectedMetaData := azblob.Metadata{}
			// split the metadata to get individual keyvalue pair in format key1=value1
			keyValuePair := strings.Split(testBlobCmd.MetaData, ";")
			for index := 0; index < len(keyValuePair); index++{
				// split the individual key value pair to get key and value
				keyValue := strings.Split(keyValuePair[index], "=")
				expectedMetaData[keyValue[0]] = keyValue[1]
			}
			actualMetaData := blobProperties.NewMetadata()
			if len(expectedMetaData) != len(actualMetaData){
				fmt.Println("number of user given key value pair of the actual metadata differs from key value pair of expected metaData")
				os.Exit(1)
			}
			// iterating through each key value pair of actual metaData and comparing the key value pair in expected metadata
			for key, value := range actualMetaData{
				if expectedMetaData[key] != value {
					fmt.Println(fmt.Sprintf("value of user given key %s is %s in actual data while it is %s in expected metadata", key, value, expectedMetaData[key]))
					os.Exit(1)
				}
			}
		}

		// verify the content-type
		if len(testBlobCmd.ContentType) != len(blobProperties.ContentType()) {

		}
	}
}