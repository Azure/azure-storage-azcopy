package cmd

import (
	"context"
	"crypto/md5"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-storage-file-go/azfile"
	"github.com/spf13/cobra"
)

// TestFileCommand represents the struct to get command
// for validating azcopy operations.
type TestFileCommand struct {
	// object is the resource which needs to be validated against a resource in bucket(share/container).
	Object string
	//Subject is the remote resource against which object needs to be validated.
	Subject string
	// IsObjectDirectory defines if the object is a directory or not.
	// If the object is directory, then validation goes through another path.
	IsObjectDirectory bool
	// IsRecursive defines if recursive switch is on during transfer.
	IsRecursive bool
	// Metadata of the file to be validated.
	MetaData string
	// NoGuessMimeType represent the azcopy NoGuessMimeType flag set while uploading the file.
	NoGuessMimeType bool
	// Content Type of the file to be validated.
	ContentType string
	// Content Encoding of the file to be validated.
	ContentEncoding    string
	ContentDisposition string
	ContentLanguage    string
	CacheControl       string
	CheckContentMD5    bool

	// Represents the flag to determine whether number of blocks or pages needs
	// to be verified or not.
	// todo always set this to true
	VerifyBlockOrPageSize bool
	// FileType of the resource to be validated.
	FileType string
	// Number of Blocks or Pages Expected from the file.
	NumberOfBlocksOrPages uint64
	// todo : numberofblockorpages can be an array with offset : end url.
	//todo consecutive page ranges get squashed.
	// PreserveLastModifiedTime represents the azcopy PreserveLastModifiedTime flag while downloading the file.
	PreserveLastModifiedTime bool
}

// initializes the testfile command, its aliases and description.
// also adds the possible flags that can be supplied with testFile command.
func init() {
	cmdInput := TestFileCommand{}
	testFileCmd := &cobra.Command{
		Use:     "testFile",
		Aliases: []string{"tFile"},
		Short:   "tests the file created using AZCopy v2",

		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 2 {
				return fmt.Errorf("invalid arguments for test file command")
			}
			// first argument is the resource name.
			cmdInput.Object = args[0]

			// second argument is the test directory.
			cmdInput.Subject = args[1]

			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			verifyFile(cmdInput)
		},
	}
	rootCmd.AddCommand(testFileCmd)
	// add flags.
	testFileCmd.PersistentFlags().StringVar(&cmdInput.MetaData, "metadata", "", "metadata expected from the file in the container")
	testFileCmd.PersistentFlags().StringVar(&cmdInput.ContentType, "content-type", "", "content type expected from the file in the container")
	testFileCmd.PersistentFlags().StringVar(&cmdInput.ContentEncoding, "content-encoding", "", "validate the given HTTP header.")
	testFileCmd.PersistentFlags().StringVar(&cmdInput.ContentDisposition, "content-disposition", "", "validate the given HTTP header.")
	testFileCmd.PersistentFlags().StringVar(&cmdInput.ContentLanguage, "content-language", "", "validate the given HTTP header.")
	testFileCmd.PersistentFlags().StringVar(&cmdInput.CacheControl, "cache-control", "", "validate the given HTTP header.")
	testFileCmd.PersistentFlags().BoolVar(&cmdInput.CheckContentMD5, "check-content-md5", false, "Validate content MD5 is not empty.")
	testFileCmd.PersistentFlags().BoolVar(&cmdInput.IsObjectDirectory, "is-object-dir", false, "set the type of object to verify against the subject")
	testFileCmd.PersistentFlags().BoolVar(&cmdInput.IsRecursive, "is-recursive", true, "Set whether to validate against subject recursively when object is directory.")
	// TODO: parameter name doesn't match file scenario, discuss and refactor.
	testFileCmd.PersistentFlags().Uint64Var(&cmdInput.NumberOfBlocksOrPages, "number-blocks-or-pages", 0, "Use this block size to verify the number of blocks uploaded")
	testFileCmd.PersistentFlags().BoolVar(&cmdInput.VerifyBlockOrPageSize, "verify-block-size", false, "this flag verify the block size by determining the number of blocks")
	testFileCmd.PersistentFlags().BoolVar(&cmdInput.NoGuessMimeType, "no-guess-mime-type", false, "This sets the content-type based on the extension of the file.")
	testFileCmd.PersistentFlags().BoolVar(&cmdInput.PreserveLastModifiedTime, "preserve-last-modified-time", false, "Only available when destination is file system.")
}

// verify the file downloaded or uploaded.
func verifyFile(testFileCmd TestFileCommand) {
	if testFileCmd.IsObjectDirectory {
		verifyFileDirUpload(testFileCmd)
	} else {
		verifySingleFileUpload(testFileCmd)
	}
}

// verifyFileDirUpload verifies the directory recursively uploaded to the share or directory.
func verifyFileDirUpload(testFileCmd TestFileCommand) {
	// parse the subject url.
	sasURL, err := url.Parse(testFileCmd.Subject)
	if err != nil {
		// fmt.Println("fail to parse the container sas ", testFileCmd.Subject)
		os.Exit(1)
	}

	// as it's a directory validation, regard the sasURL as a directory
	p := azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{})
	directoryURL := azfile.NewDirectoryURL(*sasURL, p)

	// get the original dir path, which can be used to get file relative path during enumerating and comparing
	baseAzureDirPath := azfile.NewFileURLParts(*sasURL).DirectoryOrFilePath

	// validate azure directory
	validateAzureDirWithLocalFile(directoryURL, baseAzureDirPath, testFileCmd.Object, testFileCmd.IsRecursive)
}

// recursively validate files in azure directories and sub-directories
func validateAzureDirWithLocalFile(curAzureDirURL azfile.DirectoryURL, baseAzureDirPath string, localBaseDir string, isRecursive bool) {
	for marker := (azfile.Marker{}); marker.NotDone(); {
		// look for all files that in current directory
		listFile, err := curAzureDirURL.ListFilesAndDirectoriesSegment(context.Background(), marker, azfile.ListFilesAndDirectoriesOptions{})
		if err != nil {
			// fmt.Println(fmt.Sprintf("fail to list files and directories inside the directory. Please check the directory sas, %v", err))
			os.Exit(1)
		}

		if isRecursive {
			for _, dirInfo := range listFile.DirectoryItems {
				newDirURL := curAzureDirURL.NewDirectoryURL(dirInfo.Name)
				validateAzureDirWithLocalFile(newDirURL, baseAzureDirPath, localBaseDir, isRecursive)
			}
		}

		// Process the files returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, fileInfo := range listFile.FileItems {
			curFileURL := curAzureDirURL.NewFileURL(fileInfo.Name)
			get, err := curFileURL.Download(context.Background(), 0, azfile.CountToEnd, false)

			if err != nil {
				fmt.Println(fmt.Sprintf("fail to download the file %s", fileInfo.Name))
				os.Exit(1)
			}

			retryReader := get.Body(azfile.RetryReaderOptions{MaxRetryRequests: 3})

			// read all bytes.
			fileBytesDownloaded, err := ioutil.ReadAll(retryReader)
			if err != nil {
				fmt.Println(fmt.Sprintf("fail to read the body of file %s downloaded and failed with error %s", fileInfo.Name, err.Error()))
				os.Exit(1)
			}
			retryReader.Close()

			tokens := strings.SplitAfterN(curFileURL.URL().Path, baseAzureDirPath, 2)
			if len(tokens) < 2 {
				fmt.Println(fmt.Sprintf("fail to get sub directory and file name, file URL '%s', original dir path '%s'", curFileURL.String(), baseAzureDirPath))
				os.Exit(1)
			}

			subDirAndFileName := tokens[1]
			var objectLocalPath string
			if subDirAndFileName != "" && subDirAndFileName[0] != '/' {
				objectLocalPath = localBaseDir + "/" + subDirAndFileName
			} else {
				objectLocalPath = localBaseDir + subDirAndFileName
			}

			// opening the file locally and memory mapping it.
			sFileInfo, err := os.Stat(objectLocalPath)
			if err != nil {
				fmt.Println("fail to get the subject file info on local disk ")
				os.Exit(1)
			}

			sFile, err := os.Open(objectLocalPath)
			if err != nil {
				fmt.Println("fail to open file ", sFile)
				os.Exit(1)
			}
			sMap, err := NewMMF(sFile, false, 0, sFileInfo.Size())
			if err != nil {
				fmt.Println("fail to memory mapping the file ", sFileInfo.Name())
			}

			// calculating the md5 of file on container.
			actualMd5 := md5.Sum(fileBytesDownloaded)
			// calculating md5 of resource locally.
			expectedMd5 := md5.Sum(sMap)

			if actualMd5 != expectedMd5 {
				fmt.Println("the upload file md5 is not equal to the md5 of actual file on disk for file ", fileInfo.Name)
				os.Exit(1)
			}
		}

		marker = listFile.NextMarker
	}
}

// validateMetadataForFile compares the meta data provided while
// uploading and metadata with file in the container.
func validateMetadataForFile(expectedMetaDataString string, actualMetaData azfile.Metadata) bool {
	if len(expectedMetaDataString) > 0 {
		// split the meta data string to get the map of key value pair
		// metadata string is in format key1=value1;key2=value2;key3=value3
		expectedMetaData := azfile.Metadata{}
		// split the metadata to get individual keyvalue pair in format key1=value1
		keyValuePair := strings.Split(expectedMetaDataString, ";")
		for index := 0; index < len(keyValuePair); index++ {
			// split the individual key value pair to get key and value
			keyValue := strings.Split(keyValuePair[index], "=")
			expectedMetaData[keyValue[0]] = keyValue[1]
		}
		// if number of metadata provided while uploading
		// doesn't match the metadata with file on the container
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

// verifySingleFileUpload verifies the pagefile uploaded or downloaded
// against the file locally.
func verifySingleFileUpload(testFileCmd TestFileCommand) {

	fileInfo, err := os.Stat(testFileCmd.Object)
	if err != nil {
		fmt.Println("error opening the destination file on local disk ")
		os.Exit(1)
	}
	file, err := os.Open(testFileCmd.Object)
	if err != nil {
		fmt.Println("error opening the file ", testFileCmd.Object)
	}

	// getting the shared access signature of the resource.
	sourceURL, err := url.Parse(testFileCmd.Subject)
	if err != nil {
		// fmt.Println(fmt.Sprintf("Error parsing the file url source %s", testFileCmd.Object))
		os.Exit(1)
	}

	// creating the page file url of the resource on container.
	p := azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{Retry: azfile.RetryOptions{TryTimeout: time.Minute * 10}})
	fileURL := azfile.NewFileURL(*sourceURL, p)
	get, err := fileURL.Download(context.Background(), 0, azfile.CountToEnd, false)
	if err != nil {
		fmt.Println("unable to get file properties ", err.Error())
		os.Exit(1)
	}

	// reading all the bytes downloaded.
	retryReader := get.Body(azfile.RetryReaderOptions{MaxRetryRequests: 3})
	defer retryReader.Close()
	fileBytesDownloaded, err := ioutil.ReadAll(retryReader)
	if err != nil {
		fmt.Println("error reading the byes from response and failed with error ", err.Error())
		os.Exit(1)
	}

	if fileInfo.Size() == 0 {
		// If the fileSize is 0 and the len of downloaded bytes is not 0
		// validation fails
		if len(fileBytesDownloaded) != 0 {
			fmt.Println(fmt.Sprintf("validation failed since the actual file size %d differs from the downloaded file size %d", fileInfo.Size(), len(fileBytesDownloaded)))
			os.Exit(1)
		}
		// If both the actual and downloaded file size is 0,
		// validation is successful, no need to match the md5
		os.Exit(0)
	}

	// memory mapping the resource on local path.
	mmap, err := NewMMF(file, false, 0, fileInfo.Size())
	if err != nil {
		fmt.Println("error mapping the destination file: ", file, " file size: ", fileInfo.Size(), " Error: ", err.Error())
		os.Exit(1)
	}

	// calculating and verify the md5 of the resource
	// both locally and on the container.
	actualMd5 := md5.Sum(mmap)
	expectedMd5 := md5.Sum(fileBytesDownloaded)
	if actualMd5 != expectedMd5 {
		fmt.Println("the uploaded file's md5 doesn't matches the actual file's md5 for file ", testFileCmd.Object)
		os.Exit(1)
	}

	if testFileCmd.CheckContentMD5 && (get.ContentMD5() == nil || len(get.ContentMD5()) == 0) {
		fmt.Println("ContentMD5 should not be empty")
		os.Exit(1)
	}

	// verify the user given metadata supplied while uploading the file against the metadata actually present in the file
	if !validateMetadataForFile(testFileCmd.MetaData, get.NewMetadata()) {
		fmt.Println("meta data does not match between the actual and uploaded file.")
		os.Exit(1)
	}

	// verify the content-type
	expectedContentType := ""
	if testFileCmd.NoGuessMimeType {
		expectedContentType = testFileCmd.ContentType
	} else {
		expectedContentType = http.DetectContentType(mmap)
	}
	if !validateString(expectedContentType, get.ContentType()) {
		fmt.Println("mismatch content type between actual and user given file content type")
		os.Exit(1)
	}

	//verify the content-encoding
	if !validateString(testFileCmd.ContentEncoding, get.ContentEncoding()) {
		fmt.Println("mismatch content encoding between actual and user given file content encoding")
		os.Exit(1)
	}

	if !validateString(testFileCmd.ContentDisposition, get.ContentDisposition()) {
		fmt.Println("mismatch content disposition between actual and user given value")
		os.Exit(1)
	}

	if !validateString(testFileCmd.ContentLanguage, get.ContentLanguage()) {
		fmt.Println("mismatch content encoding between actual and user given value")
		os.Exit(1)
	}

	if !validateString(testFileCmd.CacheControl, get.CacheControl()) {
		fmt.Println("mismatch cache control between actual and user given value")
		os.Exit(1)
	}

	mmap.Unmap()
	file.Close()

	// verify the number of pageranges.
	// this verifies the page-size and azcopy pagefile implementation.
	if testFileCmd.VerifyBlockOrPageSize {
		numberOfPages := int(testFileCmd.NumberOfBlocksOrPages)
		resp, err := fileURL.GetRangeList(context.Background(), 0, azfile.CountToEnd)
		if err != nil {
			fmt.Println("error getting the range list ", err.Error())
			os.Exit(1)
		}
		if numberOfPages != (len(resp.Items)) {
			fmt.Println("number of ranges uploaded is different from the number of expected to be uploaded")
			os.Exit(1)
		}
	}
}
