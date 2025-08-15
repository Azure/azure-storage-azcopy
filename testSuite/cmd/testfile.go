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

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/directory"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-storage-azcopy/v10/common"
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
	directoryClient, _ := directory.NewClientWithNoCredential(testFileCmd.Subject, nil)

	// get the original dir path, which can be used to get file relative path during enumerating and comparing
	fileURLParts, err := file.ParseURL(testFileCmd.Subject)
	if err != nil {
		os.Exit(1)
	}
	baseAzureDirPath := fileURLParts.DirectoryOrFilePath

	// validate azure directory
	validateAzureDirWithLocalFile(directoryClient, baseAzureDirPath, testFileCmd.Object, testFileCmd.IsRecursive)
}

// recursively validate files in azure directories and sub-directories
func validateAzureDirWithLocalFile(curAzureDirURL *directory.Client, baseAzureDirPath string, localBaseDir string, isRecursive bool) {
	pager := curAzureDirURL.NewListFilesAndDirectoriesPager(nil)
	for pager.More() {
		// look for all files that in current directory
		listFile, err := pager.NextPage(context.Background())
		if err != nil {
			// fmt.Printf("fail to list files and directories inside the directory. Please check the directory sas, %v\n", err)
			os.Exit(1)
		}

		if isRecursive {
			for _, dirInfo := range listFile.Segment.Directories {
				newDirURL := curAzureDirURL.NewSubdirectoryClient(*dirInfo.Name)
				validateAzureDirWithLocalFile(newDirURL, baseAzureDirPath, localBaseDir, isRecursive)
			}
		}

		// Process the files returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, fileInfo := range listFile.Segment.Files {
			curFileURL := curAzureDirURL.NewFileClient(*fileInfo.Name)
			get, err := curFileURL.DownloadStream(context.Background(), nil)

			if err != nil {
				fmt.Printf("fail to download the file %s\n", *fileInfo.Name)
				os.Exit(1)
			}

			retryReader := get.NewRetryReader(context.Background(), &file.RetryReaderOptions{MaxRetries: 3})

			// read all bytes.
			fileBytesDownloaded, err := io.ReadAll(retryReader)
			if err != nil {
				fmt.Printf("fail to read the body of file %s downloaded and failed with error %s\n", *fileInfo.Name, err.Error())
				os.Exit(1)
			}
			retryReader.Close()

			url, err := url.Parse(curFileURL.URL())
			if err != nil {
				fmt.Printf("fail to parse the file URL %s\n", curFileURL.URL())
				os.Exit(1)
			}
			tokens := strings.SplitAfterN(url.Path, baseAzureDirPath, 2)
			if len(tokens) < 2 {
				fmt.Printf("fail to get sub directory and file name, file URL '%s', original dir path '%s'\n", curFileURL.URL(), baseAzureDirPath)
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

	}
}

// verifySingleFileUpload verifies the pagefile uploaded or downloaded
// against the file locally.
func verifySingleFileUpload(testFileCmd TestFileCommand) {

	fileInfo, err := os.Stat(testFileCmd.Object)
	if err != nil {
		fmt.Println("error opening the destination localFile on local disk ")
		os.Exit(1)
	}
	localFile, err := os.Open(testFileCmd.Object)
	if err != nil {
		fmt.Println("error opening the localFile ", testFileCmd.Object)
	}

	fileClient, _ := file.NewClientWithNoCredential(testFileCmd.Subject, nil)
	get, err := fileClient.DownloadStream(context.Background(), nil)
	if err != nil {
		fmt.Println("unable to get localFile properties ", err.Error())
		os.Exit(1)
	}

	// reading all the bytes downloaded.
	retryReader := get.NewRetryReader(context.Background(), &file.RetryReaderOptions{MaxRetries: 3})
	defer retryReader.Close()
	fileBytesDownloaded, err := io.ReadAll(retryReader)
	if err != nil {
		fmt.Println("error reading the byes from response and failed with error ", err.Error())
		os.Exit(1)
	}

	if fileInfo.Size() == 0 {
		// If the fileSize is 0 and the len of downloaded bytes is not 0
		// validation fails
		if len(fileBytesDownloaded) != 0 {
			fmt.Printf("validation failed since the actual localFile size %d differs from the downloaded localFile size %d\n", fileInfo.Size(), len(fileBytesDownloaded))
			os.Exit(1)
		}
		// If both the actual and downloaded localFile size is 0,
		// validation is successful, no need to match the md5
		os.Exit(0)
	}

	// memory mapping the resource on local path.
	mmap, err := NewMMF(localFile, false, 0, fileInfo.Size())
	if err != nil {
		fmt.Println("error mapping the destination localFile: ", localFile, " localFile size: ", fileInfo.Size(), " OnError: ", err.Error())
		os.Exit(1)
	}

	// calculating and verify the md5 of the resource
	// both locally and on the container.
	actualMd5 := md5.Sum(mmap)
	expectedMd5 := md5.Sum(fileBytesDownloaded)
	if actualMd5 != expectedMd5 {
		fmt.Println("the uploaded localFile's md5 doesn't matches the actual localFile's md5 for localFile ", testFileCmd.Object)
		os.Exit(1)
	}

	if testFileCmd.CheckContentMD5 && len(get.ContentMD5) == 0 {
		fmt.Println("ContentMD5 should not be empty")
		os.Exit(1)
	}

	// verify the user given metadata supplied while uploading the localFile against the metadata actually present in the localFile
	if !validateMetadata(testFileCmd.MetaData, get.Metadata) {
		fmt.Println("meta data does not match between the actual and uploaded localFile.")
		os.Exit(1)
	}

	// verify the content-type
	expectedContentType := ""
	if testFileCmd.NoGuessMimeType {
		expectedContentType = testFileCmd.ContentType
	} else {
		expectedContentType = http.DetectContentType(mmap)
	}
	expectedContentType = strings.Split(expectedContentType, ";")[0]
	if !validateString(expectedContentType, common.IffNotNil(get.ContentType, "")) {
		str1 := fmt.Sprintf(" %s    %s", expectedContentType, common.IffNotNil(get.ContentDisposition, ""))
		fmt.Println(str1 + "mismatch content type between actual and user given localFile content type")
		os.Exit(1)
	}

	//verify the content-encoding
	if !validateString(testFileCmd.ContentEncoding, common.IffNotNil(get.ContentEncoding, "")) {
		fmt.Println("mismatch content encoding between actual and user given localFile content encoding")
		os.Exit(1)
	}

	if !validateString(testFileCmd.ContentDisposition, common.IffNotNil(get.ContentDisposition, "")) {
		fmt.Println("mismatch content disposition between actual and user given value")
		os.Exit(1)
	}

	if !validateString(testFileCmd.ContentLanguage, common.IffNotNil(get.ContentLanguage, "")) {
		fmt.Println("mismatch content encoding between actual and user given value")
		os.Exit(1)
	}

	if !validateString(testFileCmd.CacheControl, common.IffNotNil(get.CacheControl, "")) {
		fmt.Println("mismatch cache control between actual and user given value")
		os.Exit(1)
	}

	mmap.Unmap()
	localFile.Close()

	// verify the number of pageranges.
	// this verifies the page-size and azcopy pagefile implementation.
	if testFileCmd.VerifyBlockOrPageSize {
		numberOfPages := int(testFileCmd.NumberOfBlocksOrPages)
		resp, err := fileClient.GetRangeList(context.Background(), nil)
		if err != nil {
			fmt.Println("error getting the range list ", err.Error())
			os.Exit(1)
		}
		if numberOfPages != (len(resp.Ranges)) {
			fmt.Println("number of ranges uploaded is different from the number of expected to be uploaded")
			os.Exit(1)
		}
	}
}
