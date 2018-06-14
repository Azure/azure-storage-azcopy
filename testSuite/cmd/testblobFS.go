package cmd

import (
	"context"
	"crypto/md5"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/azbfs"
	"github.com/spf13/cobra"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

// TestBlobFSCommand represents the struct to get command
// for validating azcopy operations upload and download operations
// to and from Blob FS Service.
type TestBlobFSCommand struct {
	// object is the resource which needs to be validated against a resource on container.
	Object string
	//Subject is the remote resource against which object needs to be validated.
	Subject string
	// IsObjectDirectory defines if the object is a directory or not.
	// If the object is directory, then validation goes through another path.
	IsObjectDirectory bool
}

// initializes the testblobfs command, its aliases and description.
// also adds the possible flags that can be supplied with testBlob command.
func init() {
	cmdInput := TestBlobFSCommand{}
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
			cmdInput.processTest()
		},
	}
	rootCmd.AddCommand(testBlobCmd)
	// add flags.
	testBlobCmd.PersistentFlags().BoolVar(&cmdInput.IsObjectDirectory, "is-object-dir", false, "set the type of object to verify against the subject")
}

// verify the blob downloaded or uploaded.
func (tbfsc TestBlobFSCommand) processTest() {
	if tbfsc.IsObjectDirectory {
		tbfsc.verifyRemoteDir()
	} else {
		tbfsc.verifyRemoteFile()
	}
}

// verifyBlockBlobDirUpload verifies the directory recursively uploaded to the container.
func (tbfsc TestBlobFSCommand) verifyRemoteFile() {
	// parse the subject url.
	subjectUrl, err := url.Parse(tbfsc.Subject)
	if err != nil {
		fmt.Println("error parsing the container sas ", tbfsc.Subject)
		os.Exit(1)
	}

	// Get the Account Name and Key variables from environment
	name := os.Getenv("ACCOUNT_NAME")
	key := os.Getenv("ACCOUNT_KEY")
	// If the ACCOUNT_NAME and ACCOUNT_KEY are not set in environment variables
	if name == "" || key == "" {
		fmt.Println("ACCOUNT_NAME and ACCOUNT_KEY should be set before executing the test")
		os.Exit(1)
	}
	c := azbfs.NewSharedKeyCredential(name, key)
	p := azbfs.NewPipeline(c, azbfs.PipelineOptions{})

	fileUrl := azbfs.NewFileURL(*subjectUrl, p)

	dResp, err := fileUrl.Download(context.Background(), 0, 0)
	if err != nil {
		fmt.Println(fmt.Sprintf("error downloading the subject %s. Failed with error %s", fileUrl.String(), err.Error()))
		os.Exit(1)
	}
	var downloadedBuffer []byte
	// step 2: write the body into the memory mapped file directly
	retryReader := dResp.Body(azbfs.RetryReaderOptions{MaxRetryRequests: 5})
	_, err = io.ReadFull(retryReader, downloadedBuffer)
	if err != nil {
		fmt.Println("error reading the downloaded body ", err.Error())
		os.Exit(1)
	}
	f, err := os.Open(tbfsc.Object)
	if err != nil {
		fmt.Println("error opening the object ", tbfsc.Object, " failed with error %", err.Error())
		os.Exit(1)
	}
	fInfo, err := f.Stat()
	if err != nil {
		fmt.Println("error getting the file Info of opened file ", tbfsc.Object, " failed with error ", err.Error())
		os.Exit(1)
	}
	defer f.Close()

	mMap, err := NewMMF(f, false, 0, fInfo.Size())
	if err != nil {
		fmt.Println("error memory mapping the file ", tbfsc.Object, " failed with error ", err.Error())
		os.Exit(1)
	}

	defer mMap.Unmap()
	// calculate the md5Sum of object
	objMd5 := md5.Sum(mMap)
	subjMd5 := md5.Sum(downloadedBuffer)
	if objMd5 != subjMd5 {
		fmt.Println("object md5 is not equal to the downloaded md5")
		os.Exit(1)
	}
}

func (tbfsc TestBlobFSCommand) verifyRemoteDir() {
	// Get the Account Name and Key variables from environment
	name := os.Getenv("ACCOUNT_NAME")
	key := os.Getenv("ACCOUNT_KEY")
	// If the ACCOUNT_NAME and ACCOUNT_KEY are not set in environment variables
	if name == "" || key == "" {
		fmt.Println("ACCOUNT_NAME and ACCOUNT_KEY should be set before executing the test")
		os.Exit(1)
	}
	c := azbfs.NewSharedKeyCredential(name, key)
	p := azbfs.NewPipeline(c, azbfs.PipelineOptions{})

	subjectUrl, err := url.Parse(tbfsc.Subject)
	if err != nil {
		fmt.Println(fmt.Sprintf("Error parsing the url %s. failed with error %s", subjectUrl, err.Error()))
		os.Exit(1)
	}
	objectInfo, err := os.Stat(tbfsc.Object)
	if err != nil {
		fmt.Println(fmt.Sprintf("error getting the file info for dir %s. failed with error %s", tbfsc.Object, err.Error()))
		os.Exit(1)
	}
	if !objectInfo.IsDir() {
		fmt.Println(fmt.Sprintf("the source provided %s is not a directory path", tbfsc.Object))
		os.Exit(1)
	}
	urlParts := azbfs.NewFileURLParts(*subjectUrl)
	currentDirectoryPath := urlParts.DirectoryOrFilePath

	dirUrl := azbfs.NewDirectoryURL(*subjectUrl, p)
	continuationMarker := ""
	var firstListing bool = true
	dResp, err := dirUrl.ListDirectory(context.Background(), &continuationMarker, true)
	if err != nil {
		fmt.Println(fmt.Sprintf("error listing the directory path defined by url %s. Failed with error %s", dirUrl.String(), err.Error()))
		os.Exit(1)
	}
	for continuationMarker != "" || firstListing == true {
		firstListing = false
		continuationMarker = dResp.XMsContinuation()
		files := dResp.Files()
		for _, file := range files {
			// Get the file path
			// remove the directory path from the file path
			// to get the relative path
			filePath := *file.Name
			filePath = strings.Replace(filePath, currentDirectoryPath, "", 1)
			relativefilepath := strings.Trim(filePath, "/")
			// replace the "/" with os path separator
			relativefilepath = strings.Replace(relativefilepath, "/", string(os.PathSeparator), -1)
			// create the expected local path of remote file
			filepathLocal := filepath.Join(tbfsc.Object, relativefilepath)
			// open the filePath locally and calculate the md5
			fpLocal, err := os.Open(filepathLocal)
			if err != nil {
				fmt.Println(fmt.Sprintf("error opening the file %s. failed with error %s", filepathLocal, err.Error()))
				os.Exit(1)
			}
			// Get the fileInfo to get size.
			fpLocalInfo, err := fpLocal.Stat()
			if err != nil {
				fmt.Println(fmt.Sprintf("error getting the file info for file %s. failed with error %s", filepathLocal, err.Error()))
				os.Exit(1)
			}
			// Check the size of file
			// If the size of file doesn't matches, then exit with error
			if fpLocalInfo.Size() != *file.ContentLength {
				fmt.Println("the size of local file does not match the remote file")
				os.Exit(1)
			}
			// If the size of file is zero then continue to next file
			if fpLocalInfo.Size() == 0 {
				continue
			}

			defer fpLocal.Close()

			fpMMf, err := NewMMF(fpLocal, false, 0, fpLocalInfo.Size())
			if err != nil {
				fmt.Println(fmt.Sprintf("error memory mapping the file %s. failed with error %s", filepathLocal, err.Error()))
				os.Exit(1)
			}

			defer fpMMf.Unmap()

			// calculated the source md5
			objMd5 := md5.Sum(fpMMf)
			// Download the remote file and calculate md5
			tempUrlParts := urlParts
			tempUrlParts.DirectoryOrFilePath = filePath
			fileUrl := azbfs.NewFileURL(tempUrlParts.URL(), p)
			fResp, err := fileUrl.Download(context.Background(), 0, 0)
			if err != nil {
				fmt.Println(fmt.Sprintf("error downloading the file %s. failed with error %s", fileUrl.String(), err.Error()))
				os.Exit(1)
			}
			var downloadedBuffer []byte // byte buffer in which file will be downloaded to
			retryReader := fResp.Body(azbfs.RetryReaderOptions{MaxRetryRequests: 5})
			_, err = io.ReadFull(retryReader, downloadedBuffer)
			if err != nil {
				fmt.Println("error reading the downloaded body ", err.Error())
				os.Exit(1)
			}
			// calculate the downloaded file Md5
			subjMd5 := md5.Sum(downloadedBuffer)
			if objMd5 != subjMd5 {
				fmt.Println("source file %s doesn't match the remote file %s", filepathLocal, fileUrl.String())
				os.Exit(1)
			}
		}
	}
}
