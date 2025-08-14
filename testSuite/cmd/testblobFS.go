package cmd

import (
	"context"
	"crypto/md5"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/file"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/filesystem"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
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
		Use:     "testBlobFS",
		Aliases: []string{"tBlobFS"},
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

// verifyRemoteFile verifies the local file (object) against the file on remote fileSystem (subject)
func (tbfsc TestBlobFSCommand) verifyRemoteFile() {
	// Get BFS url parts to test SAS
	datalakeURLParts, err := azdatalake.ParseURL(tbfsc.Subject)
	if err != nil {
		fmt.Println("error parsing the datalake sas ", tbfsc.Subject)
		os.Exit(1)
	}

	// Get the Account Name and Key variables from environment
	name := os.Getenv("ACCOUNT_NAME")
	key := os.Getenv("ACCOUNT_KEY")
	// If ACCOUNT_NAME or ACCOUNT_KEY is not supplied AND a SAS is not supplied
	if (name == "" && key == "") && datalakeURLParts.SAS.Encode() == "" {
		fmt.Println("ACCOUNT_NAME and ACCOUNT_KEY should be set before executing the test, OR a SAS token should be supplied in the subject URL.")
		os.Exit(1)
	}
	var fc *file.Client
	ctx := context.Background()
	if datalakeURLParts.SAS.Encode() != "" {
		fc, err = file.NewClientWithNoCredential(datalakeURLParts.String(), nil)
	} else {
		var cred *azdatalake.SharedKeyCredential
		cred, err = azdatalake.NewSharedKeyCredential(name, key)
		if err != nil {
			fmt.Printf("error creating shared key cred. failed with error %s\n", err.Error())
			os.Exit(1)
		}
		perCallPolicies := []policy.Policy{ste.NewVersionPolicy()}
		fc, err = file.NewClientWithSharedKeyCredential(datalakeURLParts.String(), cred, &file.ClientOptions{ClientOptions: azcore.ClientOptions{PerCallPolicies: perCallPolicies}})
		ctx = context.WithValue(ctx, ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	}
	if err != nil {
		fmt.Printf("error creating client. failed with error %s\n", err.Error())
		os.Exit(1)
	}

	// create the file url and download the file Url
	dResp, err := fc.DownloadStream(ctx, nil)
	if err != nil {
		fmt.Printf("error downloading the subject %s. Failed with error %s\n", datalakeURLParts.String(), err.Error())
		os.Exit(1)
	}
	// get the size of the downloaded file
	downloadedLength := *dResp.ContentLength

	// open the local file
	f, err := os.Open(tbfsc.Object)
	if err != nil {
		fmt.Println("error opening the object ", tbfsc.Object, " failed with error %", err.Error())
		os.Exit(1)
	}
	fInfo, err := f.Stat()
	if err != nil {
		fmt.Println("error getting the file OnInfo of opened file ", tbfsc.Object, " failed with error ", err.Error())
		os.Exit(1)
	}
	defer f.Close()

	// If the length of file at two location is not same
	// validation has failed
	if downloadedLength != fInfo.Size() {
		fmt.Printf("validation failed because there is difference in the source size %d and destination size %d\n", fInfo.Size(), downloadedLength)
		os.Exit(1)
	}
	// If the size of the file is 0 both locally and remote
	// there is no need to download the file and memory map the file
	// validation is passed.
	if fInfo.Size() == 0 {
		os.Exit(1)
	}

	// read the downloaded content into the buffer
	downloadedBuffer := make([]byte, downloadedLength)
	_, err = io.ReadFull(dResp.Body, downloadedBuffer)
	if err != nil {
		fmt.Println("error reading the downloaded body ", err.Error())
		os.Exit(1)
	}
	// memory map the local file
	mMap, err := NewMMF(f, false, 0, fInfo.Size())
	if err != nil {
		fmt.Println("error memory mapping the file ", tbfsc.Object, " failed with error ", err.Error())
		os.Exit(1)
	}

	defer mMap.Unmap()
	// calculate the md5Sum of object and subject
	objMd5 := md5.Sum(mMap)
	subjMd5 := md5.Sum(downloadedBuffer)
	// if the md5 of two doesn't match
	// validation has failed
	if objMd5 != subjMd5 {
		fmt.Println("object md5 is not equal to the downloaded md5")
		os.Exit(1)
	}
}

// verifyRemoteDir validates the local directory (object) against the directory
// on filesystem (subject)
func (tbfsc TestBlobFSCommand) verifyRemoteDir() {
	// Get BFS url parts to test SAS
	datalakeURLParts, err := azdatalake.ParseURL(tbfsc.Subject)
	if err != nil {
		fmt.Println("error parsing the datalake sas ", tbfsc.Subject)
		os.Exit(1)
	}
	// break the remote Url into parts
	// and save the directory path
	currentDirectoryPath := datalakeURLParts.PathName
	datalakeURLParts.PathName = ""

	// Get the Account Name and Key variables from environment
	name := os.Getenv("ACCOUNT_NAME")
	key := os.Getenv("ACCOUNT_KEY")
	// If ACCOUNT_NAME or ACCOUNT_KEY is not supplied AND a SAS is not supplied
	if (name == "" && key == "") && datalakeURLParts.SAS.Encode() == "" {
		fmt.Println("ACCOUNT_NAME and ACCOUNT_KEY should be set before executing the test, OR a SAS token should be supplied in the subject URL.")
		os.Exit(1)
	}
	var fsc *filesystem.Client
	ctx := context.Background()
	if datalakeURLParts.SAS.Encode() != "" {
		fsc, err = filesystem.NewClientWithNoCredential(datalakeURLParts.String(), nil)
	} else {
		var cred *azdatalake.SharedKeyCredential
		cred, err = azdatalake.NewSharedKeyCredential(name, key)
		if err != nil {
			fmt.Printf("error creating shared key cred. failed with error %s\n", err.Error())
			os.Exit(1)
		}
		perCallPolicies := []policy.Policy{ste.NewVersionPolicy()}
		fsc, err = filesystem.NewClientWithSharedKeyCredential(datalakeURLParts.String(), cred, &filesystem.ClientOptions{ClientOptions: azcore.ClientOptions{PerCallPolicies: perCallPolicies}})
		ctx = context.WithValue(ctx, ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	}
	if err != nil {
		fmt.Printf("error creating client. failed with error %s\n", err.Error())
		os.Exit(1)
	}
	// Get the object OnInfo and If the object is not a directory
	// validation fails since validation has two be done between directories
	// local and remote
	objectInfo, err := os.Stat(tbfsc.Object)
	if err != nil {
		fmt.Printf("error getting the file info for dir %s. failed with error %s\n", tbfsc.Object, err.Error())
		os.Exit(1)
	}
	if !objectInfo.IsDir() {
		fmt.Printf("the source provided %s is not a directory path\n", tbfsc.Object)
		os.Exit(1)
	}

	// List the directory
	pager := fsc.NewListPathsPager(true, &filesystem.ListPathsOptions{Prefix: &currentDirectoryPath})
	// numberOfFilesinSubject keeps the count of number of files of at the destination
	numberOfFilesinSubject := int(0)
	for pager.More() {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			fmt.Printf("error listing the directory path defined by url %s. Failed with error %s\n", datalakeURLParts.String(), err.Error())
			os.Exit(1)
		}
		paths := resp.PathList.Paths
		numberOfFilesinSubject += len(paths)
		for _, p := range paths {
			// Get the file path
			// remove the directory path from the file path
			// to get the relative path
			filePath := *p.Name
			filePath = strings.Replace(filePath, currentDirectoryPath, "", 1)
			relativefilepath := strings.Trim(filePath, "/")
			// replace the "/" with os path separator
			relativefilepath = strings.Replace(relativefilepath, "/", string(os.PathSeparator), -1)
			// create the expected local path of remote file
			filepathLocal := filepath.Join(tbfsc.Object, relativefilepath)
			// open the filePath locally and calculate the md5
			fpLocal, err := os.Open(filepathLocal)
			if err != nil {
				fmt.Printf("error opening the file %s. failed with error %s\n", filepathLocal, err.Error())
				os.Exit(1)
			}
			// Get the fileInfo to get size.
			fpLocalInfo, err := fpLocal.Stat()
			if err != nil {
				fmt.Printf("error getting the file info for file %s. failed with error %s\n", filepathLocal, err.Error())
				os.Exit(1)
			}
			// Check the size of file
			// If the size of file doesn't matches, then exit with error
			if fpLocalInfo.Size() != *p.ContentLength {
				fmt.Println("the size of local file does not match the remote file")
				os.Exit(1)
			}
			// If the size of file is zero then continue to next file
			if fpLocalInfo.Size() == 0 {
				continue
			}

			defer fpLocal.Close()

			// memory map the file
			fpMMf, err := NewMMF(fpLocal, false, 0, fpLocalInfo.Size())
			if err != nil {
				fmt.Printf("error memory mapping the file %s. failed with error %s\n", filepathLocal, err.Error())
				os.Exit(1)
			}

			defer fpMMf.Unmap()

			// calculated the source md5
			objMd5 := md5.Sum(fpMMf)
			// Download the remote file and calculate md5
			tempUrlParts := datalakeURLParts
			tempUrlParts.PathName = *p.Name
			fc := fsc.NewFileClient(tempUrlParts.PathName)
			fResp, err := fc.DownloadStream(ctx, nil)
			if err != nil {
				fmt.Printf("error downloading the file %s. failed with error %s\n", fc.DFSURL(), err.Error())
				os.Exit(1)
			}
			downloadedBuffer := make([]byte, *p.ContentLength) // byte buffer in which file will be downloaded to
			_, err = io.ReadFull(fResp.Body, downloadedBuffer)
			if err != nil {
				fmt.Println("error reading the downloaded body ", err.Error())
				os.Exit(1)
			}
			// calculate the downloaded file Md5
			subjMd5 := md5.Sum(downloadedBuffer)
			if objMd5 != subjMd5 {
				fmt.Printf("source file %s doesn't match the remote file %s\n", filepathLocal, fc.DFSURL())
				os.Exit(1)
			}
		}
	}
	// walk through the directory and count the number of files inside the local directory
	numberOFFilesInObject := int(0)
	err = filepath.Walk(tbfsc.Object, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			numberOFFilesInObject++
		}
		return nil
	})
	if err != nil {
		fmt.Printf("validation failed with error %s walking inside the source %s\n", err.Error(), tbfsc.Object)
		os.Exit(1)
	}

	// If the number of files inside the directories locally and remote
	// is not same, validation fails.
	if numberOFFilesInObject != numberOfFilesinSubject {
		fmt.Println("validation failed since there is difference in the number of files in source and destination")
		os.Exit(1)
	}
	fmt.Printf("successfully validated the source %s and destination %s\n", tbfsc.Object, tbfsc.Subject)
}
