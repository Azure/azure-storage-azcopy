package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"

	"github.com/Azure/azure-storage-azcopy/common"
)

type copyUploadEnumerator common.CopyJobPartOrderRequest

// accept a new transfer, if the threshold is reached, dispatch a job part order
func (e *copyUploadEnumerator) addTransfer(transfer common.CopyTransfer, wg *sync.WaitGroup,
	waitUntilJobCompletion func(jobID common.JobID, wg *sync.WaitGroup)) error {

	if len(e.Transfers) == NumOfFilesPerUploadJobPart {
		resp := common.CopyJobPartOrderResponse{}
		Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(e), &resp)

		if !resp.JobStarted {
			return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNum, resp.ErrorMsg)
		}
		// if the current part order sent to engine is 0, then start fetching the Job Progress summary.
		if e.PartNum == 0 {
			wg.Add(1)
			go waitUntilJobCompletion(e.JobID, wg)
		}
		e.Transfers = []common.CopyTransfer{}
		e.PartNum++
	}
	e.Transfers = append(e.Transfers, transfer)
	return nil
}

// we need to send a last part with isFinalPart set to true, along with whatever transfers that still haven't been sent
func (e *copyUploadEnumerator) dispatchFinalPart() error {
	e.IsFinalPart = true
	var resp common.CopyJobPartOrderResponse
	Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(e), &resp)

	if !resp.JobStarted {
		return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNum, resp.ErrorMsg)
	}

	return nil
}

// this function accepts the list of files/directories to transfer and processes them
func (e *copyUploadEnumerator) enumerate(src string, isRecursiveOn bool, dst string, wg *sync.WaitGroup,
	waitUntilJobCompletion func(jobID common.JobID, wg *sync.WaitGroup)) error {
	util := copyHandlerUtil{}

	// attempt to parse the destination url
	destinationUrl, err := url.Parse(dst)
	if err != nil {
		// the destination should have already been validated, it would be surprising if it cannot be parsed at this point
		panic(err)
	}

	// list the source files and directories
	listOfFilesAndDirectories, err := filepath.Glob(src)
	if err != nil || len(listOfFilesAndDirectories) == 0 {
		return fmt.Errorf("cannot find source to upload")
	}

	// when a single file is being uploaded, we need to treat this case differently, as the destinationUrl might be a blob
	if len(listOfFilesAndDirectories) == 1 {
		f, err := os.Stat(listOfFilesAndDirectories[0])
		if err != nil {
			return errors.New("cannot find source to upload")
		}

		if !f.IsDir() {
			// append file name as blob name in case the given URL is a container
			if (e.FromTo == common.EFromTo.LocalBlob() && util.urlIsContainerOrShare(destinationUrl)) ||
				(e.FromTo == common.EFromTo.LocalFile() && util.urlIsAzureFileDirectory(context.TODO(), destinationUrl)) {
				destinationUrl.Path = util.generateObjectPath(destinationUrl.Path, f.Name())
			}

			err = e.addTransfer(common.CopyTransfer{
				Source:           listOfFilesAndDirectories[0],
				Destination:      destinationUrl.String(),
				LastModifiedTime: f.ModTime(),
				SourceSize:       f.Size(),
			}, wg, waitUntilJobCompletion)

			if err != nil {
				return err
			}
			return e.dispatchFinalPart()
		}
	}

	// if the user specifies a virtual directory ex: /container_name/extra_path
	// then we should extra_path as a prefix while uploading
	// temporarily save the path of the container
	cleanContainerPath := destinationUrl.Path

	// walk through every file and directory
	// upload every file
	// upload directory recursively if recursive option is on
	for _, fileOrDirectoryPath := range listOfFilesAndDirectories {
		f, err := os.Stat(fileOrDirectoryPath)
		if err == nil {
			// directories are uploaded only if recursive is on
			if f.IsDir() && isRecursiveOn {
				// walk goes through the entire directory tree
				err = filepath.Walk(fileOrDirectoryPath, func(pathToFile string, f os.FileInfo, err error) error {
					if err != nil {
						return err
					}

					if f.IsDir() {
						// skip the subdirectories, we only care about files
						return nil
					} else { // upload the files
						// the path in the blob name started at the given fileOrDirectoryPath
						// example: fileOrDirectoryPath = "/dir1/dir2/dir3" pathToFile = "/dir1/dir2/dir3/file1.txt" result = "dir3/file1.txt"
						destinationUrl.Path = util.generateObjectPath(cleanContainerPath,
							util.getRelativePath(fileOrDirectoryPath, pathToFile, string(os.PathSeparator)))
						err = e.addTransfer(common.CopyTransfer{
							Source:           pathToFile,
							Destination:      destinationUrl.String(),
							LastModifiedTime: f.ModTime(),
							SourceSize:       f.Size(),
						}, wg, waitUntilJobCompletion)
						if err != nil {
							return err
						}
					}
					return nil
				})
			} else if !f.IsDir() {
				// files are uploaded using their file name as blob name
				destinationUrl.Path = util.generateObjectPath(cleanContainerPath, f.Name())
				err = e.addTransfer(common.CopyTransfer{
					Source:           fileOrDirectoryPath,
					Destination:      destinationUrl.String(),
					LastModifiedTime: f.ModTime(),
					SourceSize:       f.Size(),
				}, wg, waitUntilJobCompletion)
				if err != nil {
					return err
				}
			}
		}
	}

	if e.PartNum == 0 && len(e.Transfers) == 0 {
		return errors.New("nothing can be uploaded, please use --recursive to upload directories")
	}
	return e.dispatchFinalPart()
}
