package cmd

import (
	"errors"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"net/url"
	"os"
	"path/filepath"
)

type copyUploadEnumerator common.CopyJobPartOrderRequest

// accept a new transfer, if the threshold is reached, dispatch a job part order
func (e *copyUploadEnumerator) addTransfer(transfer common.CopyTransfer) error {
	e.Transfers = append(e.Transfers, transfer)

	// TODO move this to appropriate location
	//// if the transfer to be added is a page blob, we need to validate its file size
	//if enumerator.jpo.BlobAttributes.BlobType == common.PageBlob && transfer.SourceSize%512 != 0 {
	//	return fmt.Errorf("cannot perform upload for %s as a page blob because its size is not an exact multiple 512 bytes", transfer.Source)
	//}

	// dispatch the transfers once the number reaches NumOfFilesPerUploadJobPart
	// we do this so that in the case of large uploads, the transfer engine can get started
	// while the frontend is still gathering more transfers
	if len(e.Transfers) == NumOfFilesPerUploadJobPart {
		resp := common.CopyJobPartOrderResponse{}
		Rpc(common.ERpcCmd.CopyJobPartOrder(), e, &resp)

		if !resp.JobStarted {
			return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNum, resp.ErrorMsg)
		}

		e.Transfers = []common.CopyTransfer{}
		e.PartNum++
	}

	return nil
}

// we need to send a last part with isFinalPart set to true, along with whatever transfers that still haven't been sent
func (e *copyUploadEnumerator) dispatchFinalPart() error {
	e.IsFinalPart = true
	resp := common.CopyJobPartOrderResponse{}
	Rpc(common.ERpcCmd.CopyJobPartOrder(), e, &resp)

	if !resp.JobStarted {
		return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNum, resp.ErrorMsg)
	}

	return nil
}

// this function accepts the list of files/directories to transfer and processes them
func (e *copyUploadEnumerator) enumerate(src string, isRecursiveOn bool, dst string) error {
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
			if util.urlIsContainer(destinationUrl) {
				destinationUrl.Path = util.generateBlobPath(destinationUrl.Path, f.Name())
			}

			err = e.addTransfer(common.CopyTransfer{
				Source:           listOfFilesAndDirectories[0],
				Destination:      destinationUrl.String(),
				LastModifiedTime: f.ModTime(),
				SourceSize:       f.Size(),
			})

			if err != nil {
				return err
			}
			return e.dispatchFinalPart()
		}
	}

	// in any other case, the destination url must point to a container
	if !util.urlIsContainer(destinationUrl) {
		return errors.New("please provide a valid container URL as destination")
	}

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
						destinationUrl.Path = util.generateBlobPath(cleanContainerPath, util.getRelativePath(fileOrDirectoryPath, pathToFile))
						err = e.addTransfer(common.CopyTransfer{
							Source:           pathToFile,
							Destination:      destinationUrl.String(),
							LastModifiedTime: f.ModTime(),
							SourceSize:       f.Size(),
						})
						if err != nil {
							return err
						}
					}
					return nil
				})
			} else if !f.IsDir() {
				// files are uploaded using their file name as blob name
				destinationUrl.Path = util.generateBlobPath(cleanContainerPath, f.Name())
				err = e.addTransfer(common.CopyTransfer{
					Source:           fileOrDirectoryPath,
					Destination:      destinationUrl.String(),
					LastModifiedTime: f.ModTime(),
					SourceSize:       f.Size(),
				})
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
