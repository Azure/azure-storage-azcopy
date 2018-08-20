package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	"strings"

	"github.com/Azure/azure-storage-azcopy/common"
)

type copyUploadEnumerator common.CopyJobPartOrderRequest

// this function accepts the list of files/directories to transfer and processes them
func (e *copyUploadEnumerator) enumerate(cca *cookedCopyCmdArgs) error {
	util := copyHandlerUtil{}
	ctx := context.TODO() // Ensure correct context is used

	// attempt to parse the destination url
	destinationURL, err := url.Parse(cca.destination)
	// the destination should have already been validated, it would be surprising if it cannot be parsed at this point
	common.PanicIfErr(err)

	// list the source files and directories
	listOfFilesAndDirectories, err := filepath.Glob(cca.source)
	if err != nil || len(listOfFilesAndDirectories) == 0 {
		return fmt.Errorf("cannot find source to upload")
	}

	// when a single file is being uploaded, we need to treat this case differently, as the destinationURL might be a blob
	if len(listOfFilesAndDirectories) == 1 {
		f, err := os.Stat(listOfFilesAndDirectories[0])
		if err != nil {
			return errors.New("cannot find source to upload")
		}

		if f.Mode().IsRegular() {
			// Check if the files are passed with include flag
			// then source needs to be directory, if it is a file
			// then error is returned
			if len(e.Include) > 0 {
				return fmt.Errorf("for the use of include flag, source needs to be a directory")
			}
			// append file name as blob name in case the given URL is a container
			if (e.FromTo == common.EFromTo.LocalBlob() && util.urlIsContainerOrShare(destinationURL)) ||
				(e.FromTo == common.EFromTo.LocalFile() && util.urlIsAzureFileDirectory(ctx, destinationURL)) {
				destinationURL.Path = util.generateObjectPath(destinationURL.Path, f.Name())
			}

			// append file name as blob name in case the given URL is a blob FS directory.
			if e.FromTo == common.EFromTo.LocalBlobFS() {
				// Create blob FS pipeline.
				p, err := createBlobFSPipeline(ctx, e.CredentialInfo)
				if err != nil {
					return err
				}

				if util.urlIsBFSFileSystemOrDirectory(ctx, destinationURL, p) {
					destinationURL.Path = util.generateObjectPath(destinationURL.Path, f.Name())
				}
			}

			err = e.addTransfer(common.CopyTransfer{
				Source:           listOfFilesAndDirectories[0],
				Destination:      destinationURL.String(),
				LastModifiedTime: f.ModTime(),
				SourceSize:       f.Size(),
			}, cca)

			if err != nil {
				return err
			}
			return e.dispatchFinalPart(cca)
		}
	}
	// if the user specifies a virtual directory ex: /container_name/extra_path
	// then we should extra_path as a prefix while uploading
	// temporarily save the path of the container
	cleanContainerPath := destinationURL.Path

	// Get the source path without the wildcards
	// This is defined since the files mentioned with exclude flag
	// & include flag are relative to the Source
	// If the source has wildcards, then files are relative to the
	// parent source path which is the path of last directory in the source
	// without wildcards
	// For Example: src = "/home/user/dir1" parentSourcePath = "/home/user/dir1"
	// For Example: src = "/home/user/dir*" parentSourcePath = "/home/user"
	// For Example: src = "/home/*" parentSourcePath = "/home"
	parentSourcePath := cca.source
	wcIndex := util.firstIndexOfWildCard(parentSourcePath)
	if wcIndex != -1 {
		parentSourcePath = parentSourcePath[:wcIndex]
		pathSepIndex := strings.LastIndex(parentSourcePath, common.AZCOPY_PATH_SEPARATOR_STRING)
		parentSourcePath = parentSourcePath[:pathSepIndex]
	}

	// walk through every file and directory
	// upload every file
	// upload directory recursively if recursive option is on
	for _, fileOrDirectoryPath := range listOfFilesAndDirectories {
		f, err := os.Stat(fileOrDirectoryPath)
		if err == nil {
			// directories are uploaded only if recursive is on
			if f.IsDir() && cca.recursive {
				// walk goes through the entire directory tree
				filepath.Walk(fileOrDirectoryPath, func(pathToFile string, f os.FileInfo, err error) error {
					if err != nil {
						glcm.Info(fmt.Sprintf("Accessing %s failed with error %s", pathToFile, err.Error()))
						return nil
					}
					if f.IsDir() {
						// For Blob and Azure Files, empty directories are not uploaded
						// For BlobFs, empty directories are to be uploaded as well
						// If the directory is not empty, then uploading a file inside the directory path
						// will create the parent directory of file, so transfer is not required to create
						// a directory
						// TODO: Currently not implemented the upload of empty directories for BlobFS
						return nil
					} else if f.Mode().IsRegular() { // If the resource is file
						// replace the OS path separator in pathToFile string with AZCOPY_PATH_SEPARATOR
						// this replacement is done to handle the windows file paths where path separator "\\"
						pathToFile = strings.Replace(pathToFile, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)

						// replace the OS path separator in fileOrDirectoryPath string with AZCOPY_PATH_SEPARATOR
						// this replacement is done to handle the windows file paths where path separator "\\"
						fileOrDirectoryPath = strings.Replace(fileOrDirectoryPath, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)

						// check if the should be included or not
						if !util.resourceShouldBeIncluded(parentSourcePath, e.Include, pathToFile) {
							return nil
						}
						// Check if the file should be excluded or not.
						if util.resourceShouldBeExcluded(parentSourcePath, e.Exclude, pathToFile) {
							return nil
						}
						// upload the files
						// the path in the blob name started at the given fileOrDirectoryPath
						// example: fileOrDirectoryPath = "/dir1/dir2/dir3" pathToFile = "/dir1/dir2/dir3/file1.txt" result = "dir3/file1.txt"
						destinationURL.Path = util.generateObjectPath(cleanContainerPath,
							util.getRelativePath(fileOrDirectoryPath, pathToFile))
						err = e.addTransfer(common.CopyTransfer{
							Source:           pathToFile,
							Destination:      destinationURL.String(),
							LastModifiedTime: f.ModTime(),
							SourceSize:       f.Size(),
						}, cca)
						if err != nil {
							return err
						}
					} else if f.Mode()&os.ModeSymlink != 0 {
						// If follow symlink is set to false, then symlinks are not evaluated.
						if !cca.followSymlinks {
							return nil
						}
						evaluatedSymlinkPath, err := filepath.EvalSymlinks(pathToFile)
						if err != nil {
							glcm.Info(fmt.Sprintf("error evaluating the symlink path %s", evaluatedSymlinkPath))
							return nil
						}
						// If the path is a windows file system path, replace '\\' with '/'
						// to maintain the consistency with other system paths.
						if common.AZCOPY_PATH_SEPARATOR_CHAR == '\\' {
							evaluatedSymlinkPath = strings.Replace(evaluatedSymlinkPath, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)
						}
						tList, errorList := util.getSymlinkTransferList(evaluatedSymlinkPath, fileOrDirectoryPath, parentSourcePath, cleanContainerPath, destinationURL, e.Include, e.Exclude)
						// Iterate though the list of all transfers and add it to the CopyJobPartOrder Request
						for _, tl := range tList {
							e.addTransfer(tl, cca)
						}
						// Iterate through all the errors occurred while traversing the symlinks and
						// put them into the lifecycle manager
						for _, err := range errorList {
							glcm.Info(err.Error())
						}
					}
					return nil
				})
			} else if f.Mode().IsRegular() {
				// replace the OS path separator in fileOrDirectoryPath string with AZCOPY_PATH_SEPARATOR
				// this replacement is done to handle the windows file paths where path separator "\\"
				fileOrDirectoryPath = strings.Replace(fileOrDirectoryPath, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)
				// check if the should be included or not
				if !util.resourceShouldBeIncluded(parentSourcePath, e.Include, fileOrDirectoryPath) {
					continue
				}
				// Check if the file should be excluded or not.
				if util.resourceShouldBeExcluded(parentSourcePath, e.Exclude, fileOrDirectoryPath) {
					continue
				}
				// files are uploaded using their file name as blob name
				destinationURL.Path = util.generateObjectPath(cleanContainerPath, f.Name())
				err = e.addTransfer(common.CopyTransfer{
					Source:           fileOrDirectoryPath,
					Destination:      destinationURL.String(),
					LastModifiedTime: f.ModTime(),
					SourceSize:       f.Size(),
				}, cca)
				if err != nil {
					return err
				}
			}
		}
	}

	if e.PartNum == 0 && len(e.Transfers) == 0 {
		return errors.New("nothing can be uploaded, please use --recursive to upload directories")
	}
	return e.dispatchFinalPart(cca)
}

func (e *copyUploadEnumerator) addTransfer(transfer common.CopyTransfer, cca *cookedCopyCmdArgs) error {
	return addTransfer((*common.CopyJobPartOrderRequest)(e), transfer, cca)
}

func (e *copyUploadEnumerator) dispatchFinalPart(cca *cookedCopyCmdArgs) error {
	return dispatchFinalPart((*common.CopyJobPartOrderRequest)(e), cca)
}
