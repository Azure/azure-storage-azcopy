package cmd

import (
	"context"
	"errors"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"net/url"
	"os"
	"path/filepath"
)

type copyUploadEnumerator common.CopyJobPartOrderRequest

// this function accepts the list of files/directories to transfer and processes them
func (e *copyUploadEnumerator) enumerate(cca *cookedCopyCmdArgs) error {
	util := copyHandlerUtil{}
	ctx := context.TODO() // Ensure correct context is used

	// attempt to parse the destination url
	destinationURL, err := url.Parse(cca.dst)
	if err != nil {
		// the destination should have already been validated, it would be surprising if it cannot be parsed at this point
		panic(err)
	}

	// list the source files and directories
	listOfFilesAndDirectories, err := filepath.Glob(cca.src)
	if err != nil || len(listOfFilesAndDirectories) == 0 {
		return fmt.Errorf("cannot find source to upload")
	}

	// when a single file is being uploaded, we need to treat this case differently, as the destinationURL might be a blob
	if len(listOfFilesAndDirectories) == 1 {
		f, err := os.Stat(listOfFilesAndDirectories[0])
		if err != nil {
			return errors.New("cannot find source to upload")
		}

		if !f.IsDir() {
			// Check if the files are passed with include flag
			// then source needs to be directory, if it is a file
			// then error is returned
			if len(e.Include) > 0 {
				return fmt.Errorf("for the use of include flag, source needs to be a directory")
			}
			// append file name as blob name in case the given URL is a container
			if (e.FromTo == common.EFromTo.LocalBlob() && util.urlIsContainerOrShare(destinationURL)) ||
				(e.FromTo == common.EFromTo.LocalFile() && util.urlIsAzureFileDirectory(ctx, destinationURL)) ||
				(e.FromTo == common.EFromTo.LocalBlobFS() && util.urlIsDFSFileSystemOrDirectory(ctx, destinationURL)) {
				destinationURL.Path = util.generateObjectPath(destinationURL.Path, f.Name())
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
			return e.dispatchFinalPart()
		}
	}
	// if the user specifies a virtual directory ex: /container_name/extra_path
	// then we should extra_path as a prefix while uploading
	// temporarily save the path of the container
	cleanContainerPath := destinationURL.Path

	// If the files have been explicitly mentioned in the Include flag
	// then we do not need to iterate through the entire directory
	// Iterate through the files or sub-dir in the include flag
	// and queue them for transfer
	if len(e.Include) > 0 {
		for file, _ := range e.Include {
			// append the file name in the include flag to the soure directory
			// For Example: cca.src = C:\User\new-User include = "file.txt;file2.txt"
			// currentFile = C:\User\new\User\file.txt
			currentFile := cca.src + string(os.PathSeparator) + file
			// temporary saving the destination Url to later modify it
			// to get the resource Url
			currentDestinationUrl := *destinationURL
			f, err := os.Stat(currentFile)
			if err != nil {
				return fmt.Errorf("invalid file name %s. It doesn't exists inside the directory %s", file, cca.src)
			}
			// When the string in include flag is a file
			// add it to the transfer list.
			// Example: currentFile = C:\User\new\User\file.txt
			if !f.IsDir() {
				currentDestinationUrl.Path = util.generateObjectPath(currentDestinationUrl.Path,
					util.getRelativePath(cca.src, currentFile, string(os.PathSeparator)))
				e.addTransfer(common.CopyTransfer{
					Source:           currentFile,
					Destination:      currentDestinationUrl.String(),
					LastModifiedTime: f.ModTime(),
					SourceSize:       f.Size(),
				}, cca)
			} else {
				// When the string in include flag is a sub-directory
				// Example: currentFile = C:\User\new\User\dir1
				if !cca.recursive {
					// If the recursive flag is not set to true
					// then Ignore the files inside sub-dir
					continue
				}
				// walk through each file in sub directory
				err = filepath.Walk(currentFile, func(pathToFile string, f os.FileInfo, err error) error {
					if err != nil {
						return err
					}
					if f.IsDir() {
						// For Blob and Azure Files, empty directories are not uploaded
						// For BlobFs, empty directories are to be uploaded as well
						// If the directory is not empty, then uploading a file inside the directory path
						// will create the parent directory of file, so transfer is not required to create
						// a directory
						// For Example: Dst := FSystem/dir1/a.txt If dir1 doesn't exists
						// TODO: Currently disabling the upload of empty directories
						//if e.FromTo == common.EFromTo.LocalBlobFS() && f.Size() == 0 {
						//	currentDestinationUrl.Path = util.generateObjectPath(cleanContainerPath,
						//		util.getRelativePath(cca.src, pathToFile, string(os.PathSeparator)))
						//	err = e.addTransfer(common.CopyTransfer{
						//		Source:           pathToFile,
						//		Destination:      currentDestinationUrl.String(),
						//		LastModifiedTime: f.ModTime(),
						//		SourceSize:       f.Size(),
						//	}, wg, waitUntilJobCompletion)
						//	if err != nil {
						//		return err
						//	}
						//}
						// If the file inside sub-dir is again a sub-dir
						// then skip it, since files inside sub-dir will be
						// considered by walk func
						return nil
					} else {
						// create the remote Url of file inside sub-dir
						currentDestinationUrl.Path = util.generateObjectPath(cleanContainerPath,
							util.getRelativePath(cca.src, pathToFile, string(os.PathSeparator)))
						err = e.addTransfer(common.CopyTransfer{
							Source:           pathToFile,
							Destination:      currentDestinationUrl.String(),
							LastModifiedTime: f.ModTime(),
							SourceSize:       f.Size(),
						}, cca)
						if err != nil {
							return err
						}
					}
					return nil
				})
				// TODO: Eventually permission error won't be returned and CopyTransfer will carry the error to the transfer engine.
				if err != nil {
					return err
				}
			}
		}
		// dispatch the final part
		e.dispatchFinalPart()
		return nil
	}

	// Iterate through each file mentioned in the exclude flag
	// Verify if the file exists inside the source directory or not.
	// Replace the file entry in the exclude map with entire path of file.
	if len(e.Exclude) > 0 {
		for file, index := range e.Exclude {
			var filePath = ""
			// If the source directory doesn't have a separator at the end
			// place a separator between the source and file
			if cca.src[len(cca.src)-1] != os.PathSeparator {
				filePath = fmt.Sprintf("%s%s%s", cca.src, string(os.PathSeparator), file)
			} else {
				filePath = fmt.Sprintf("%s%s", cca.src, file)
			}
			// Get the file info to verify file exists or not.
			f, err := os.Stat(filePath)
			if err != nil {
				return fmt.Errorf("file %s mentioned in the exclude doesn't exists inside the source dir %s", file, cca.src)
			}

			// If the file passed is a sub-directory inside the source directory
			// append '*' at the end of the path of sub-dir
			// '*' is added so that sub-dir path matches the path of all the files inside this sub-dir
			// while enumeration
			// For Example: Src = C:\\User\user-1 exclude = "dir1"
			// filePath = C:\User\user-1\dir1\*
			// filePath matches with Path of C:\User\user-1\dir1\a.txt; C:\User\user-1\dir1\b.txt
			if f.IsDir() {
				// If the filePath doesn't have a separator at the end
				// place a separator between filePath and '*'
				if filePath[len(filePath)-1] != os.PathSeparator {
					filePath = fmt.Sprintf("%s%s%s", filePath, string(os.PathSeparator), "*")
				} else {
					filePath = fmt.Sprintf("%s%s", filePath, "*")
				}
			}
			delete(e.Exclude, file)
			e.Exclude[filePath] = index
		}
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
				err = filepath.Walk(fileOrDirectoryPath, func(pathToFile string, f os.FileInfo, err error) error {
					if err != nil {
						return err
					}
					if f.IsDir() {
						// For Blob and Azure Files, empty directories are not uploaded
						// For BlobFs, empty directories are to be uploaded as well
						// If the directory is not empty, then uploading a file inside the directory path
						// will create the parent directory of file, so transfe is not required to create
						// a directory
						// For Example: Dst := FSystem/dir1/a.txt If dir1 doesn't exists
						// TODO: Currently disabling the upload of empty directories
						//if e.FromTo == common.EFromTo.LocalBlobFS() && f.Size() == 0 {
						//	destinationURL.Path = util.generateObjectPath(cleanContainerPath,
						//		util.getRelativePath(cca.src, pathToFile, string(os.PathSeparator)))
						//	err = e.addTransfer(common.CopyTransfer{
						//		Source:           pathToFile,
						//		Destination:      destinationURL.String(),
						//		LastModifiedTime: f.ModTime(),
						//		SourceSize:       f.Size(),
						//	}, wg, waitUntilJobCompletion)
						//	if err != nil {
						//		return err
						//	}
						//}
						// If the file inside sub-dir is again a sub-dir
						// then skip it, since files inside sub-dir will be
						// considered by walk func
						return nil
					} else {
						// Check if the file should be excluded or not.
						if util.resourceShouldBeExcluded(e.Exclude, pathToFile) {
							return nil
						}
						// upload the files
						// the path in the blob name started at the given fileOrDirectoryPath
						// example: fileOrDirectoryPath = "/dir1/dir2/dir3" pathToFile = "/dir1/dir2/dir3/file1.txt" result = "dir3/file1.txt"
						destinationURL.Path = util.generateObjectPath(cleanContainerPath,
							util.getRelativePath(fileOrDirectoryPath, pathToFile, string(os.PathSeparator)))
						err = e.addTransfer(common.CopyTransfer{
							Source:           pathToFile,
							Destination:      destinationURL.String(),
							LastModifiedTime: f.ModTime(),
							SourceSize:       f.Size(),
						}, cca)
						if err != nil {
							return err
						}
					}
					return nil
				})
			} else if !f.IsDir() {
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
	return e.dispatchFinalPart()
}

func (e *copyUploadEnumerator) addTransfer(transfer common.CopyTransfer, cca *cookedCopyCmdArgs) error {
	return addTransfer((*common.CopyJobPartOrderRequest)(e), transfer, cca)
}

func (e *copyUploadEnumerator) dispatchFinalPart() error {
	return dispatchFinalPart((*common.CopyJobPartOrderRequest)(e))
}

func (e *copyUploadEnumerator) partNum() common.PartNumber {
	return e.PartNum
}
