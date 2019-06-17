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
	e.SourceRoot = common.ToExtendedPath(e.SourceRoot)
	util := copyHandlerUtil{}
	ctx := context.TODO() // Ensure correct context is used

	// attempt to parse the destination url
	destinationURL, err := url.Parse(cca.destination)
	// the destination should have already been validated, it would be surprising if it cannot be parsed at this point
	common.PanicIfErr(err)

	// list the source files and directories
	listOfFilesAndDirectories, err := filepath.Glob(common.ToExtendedPath(cca.source))
	if err != nil || len(listOfFilesAndDirectories) == 0 {
		return fmt.Errorf("cannot find source to upload")
	}

	// when a single file is being uploaded, we need to treat this case differently, as the destinationURL might be a blob
	if len(listOfFilesAndDirectories) == 1 {
		f, err := os.Stat(common.ToExtendedPath(listOfFilesAndDirectories[0]))
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

			cleanedSource := listOfFilesAndDirectories[0]
			if !strings.HasPrefix(listOfFilesAndDirectories[0], `\\?\`) {
				cleanedSource = strings.Replace(listOfFilesAndDirectories[0], common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)
			}
			err = e.addTransfer(common.CopyTransfer{
				Source:           cleanedSource,
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

	// If the user has provided the listofFiles explicitly to copy, there is no
	// need to glob the source and match the patterns.
	// This feature is supported only for Storage Explorer and doesn't follow the symlinks.
	if len(cca.listOfFilesToCopy) > 0 {
		for _, file := range cca.listOfFilesToCopy {
			tempDestinationURl := *destinationURL
			parentSourcePath, _ := util.getRootPathWithoutWildCards(cca.source)
			if len(parentSourcePath) > 0 && parentSourcePath[len(parentSourcePath)-1] == common.AZCOPY_PATH_SEPARATOR_CHAR {
				parentSourcePath = parentSourcePath[:len(parentSourcePath)-1]
			}
			filePath := fmt.Sprintf("%s%s%s", parentSourcePath, common.AZCOPY_PATH_SEPARATOR_STRING, file)
			f, err := os.Stat(common.ToExtendedPath(filePath))
			if err != nil {
				glcm.Info(fmt.Sprintf("Error getting the fileInfo for file %s. failed with error %s", filePath, err.Error()))
				continue
			}
			if f.Mode().IsRegular() {
				// replace the parent source path in the filePath to to ensure the correct path mentioned in the list of flags.
				// For Example: Source = /home/user/dir list-of-files = dir2/1.txt;dir2/2.txt dst = https://container/dir2/1.txt
				// and https://container/dir2/2.txt
				relativePath := strings.Replace(filePath, parentSourcePath, "", 1)
				if len(relativePath) > 0 && relativePath[0] == common.AZCOPY_PATH_SEPARATOR_CHAR {
					relativePath = relativePath[1:]
				}
				// If the file is a regular file, calculate the destination path and queue for transfer.
				tempDestinationURl.Path = util.generateObjectPath(tempDestinationURl.Path, relativePath)
				err = e.addTransfer(common.CopyTransfer{
					Source:           filePath,
					Destination:      tempDestinationURl.String(),
					LastModifiedTime: f.ModTime(),
					SourceSize:       f.Size(),
				}, cca)

				if err != nil {
					glcm.Info(fmt.Sprintf("error %s adding source %s and destination %s as a transfer", err.Error(), filePath, destinationURL))
				}
				continue
			}
			// If the last character of the filePath is a path separator, strip the path separator.
			if len(filePath) > 0 && filePath[len(filePath)-1] == common.AZCOPY_PATH_SEPARATOR_CHAR {
				filePath = filePath[:len(filePath)-1]
			}
			if f.IsDir() && cca.recursive {
				// If the file is a directory, walk through all the elements inside the directory and queue the elements for transfer.
				filepath.Walk(filePath, func(pathToFile string, info os.FileInfo, err error) error {
					if err != nil {
						glcm.Info(fmt.Sprintf("Accessing %s failed with error %s", pathToFile, err.Error()))
						return nil
					}
					if info.IsDir() {
						return nil
					} else if info.Mode().IsRegular() { // If the resource is file
						// replace the OS path separator in pathToFile string with AZCOPY_PATH_SEPARATOR
						// this replacement is done to handle the windows file paths where path separator "\\"
						pathToFile = strings.Replace(pathToFile, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)

						// replace the OS path separator in fileOrDirectoryPath string with AZCOPY_PATH_SEPARATOR
						// this replacement is done to handle the windows file paths where path separator "\\"
						filePath = strings.Replace(filePath, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)

						// upload the files
						// the path in the blob name started at the given fileOrDirectoryPath
						// example: fileOrDirectoryPath = "/dir1/dir2/dir3" pathToFile = "/dir1/dir2/dir3/file1.txt" result = "dir3/file1.txt"
						tempDestinationURl.Path = util.generateObjectPath(cleanContainerPath,
							util.getRelativePath(filePath, pathToFile))
						err = e.addTransfer(common.CopyTransfer{
							Source:           pathToFile,
							Destination:      tempDestinationURl.String(),
							LastModifiedTime: info.ModTime(),
							SourceSize:       info.Size(),
						}, cca)
						if err != nil {
							return err
						}
					}
					return nil
				})
			}
		}
		if e.PartNum == 0 && len(e.Transfers) == 0 {
			return errors.New("nothing can be uploaded, please use --recursive to upload directories")
		}
		return e.dispatchFinalPart(cca)
	}

	// Get the source path without the wildcards
	// This is defined since the files mentioned with exclude flag
	// & include flag are relative to the Source
	// If the source has wildcards, then files are relative to the
	// parent source path which is the path of last directory in the source
	// without wildcards
	// For Example: src = "/home/user/dir1" parentSourcePath = "/home/user/dir1"
	// For Example: src = "/home/user/dir*" parentSourcePath = "/home/user"
	// For Example: src = "/home/*" parentSourcePath = "/home"
	parentSourcePath := common.ToExtendedPath(cca.source)
	wcIndex := util.firstIndexOfWildCard(parentSourcePath)
	if wcIndex != -1 {
		parentSourcePath = parentSourcePath[:wcIndex]
		pathSepIndex := strings.LastIndex(parentSourcePath, common.OS_PATH_SEPARATOR)
		if len(parentSourcePath) != 0 {
			parentSourcePath = parentSourcePath[:pathSepIndex]
		} else {
			parentSourcePath, err = os.Getwd()
			if err != nil {
				return err
			}
		}
	}

	// walk through every file and directory
	// upload every file
	// upload directory recursively if recursive option is on
	for _, fileOrDirectoryPath := range listOfFilesAndDirectories {
		f, err := os.Stat(common.ToExtendedPath(fileOrDirectoryPath))
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
						if !strings.HasPrefix(pathToFile, `\\?\`) {
							pathToFile = strings.Replace(pathToFile, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)
						}

						// replace the OS path separator in fileOrDirectoryPath string with AZCOPY_PATH_SEPARATOR
						// this replacement is done to handle the windows file paths where path separator "\\"
						if !strings.HasPrefix(fileOrDirectoryPath, `\\?\`) {
							fileOrDirectoryPath = strings.Replace(fileOrDirectoryPath, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)
						}

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
						// evaluate the symlinkPath.
						evaluatedSymlinkPath, err := util.evaluateSymlinkPath(pathToFile)
						if err != nil {
							glcm.Info(fmt.Sprintf("error evaluating the symlink path %s", evaluatedSymlinkPath))
							return nil
						}
						// If the path is a windows file system path, replace '\\' with '/'
						// to maintain the consistency with other system paths.
						if common.OS_PATH_SEPARATOR == "\\" {
							pathToFile = strings.Replace(pathToFile, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)
						}
						err = e.getSymlinkTransferList(evaluatedSymlinkPath, pathToFile, parentSourcePath, cleanContainerPath, destinationURL, cca)
						if err != nil {
							glcm.Info(fmt.Sprintf("error %s evaluating the symlinkPath %s", err.Error(), evaluatedSymlinkPath))
						}
					}
					return nil
				})
			} else if f.Mode().IsRegular() {
				// replace the OS path separator in fileOrDirectoryPath string with AZCOPY_PATH_SEPARATOR
				// this replacement is done to handle the windows file paths where path separator "\\"
				if !strings.HasPrefix(fileOrDirectoryPath, `\\?\`) {
					fileOrDirectoryPath = strings.Replace(fileOrDirectoryPath, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)
				}
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
		} else {
			glcm.Info(fmt.Sprintf("error %s accessing the filepath %s", err.Error(), fileOrDirectoryPath))
		}
	}

	if e.PartNum == 0 && len(e.Transfers) == 0 {
		return errors.New("nothing can be uploaded, please use --recursive to upload directories")
	}
	return e.dispatchFinalPart(cca)
}

// getSymlinkTransferList api scans all the elements inside the symlinkPath and enumerates the transfers.
// If there exists a symlink in the given symlinkPath, it recursively scans it and enumerate the transfer.
// The path of the files in the symlinkPath will be relative to the original path.
// Example 1: C:\MountedD is a symlink to D: and D: contains file1, file2.
// The destination for file1, file2 remotely will be MountedD/file1, MountedD/file2.
// Example 2. If there exists a symlink inside the D: "D:\MountecF" pointing to F: and there exists
// ffile1, ffile2, then destination for ffile1, ffile2 remotely will be MountedD/MountedF/ffile1 and
// MountedD/MountedF/ffile2
func (e *copyUploadEnumerator) getSymlinkTransferList(symlinkPath, source, parentSource, cleanContainerPath string,
	destinationUrl *url.URL, cca *cookedCopyCmdArgs) error {

	util := copyHandlerUtil{}
	// replace the "\\" path separator with "/" separator
	symlinkPath = strings.Replace(symlinkPath, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)

	// Glob the evaluated symlinkPath and iterate through each files and sub-directories.
	listOfFilesDirs, err := filepath.Glob(symlinkPath)
	if err != nil {
		return fmt.Errorf(fmt.Sprintf("found cycle in symlink path %s", symlinkPath))
	}
	for _, files := range listOfFilesDirs {
		// replace the windows path separator in the path with "/" path separator
		files = strings.Replace(files, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)
		fInfo, err := os.Stat(common.ToExtendedPath(files))
		if err != nil {
			glcm.Info(fmt.Sprintf("error %s fetching the fileInfo for filePath %s", err.Error(), files))
		} else if fInfo.IsDir() {
			filepath.Walk(files, func(path string, fileInfo os.FileInfo, err error) error {
				if err != nil {
					glcm.Info(err.Error())
					return nil
				} else if fileInfo.IsDir() {
					return nil
				} else if fileInfo.Mode().IsRegular() { // If the file is a regular file i.e not a directory and symlink.
					// replace the windows path separator in the path with "/" path separator
					path = strings.Replace(path, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)
					// strip the original symlink path from the filePath
					// For Example: C:\MountedD points to D:\ and path is D:\file1
					// relativePath = file1
					relativePath := strings.Replace(path, symlinkPath, "", 1)
					// If there exists a path separator at the start of the relative path, then remove the path separator
					if len(relativePath) > 0 && relativePath[0] == common.AZCOPY_PATH_SEPARATOR_CHAR {
						relativePath = relativePath[1:]
					}
					var sourcePath = ""
					// concatenate the relative symlink path to the original source path
					// For Example: C:\MountedD points to D:\ and path is D:\file1
					// sourcePath = c:\MounteD\file1
					if len(source) > 0 && source[len(source)-1] == common.AZCOPY_PATH_SEPARATOR_CHAR {
						sourcePath = fmt.Sprintf("%s%s", source, relativePath)
					} else {
						sourcePath = fmt.Sprintf("%s%s%s", source, common.AZCOPY_PATH_SEPARATOR_STRING, relativePath)
					}

					// check if the sourcePath needs to be include or not
					if !util.resourceShouldBeIncluded(parentSource, e.Include, sourcePath) {
						return nil
					}
					// check if the source has to be excluded or not
					if util.resourceShouldBeExcluded(parentSource, e.Exclude, sourcePath) {
						return nil
					}
					// create the transfer and add to the list
					destinationUrl.Path = util.generateObjectPath(cleanContainerPath,
						util.getRelativePath(parentSource, sourcePath))
					transfer := common.CopyTransfer{
						Source:           path,
						Destination:      destinationUrl.String(),
						LastModifiedTime: fileInfo.ModTime(),
						SourceSize:       fileInfo.Size(),
					}
					err = e.addTransfer(transfer, cca)
					if err != nil {
						glcm.Info(fmt.Sprintf("error %s adding the transfer source %s and destination %s", err.Error(), path, destinationUrl.String()))
					}
					return nil
				} else if fileInfo.Mode()&os.ModeSymlink != 0 { // If the file is a symlink
					// replace the windows path separator in the path with "/" path separator
					path = strings.Replace(path, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)
					// Evaulate the symlink path
					sLinkPath, err := util.evaluateSymlinkPath(path)
					if err != nil {
						glcm.Info(fmt.Sprintf("error %s evaluating the symlink path %s ", err.Error(), path))
						return nil
					}
					// strip the original symlink path and concatenate the relativePath to the original sourcePath
					// for Example: source = C:\MountedD sLinkPath = D:\MountedE
					// relativePath = MountedE , sourcePath = C;\MountedD\MountedE
					relativePath := strings.Replace(path, symlinkPath, "", 1)
					// If char of relative Path is the path separator, strip the path separator
					if len(relativePath) > 0 && relativePath[0] == common.AZCOPY_PATH_SEPARATOR_CHAR {
						relativePath = relativePath[1:]
					}
					var sourcePath = ""
					// concatenate the relative symlink path to the original source
					if len(source) > 0 && source[len(source)-1] == common.AZCOPY_PATH_SEPARATOR_CHAR {
						sourcePath = fmt.Sprintf("%s%s", source, relativePath)
					} else {
						sourcePath = fmt.Sprintf("%s%s%s", source, common.AZCOPY_PATH_SEPARATOR_STRING, relativePath)
					}
					err = e.getSymlinkTransferList(sLinkPath, sourcePath,
						parentSource, cleanContainerPath, destinationUrl, cca)
					if err != nil {
						glcm.Info(fmt.Sprintf("error %s iterating through the symlink %s", err.Error(), sLinkPath))
					}
				}
				return nil
			})
		} else if fInfo.Mode().IsRegular() {
			// strip the original symlink path
			relativePath := strings.Replace(files, symlinkPath, "", 1)

			// concatenate the path to the parent source
			var sourcePath = ""
			if len(source) > 0 && source[len(source)-1] == common.AZCOPY_PATH_SEPARATOR_CHAR {
				sourcePath = fmt.Sprintf("%s%s", source, relativePath)
			} else {
				sourcePath = fmt.Sprintf("%s%s%s", source, common.AZCOPY_PATH_SEPARATOR_STRING, relativePath)
			}

			// check if the sourcePath needs to be include or not
			if !util.resourceShouldBeIncluded(parentSource, e.Include, sourcePath) {
				continue
			}
			// check if the source has to be excluded or not
			if util.resourceShouldBeExcluded(parentSource, e.Exclude, sourcePath) {
				continue
			}

			// create the transfer and add to the list
			destinationUrl.Path = util.generateObjectPath(cleanContainerPath,
				util.getRelativePath(source, sourcePath))
			transfer := common.CopyTransfer{
				Source:           files,
				Destination:      destinationUrl.String(),
				LastModifiedTime: fInfo.ModTime(),
				SourceSize:       fInfo.Size(),
			}
			err = e.addTransfer(transfer, cca)
			if err != nil {
				glcm.Info(fmt.Sprintf("error %s adding the transfer source %s and destination %s", err.Error(), files, destinationUrl.String()))
			}
		} else {
			continue
		}
	}
	return nil
}

func (e *copyUploadEnumerator) addTransfer(transfer common.CopyTransfer, cca *cookedCopyCmdArgs) error {
	return addTransfer((*common.CopyJobPartOrderRequest)(e), transfer, cca)
}

func (e *copyUploadEnumerator) dispatchFinalPart(cca *cookedCopyCmdArgs) error {
	return dispatchFinalPart((*common.CopyJobPartOrderRequest)(e), cca)
}
