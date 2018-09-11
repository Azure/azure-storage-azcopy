package cmd

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
	"github.com/Azure/azure-storage-blob-go/2018-03-28/azblob"
)

type syncUploadEnumerator common.SyncJobPartOrderRequest

// accepts a new transfer which is to delete the blob on container.
func (e *syncUploadEnumerator) addTransferToDelete(transfer common.CopyTransfer, cca *cookedSyncCmdArgs) error {
	// If the existing transfers in DeleteJobRequest is equal to NumOfFilesPerDispatchJobPart,
	// then send the JobPartOrder to transfer engine.
	if len(e.DeleteJobRequest.Transfers) == NumOfFilesPerDispatchJobPart {
		resp := common.CopyJobPartOrderResponse{}
		e.DeleteJobRequest.PartNum = e.PartNumber
		Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(&e.DeleteJobRequest), &resp)

		if !resp.JobStarted {
			return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNumber, resp.ErrorMsg)
		}
		// if the current part order sent to engine is 0, then start fetching the Job Progress summary.
		if e.PartNumber == 0 {
			cca.waitUntilJobCompletion(false)
		}
		e.DeleteJobRequest.Transfers = []common.CopyTransfer{}
		e.PartNumber++
	}
	e.DeleteJobRequest.Transfers = append(e.DeleteJobRequest.Transfers, transfer)
	return nil
}

// accept a new transfer, if the threshold is reached, dispatch a job part order
func (e *syncUploadEnumerator) addTransferToUpload(transfer common.CopyTransfer, cca *cookedSyncCmdArgs) error {

	if len(e.CopyJobRequest.Transfers) == NumOfFilesPerDispatchJobPart {
		resp := common.CopyJobPartOrderResponse{}
		e.CopyJobRequest.PartNum = e.PartNumber
		Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(&e.CopyJobRequest), &resp)

		if !resp.JobStarted {
			return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNumber, resp.ErrorMsg)
		}
		// if the current part order sent to engine is 0, then start fetching the Job Progress summary.
		if e.PartNumber == 0 {
			cca.waitUntilJobCompletion(false)
		}
		e.CopyJobRequest.Transfers = []common.CopyTransfer{}
		e.PartNumber++
	}
	e.CopyJobRequest.Transfers = append(e.CopyJobRequest.Transfers, transfer)
	return nil
}

// we need to send a last part with isFinalPart set to true, along with whatever transfers that still haven't been sent
func (e *syncUploadEnumerator) dispatchFinalPart(cca *cookedSyncCmdArgs) error {
	numberOfCopyTransfers := len(e.CopyJobRequest.Transfers)
	numberOfDeleteTransfers := len(e.DeleteJobRequest.Transfers)
	if numberOfCopyTransfers == 0 && numberOfDeleteTransfers == 0 {
		return fmt.Errorf("cannot start job because there are no transfer to upload or delete. " +
			"The source and destination are in sync")
	} else if numberOfCopyTransfers > 0 && numberOfDeleteTransfers > 0 {
		var resp common.CopyJobPartOrderResponse
		e.CopyJobRequest.PartNum = e.PartNumber
		Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(&e.CopyJobRequest), &resp)
		if !resp.JobStarted {
			return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNumber, resp.ErrorMsg)
		}
		e.PartNumber++
		e.DeleteJobRequest.IsFinalPart = true
		e.DeleteJobRequest.PartNum = e.PartNumber
		Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(&e.DeleteJobRequest), &resp)
		if !resp.JobStarted {
			return fmt.Errorf("delete job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNumber, resp.ErrorMsg)
		}
	} else if numberOfCopyTransfers > 0 {
		e.CopyJobRequest.IsFinalPart = true
		e.CopyJobRequest.PartNum = e.PartNumber
		var resp common.CopyJobPartOrderResponse
		Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(&e.CopyJobRequest), &resp)
		if !resp.JobStarted {
			return fmt.Errorf("copy job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNumber, resp.ErrorMsg)
		}
	} else {
		e.DeleteJobRequest.IsFinalPart = true
		e.DeleteJobRequest.PartNum = e.PartNumber
		var resp common.CopyJobPartOrderResponse
		Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(&e.DeleteJobRequest), &resp)
		if !resp.JobStarted {
			return fmt.Errorf("delete job part order with JobId %s and part number %d failed because %s", e.JobID, e.PartNumber, resp.ErrorMsg)
		}
	}
	cca.isEnumerationComplete = true
	return nil
}

// compareRemoteAgainstLocal api compares the blob at given destination Url and
// compare with blobs locally. If the blobs locally doesn't exists, then destination
// blobs are deleted.
func (e *syncUploadEnumerator) compareRemoteAgainstLocal(cca *cookedSyncCmdArgs, p pipeline.Pipeline) error {
	util := copyHandlerUtil{}

	ctx := context.WithValue(context.Background(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	// rootPath is the path of source without wildCards
	// sourcePattern is the filePath pattern inside the source
	// For Example: cca.source = C:\a1\a* , so rootPath = C:\a1 and filePattern is a*
	// This is to avoid enumerator to compare any file inside the destination directory
	// that doesn't match the pattern
	// For Example: cca.source = C:\a1\a* des = https://<container-name>?<sig>
	// Only files that follow pattern a* will be compared
	rootPath, sourcePattern := util.sourceRootPathWithoutWildCards(cca.source)
	//replace the os path separator  with path separator "/" which is path separator for blobs
	//sourcePattern = strings.Replace(sourcePattern, string(os.PathSeparator), "/", -1)
	destinationUrl, err := url.Parse(cca.destination)
	if err != nil {
		return fmt.Errorf("error parsing the destinatio url")
	}

	// since destination is a remote url, it will have sas parameter
	// since sas parameter will be stripped from the destination url
	// while cooking the raw command arguments
	// destination sas is added to url for listing the blobs.
	destinationUrl = util.appendQueryParamToUrl(destinationUrl, cca.destinationSAS)

	blobUrlParts := azblob.NewBlobURLParts(*destinationUrl) // TODO: remove and purely use extension
	blobURLPartsExtension := blobURLPartsExtension{blobUrlParts}

	containerUrl := util.getContainerUrl(blobUrlParts)
	searchPrefix, pattern, _ := blobURLPartsExtension.searchPrefixFromBlobURL()

	containerBlobUrl := azblob.NewContainerURL(containerUrl, p)

	// Get the destination path without the wildcards
	// This is defined since the files mentioned with exclude flag
	// & include flag are relative to the Destination
	// If the Destination has wildcards, then files are relative to the
	// parent Destination path which is the path of last directory in the Destination
	// without wildcards
	// For Example: dst = "/home/user/dir1" parentSourcePath = "/home/user/dir1"
	// For Example: dst = "/home/user/dir*" parentSourcePath = "/home/user"
	// For Example: dst = "/home/*" parentSourcePath = "/home"
	parentDestinationPath := blobUrlParts.BlobName
	wcIndex := util.firstIndexOfWildCard(parentDestinationPath)
	if wcIndex != -1 {
		parentDestinationPath = parentDestinationPath[:wcIndex]
		pathSepIndex := strings.LastIndex(parentDestinationPath, common.AZCOPY_PATH_SEPARATOR_STRING)
		parentDestinationPath = parentDestinationPath[:pathSepIndex]
	}

	for marker := (azblob.Marker{}); marker.NotDone(); {
		// look for all blobs that start with the prefix
		listBlob, err := containerBlobUrl.ListBlobsFlatSegment(ctx, marker,
			azblob.ListBlobsSegmentOptions{Prefix: searchPrefix})
		if err != nil {
			return fmt.Errorf("cannot list blobs for download. Failed with error %s", err.Error())
		}

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Segment.BlobItems {
			// If blob name doesn't match the pattern
			// This check supports the Use wild cards
			// SearchPrefix is used to list to all the blobs inside the destination
			// and pattern is used to identify which blob to compare further
			if !util.matchBlobNameAgainstPattern(pattern, blobInfo.Name, cca.recursive) {
				continue
			}

			if !util.resourceShouldBeIncluded(parentDestinationPath, e.Include, blobInfo.Name) {
				continue
			}

			if util.resourceShouldBeExcluded(parentDestinationPath, e.Exclude, blobInfo.Name) {
				continue
			}

			// realtivePathofBlobLocally is the local path relative to source at which blob should be downloaded
			// Example: cca.source ="C:\User1\user-1" cca.destination = "https://<container-name>/virtual-dir?<sig>" blob name = "virtual-dir/a.txt"
			// realtivePathofBlobLocally = virtual-dir/a.txt
			realtivePathofBlobLocally := util.relativePathToRoot(searchPrefix, blobInfo.Name, '/')

			// check if the listed blob segment matches the sourcePath pattern
			// if it does not comparison is not required
			if !util.blobNameMatchesThePattern(sourcePattern, realtivePathofBlobLocally) {
				continue
			}
			blobLocalPath := util.generateLocalPath(rootPath, realtivePathofBlobLocally)
			// Check if the blob exists locally or not
			_, err := os.Stat(blobLocalPath)
			if err == nil {
				continue
			}
			// if the blob doesn't exits locally, then we need to delete blob.
			if err != nil && os.IsNotExist(err) {
				// delete the blob.
				e.addTransferToDelete(common.CopyTransfer{
					Source:      util.stripSASFromBlobUrl(util.generateBlobUrl(containerUrl, blobInfo.Name)).String(),
					Destination: "", // no destination in case of Delete JobPartOrder
					SourceSize:  *blobInfo.Properties.ContentLength,
				}, cca)
			}
		}
		marker = listBlob.NextMarker
	}
	return nil
}

// checkAndQueue is an internal function which check the modified time of file locally
// and on container and then decideds whether to queue transfer for upload or not.
func (e *syncUploadEnumerator) checkAndQueue(ctx context.Context, p pipeline.Pipeline,
	 										blobUrlParts azblob.BlobURLParts, cca *cookedSyncCmdArgs,
											root string, pathToFile string, f os.FileInfo) error {

	util := copyHandlerUtil{}

	// If root path equals the pathToFile it means that source passed was a filePath
	// For Example: root = C:\a1\a2\f1.txt, pathToFile: C:\a1\a2\f1.txt
	// localFileRelativePath = f1.txt
	// remove the last component in the root path
	// root = C:\a1\a2
	if strings.EqualFold(root, pathToFile) {
		pathSepIndex := strings.LastIndex(root, common.AZCOPY_PATH_SEPARATOR_STRING)
		if pathSepIndex <= 0 {
			root = ""
		} else {
			root = root[:pathSepIndex]
		}
	}
	// localfileRelativePath is the path of file relative to root directory
	// Example1: root = C:\User\user1\dir-1  fileAbsolutePath = :\User\user1\dir-1\a.txt localfileRelativePath = \a.txt
	// Example2: root = C:\User\user1\dir-1  fileAbsolutePath = :\User\user1\dir-1\dir-2\a.txt localfileRelativePath = \dir-2\a.txt
	localfileRelativePath := strings.Replace(pathToFile, root, "", 1)
	// remove the path separator at the start of relative path
	if len(localfileRelativePath) > 0 && localfileRelativePath[0] == common.AZCOPY_PATH_SEPARATOR_CHAR {
		localfileRelativePath = localfileRelativePath[1:]
	}
	// Appending the fileRelativePath to the destinationUrl
	// root = C:\User\user1\dir-1  cca.destination = https://<container-name>/<vir-d>?<sig>
	// fileAbsolutePath = C:\User\user1\dir-1\dir-2\a.txt localfileRelativePath = \dir-2\a.txt
	// filedestinationUrl =  https://<container-name>/<vir-d>/dir-2/a.txt?<sig>
	filedestinationUrl, _ := util.appendBlobNameToUrl(blobUrlParts, localfileRelativePath)
	// Get the properties of given on container
	blobUrl := azblob.NewBlobURL(filedestinationUrl, p)
	blobProperties, err := blobUrl.GetProperties(ctx, azblob.BlobAccessConditions{})

	if err != nil {
		if stError, ok := err.(azblob.StorageError); !ok || (ok && stError.Response().StatusCode != http.StatusNotFound) {
			return fmt.Errorf("error sync up the blob %s because it failed to get the properties. Failed with error %s", localfileRelativePath, err.Error())
		}
	}
	// If the local file modified time was behind the remote
	// then sync is not required
	if err == nil && !f.ModTime().After(blobProperties.LastModified()) {
		return nil
	}
	err = e.addTransferToUpload(common.CopyTransfer{
		Source:           pathToFile,
		Destination:      util.stripSASFromBlobUrl(filedestinationUrl).String(),
		LastModifiedTime: f.ModTime(),
		SourceSize:       f.Size(),
	}, cca)
	if err != nil {
		return err
	}
	return nil
}

func (e *syncUploadEnumerator) compareLocalAgainstRemote(cca *cookedSyncCmdArgs, p pipeline.Pipeline) (error, bool) {

	ctx := context.WithValue(context.Background(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	util := copyHandlerUtil{}

	// attempt to parse the destination url
	destinationUrl, err := url.Parse(cca.destination)
	// the destination should have already been validated, it would be surprising if it cannot be parsed at this point
	common.PanicIfErr(err)

	// since destination is a remote url, it will have sas parameter
	// since sas parameter will be stripped from the destination url
	// while cooking the raw command arguments
	// destination sas is added to url for listing the blobs.
	destinationUrl = util.appendQueryParamToUrl(destinationUrl, cca.destinationSAS)

	blobUrl := azblob.NewBlobURL(*destinationUrl, p)
	// Get the files and directories for the given source pattern
	listOfFilesAndDir, lofaderr := filepath.Glob(cca.source)
	if lofaderr != nil {
		return fmt.Errorf("error getting the files and directories for source pattern %s", cca.source), false
	}
	// isSourceASingleFile is used to determine whether given source pattern represents single file or not
	// If the source is a single file, this pointer will not be nil
	// if it is nil, it means the source is a directory or list of file
	var isSourceASingleFile os.FileInfo = nil

	// If the number of files matching the given pattern is 1
	// determine the type of the source
	if len(listOfFilesAndDir) == 1 {
		lofadInfo, lofaderr := os.Stat(listOfFilesAndDir[0])
		if lofaderr != nil {
			return fmt.Errorf("error getting the file info the source %s", listOfFilesAndDir[0]), false
		}
		// If the given source represents a single file, then set isSourceASingleFile pointer to the fileInfo pointer
		if !lofadInfo.IsDir() {
			isSourceASingleFile = lofadInfo
		}
	}
	// Get the destination blob properties
	bProperties, berr := blobUrl.GetProperties(ctx, azblob.BlobAccessConditions{})

	// If the destination is an existing blob and the source is a directory
	// sync cannot happen between an existing blob and a local directory
	if berr == nil && isSourceASingleFile != nil {
		return fmt.Errorf("cannot perform the sync since source %s "+
			"is a directory and destination %s is a blob", cca.source, destinationUrl.String()), false
	}
	// If the source is a file and destination is a blob
	// For Example: "cca.source = C:\User\user-1\a.txt" && "cca.destination = https://<container-name>/vd-1/a.txt"
	if berr == nil && isSourceASingleFile != nil {
		// Get the blob name from the destination url
		// blobName refers to the last name of the blob with which it is stored as file locally
		// Example1: "cca.destination = https://<container-name>/blob1?<sig>  blobName = blob1"
		// Example1: "cca.destination = https://<container-name>/dir1/blob1?<sig>  blobName = blob1"
		blobName := destinationUrl.Path[strings.LastIndex(destinationUrl.Path, "/")+1:]
		// Compare the blob name and file name
		// blobName and filename should be same for sync to happen
		if strings.Compare(blobName, isSourceASingleFile.Name()) != 0 {
			return fmt.Errorf("sync cannot be done since blob %s and filename %s doesn't match", blobName, isSourceASingleFile.Name()), true
		}
		// If the modified time of file local is later than that of blob
		// sync needs to happen. The transfer is queued
		if isSourceASingleFile.ModTime().After(bProperties.LastModified()) {
			e.addTransferToUpload(common.CopyTransfer{
				Source:           cca.source,
				Destination:      util.stripSASFromBlobUrl(*destinationUrl).String(),
				SourceSize:       isSourceASingleFile.Size(),
				LastModifiedTime: isSourceASingleFile.ModTime(),
			}, cca)
		}
		return nil, true
	}

	blobUrlParts := azblob.NewBlobURLParts(*destinationUrl)
	// If the source is a file and destination is not a blob, it could be a container or directory
	// then compare the file against the possible blob. If file doesn't exists as a blob upload it
	// it it exists then compare it.
	if isSourceASingleFile != nil && berr != nil {
		filedestinationUrl, _ := util.appendBlobNameToUrl(blobUrlParts, isSourceASingleFile.Name())
		blobUrl := azblob.NewBlobURL(filedestinationUrl, p)
		bProperties, err := blobUrl.GetProperties(ctx, azblob.BlobAccessConditions{})
		// If err is not nil, it means the blob does not exists
		if err != nil {
			if stError, ok := err.(azblob.StorageError); !ok || (ok && stError.Response().StatusCode != http.StatusNotFound) {
				return fmt.Errorf("error sync up the blob %s because it failed to get the properties. Failed with error %s", filedestinationUrl.String(), err.Error()), true
			}
		}
		if err == nil && !isSourceASingleFile.ModTime().After(bProperties.LastModified()) {
			return fmt.Errorf("sync is not required since the source %s modified time is before the destinaton %s modified time ", cca.source, filedestinationUrl.String()), true
		}
		e.addTransferToUpload(common.CopyTransfer{
			Source:           cca.source,
			Destination:      util.stripSASFromBlobUrl(filedestinationUrl).String(),
			LastModifiedTime: isSourceASingleFile.ModTime(),
			SourceSize:       isSourceASingleFile.Size(),
		}, cca)
		return nil, true
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
	parentSourcePath, _ := util.sourceRootPathWithoutWildCards(cca.source)
	//parentSourcePath := cca.source
	//wcIndex := util.firstIndexOfWildCard(parentSourcePath)
	//if wcIndex != -1 {
	//	parentSourcePath = parentSourcePath[:wcIndex]
	//	pathSepIndex := strings.LastIndex(parentSourcePath, common.AZCOPY_PATH_SEPARATOR_STRING)
	//	parentSourcePath = parentSourcePath[:pathSepIndex]
	//}

	// rootPath will be the parent source directory before the first wildcard
	// For Example: cca.source = C:\a\b* rootPath = C:\a
	// For Example: cca.source = C:\*\a* rootPath = c:\
	// In case of no wildCard, rootPath is equal to the source directory
	// This rootPath is effective when wildCards are provided
	// Using this rootPath, path of file on blob is calculated
	// for ex: cca.source := C:\a*\f*.txt rootPath = C:\
	// path of file C:\a1\f1.txt on the destination path will be destination/a1/f1.txt
	// TODO: rootPath and parentSourcePath (above) is likely to hold the same value.
	// TODO: commenting the root path for now and will delete after the tests are passed.
	//rootPath, _ := util.sourceRootPathWithoutWildCards(cca.source)
	// Iterate through each file / dir inside the source
	// and then checkAndQueue
	for _, fileOrDir := range listOfFilesAndDir {
		f, err := os.Stat(fileOrDir)
		if err == nil {
			// directories are uploaded only if recursive is on
			if f.IsDir() && cca.recursive {
				// walk goes through the entire directory tree
				filepath.Walk(fileOrDir, func(pathToFile string, f os.FileInfo, err error) error {
					if err != nil {
						glcm.Info(err.Error())
						return nil
					}
					if f.IsDir() {
						return nil
					} else if f.Mode().IsRegular(){
						// replace the OS path separator in pathToFile string with AZCOPY_PATH_SEPARATOR
						// this replacement is done to handle the windows file paths where path separator "\\"
						pathToFile = strings.Replace(pathToFile, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)
						if !util.resourceShouldBeIncluded(parentSourcePath, e.Include, pathToFile) {
							return nil
						}

						if util.resourceShouldBeExcluded(parentSourcePath, e.Exclude, pathToFile) {
							return nil
						}
						err = e.checkAndQueue(ctx, p, blobUrlParts, cca, parentSourcePath, pathToFile, f)
						if err != nil {
							glcm.Info(err.Error())
						}
						return nil
					}else if f.Mode() & os.ModeSymlink != 0 {
						//If follow symlink is set to false, then symlinks are not evaluated.
						if !cca.followSymlinks {
							return nil
						}

						evaluatedSymlinkPath, err := util.evaluateSymlinkPath(pathToFile)
						if err != nil {
							glcm.Info(fmt.Sprintf("error evaluating the symlink path %s", evaluatedSymlinkPath))
							return nil
						}
						// If the path is a windows file system path, replace '\\' with '/'
						// to maintain the consistency with other system paths.
						if os.PathSeparator == '\\' {
							evaluatedSymlinkPath = strings.Replace(evaluatedSymlinkPath, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)
						}
						e.getSymlinkTransferList(ctx, p, blobUrlParts, cca, evaluatedSymlinkPath, pathToFile, parentSourcePath)
					}
					return nil
				})
			} else if f.Mode().IsRegular() {
				// replace the OS path separator in fileOrDir string with AZCOPY_PATH_SEPARATOR
				// this replacement is done to handle the windows file paths where path separator "\\"
				fileOrDir = strings.Replace(fileOrDir, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)
				if !util.resourceShouldBeIncluded(parentSourcePath, e.Include, fileOrDir) {
					continue
				}

				if util.resourceShouldBeExcluded(parentSourcePath, e.Exclude, fileOrDir) {
					continue
				}
				err = e.checkAndQueue(ctx, p, blobUrlParts, cca, parentSourcePath, fileOrDir, f)
				if err != nil {
					glcm.Info(err.Error())
				}
			}else{
				continue
			}
		}else{
			glcm.Info(fmt.Sprintf("error %s accessing the filepath %s", err.Error(), fileOrDir))
		}
	}
	return nil, false
}

func (e *syncUploadEnumerator) getSymlinkTransferList(ctx context.Context, p pipeline.Pipeline,
							blobUrlParts azblob.BlobURLParts, cca *cookedSyncCmdArgs,
							symlinkPath, source, parentSource string) {

	util := copyHandlerUtil{}
	// replace the "\\" path separator with "/" separator
	symlinkPath = strings.Replace(symlinkPath, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)

	listOfFilesDirs, err := filepath.Glob(symlinkPath)
	if err != nil {
		glcm.Info(fmt.Sprintf("found cycle in symlink path %s", symlinkPath))
		return
	}
	for _, files := range listOfFilesDirs {
		// replace the windows path separator in the path with "/" path separator
		files = strings.Replace(files, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)
		fInfo, err := os.Stat(files)
		if err != nil {
			glcm.Info(err.Error())
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
					path := strings.Replace(path, symlinkPath, "", 1)

					if len(path) > 0  && path[0] == common.AZCOPY_PATH_SEPARATOR_CHAR {
						path = path[1:]
					}

					var sourcePath = ""
					// concatenate the relative symlink path to the original source
					if len(source) > 0 && source[len(source)-1] == common.AZCOPY_PATH_SEPARATOR_CHAR {
						sourcePath = fmt.Sprintf("%s%s", source, path)
					} else {
						sourcePath = fmt.Sprintf("%s%s%s", source, common.AZCOPY_PATH_SEPARATOR_STRING, path)
					}

					// check if the sourcePath needs to be include or not
					if !util.resourceShouldBeIncluded(parentSource, e.Include, sourcePath) {
						return nil
					}
					// check if the source has to be excluded or not
					if util.resourceShouldBeExcluded(parentSource, e.Exclude, sourcePath) {
						return nil
					}
					err = e.checkAndQueue(ctx, p, blobUrlParts, cca, parentSource, sourcePath, fileInfo)
					if err != nil {
						glcm.Info(err.Error())
					}
					return nil
				} else if fileInfo.Mode()&os.ModeSymlink != 0 { // If the file is a symlink
					// replace the windows path separator in the path with "/" path separator
					path = strings.Replace(path, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)
					// Evaulate the symlink path
					sLinkPath, err := util.evaluateSymlinkPath(path)
					if err != nil {
						glcm.Info(err.Error())
						return nil
					}
					// strip the original symlink path and concatenate the relativePath to the original sourcePath
					// for Example: source = C:\MountedD sLinkPath = D:\MountedE
					// relativePath = MountedE , sourcePath = C;\MountedD\MountedE
					relativePath := strings.Replace(path, symlinkPath, "", 1)
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
					e.getSymlinkTransferList(ctx, p, blobUrlParts, cca, sLinkPath, sourcePath, parentSource)
				}
				return nil
			})
		} else if fInfo.Mode().IsRegular() {
			// replace the windows path separator in the path with "/" path separator
			files = strings.Replace(files, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)
			// strip the original symlink path from the filePath
			// For Example: C:\MountedD points to D:\ and path is D:\file1
			// relativePath = file1
			files := strings.Replace(files, symlinkPath, "", 1)

			if len(files) > 0  && files[0] == common.AZCOPY_PATH_SEPARATOR_CHAR {
				files = files[1:]
			}

			var sourcePath = ""
			// concatenate the relative symlink path to the original source
			if len(source) > 0 && source[len(source)-1] == common.AZCOPY_PATH_SEPARATOR_CHAR {
				sourcePath = fmt.Sprintf("%s%s", source, files)
			} else {
				sourcePath = fmt.Sprintf("%s%s%s", source, common.AZCOPY_PATH_SEPARATOR_STRING, files)
			}

			// check if the sourcePath needs to be include or not
			if !util.resourceShouldBeIncluded(parentSource, e.Include, sourcePath) {
				continue
			}
			// check if the source has to be excluded or not
			if util.resourceShouldBeExcluded(parentSource, e.Exclude, sourcePath) {
				continue
			}

			e.checkAndQueue(ctx, p, blobUrlParts, cca, sourcePath, files, fInfo)
		} else {
			continue
		}
	}
}

// this function accepts the list of files/directories to transfer and processes them
func (e *syncUploadEnumerator) enumerate(cca *cookedSyncCmdArgs) error {
	// Create the new azblob pipeline
	p := ste.NewBlobPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{
		Telemetry: azblob.TelemetryOptions{
			Value: common.UserAgent,
		},
	},
		ste.XferRetryOptions{
			Policy:        0,
			MaxTries:      ste.UploadMaxTries,
			TryTimeout:    ste.UploadTryTimeout,
			RetryDelay:    ste.UploadRetryDelay,
			MaxRetryDelay: ste.UploadMaxRetryDelay},
		nil)

	// Copying the JobId of sync job to individual copyJobRequest
	e.CopyJobRequest.JobID = e.JobID
	// Copying the FromTo of sync job to individual copyJobRequest
	e.CopyJobRequest.FromTo = e.FromTo

	// set the sas of user given Source
	e.CopyJobRequest.SourceSAS = e.SourceSAS

	// set the sas of user given destination
	e.CopyJobRequest.DestinationSAS = e.DestinationSAS

	// Copying the JobId of sync job to individual deleteJobRequest.
	e.DeleteJobRequest.JobID = e.JobID
	// FromTo of DeleteJobRequest will be BlobTrash.
	e.DeleteJobRequest.FromTo = common.EFromTo.BlobTrash()

	// For delete the source is the destination in case of sync upload
	// For Example: source = /home/user destination = https://container/vd-1?<sig>
	// For deleting the blobs, Source in Delete Job Source will be the blob url
	// and source sas is the destination sas which is url sas.
	// set the destination sas as the source sas
	e.DeleteJobRequest.SourceSAS = e.DestinationSAS

	// Set the Log Level
	e.CopyJobRequest.LogLevel = e.LogLevel
	e.DeleteJobRequest.LogLevel = e.LogLevel

	// Set the force flag to true
	e.CopyJobRequest.ForceWrite = true

	// Copy the sync Command String to the CopyJobPartRequest and DeleteJobRequest
	e.CopyJobRequest.CommandString = e.CommandString
	e.DeleteJobRequest.CommandString = e.CommandString

	err, isSourceAFile := e.compareLocalAgainstRemote(cca, p)
	if err != nil {
		return err
	}
	// isSourceAFile defines whether source is a file or not.
	// If source is a file and destination is a blob, then destination doesn't needs to be compared against local.
	if !isSourceAFile {
		err = e.compareRemoteAgainstLocal(cca, p)
		if err != nil {
			return err
		}
	}
	// No Job Part has been dispatched, then dispatch the JobPart.
	if e.PartNumber == 0 ||
		len(e.CopyJobRequest.Transfers) > 0 ||
		len(e.DeleteJobRequest.Transfers) > 0 {
		err = e.dispatchFinalPart(cca)
		if err != nil {
			return err
		}
		cca.waitUntilJobCompletion(true)
	}
	return nil
}
