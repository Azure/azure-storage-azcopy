package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"strings"

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

type copyDownloadBlobEnumerator common.CopyJobPartOrderRequest

func (e *copyDownloadBlobEnumerator) enumerate(cca *cookedCopyCmdArgs) error {
	util := copyHandlerUtil{}

	ctx := context.WithValue(context.Background(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	// Create Pipeline to Get the Blob Properties or List Blob Segment

	p, err := createBlobPipeline(ctx, e.CredentialInfo)
	if err != nil {
		return err
	}

	// attempt to parse the source url
	sourceUrl, err := url.Parse(cca.source)
	if err != nil {
		return errors.New("cannot parse source URL")
	}

	// append the sas at the end of query params.
	sourceUrl = util.appendQueryParamToUrl(sourceUrl, cca.sourceSAS)

	// get the blob parts
	blobUrlParts := azblob.NewBlobURLParts(*sourceUrl)

	// First Check if source blob exists
	// This check is in place to avoid listing of the blobs and matching the given blob against it
	// For example given source is https://<container>/a?<query-params> and there exists other blobs aa and aab
	// Listing the blobs with prefix /a will list other blob as well
	blobUrl := azblob.NewBlobURL(*sourceUrl, p)
	srcBlobURLPartsExtension := blobURLPartsExtension{BlobURLParts: blobUrlParts}
	if srcBlobURLPartsExtension.isBlobSyntactically() {
		blobProperties, err := blobUrl.GetProperties(ctx, azblob.BlobAccessConditions{})

		// If the source blob exists, then queue transfer and return
		// Example: https://<container>/<blob>?<query-params>
		if err == nil {
			// For a single blob, destination provided can be either a directory or file.
			// If the destination is directory, then name of blob is preserved
			// If the destination is file, then blob will be downloaded as the given file name
			// Example1: Downloading https://<container>/a?<query-params> to directory C:\\Users\\User1
			// will download the blob as C:\\Users\\User1\\a
			// Example2: Downloading https://<container>/a?<query-params> to directory C:\\Users\\User1\\b
			// (b is not a directory) will download blob as C:\\Users\\User1\\b
			var blobLocalPath string

			if util.isPathALocalDirectory(cca.destination) {
				blobNameFromUrl := util.blobNameFromUrl(blobUrlParts)

				// check for special characters and get blobName without special character.
				blobNameFromUrl = util.blobPathWOSpecialCharacters(blobNameFromUrl)
				blobLocalPath = util.generateLocalPath(cca.destination, blobNameFromUrl)
			} else {
				blobLocalPath = cca.destination
			}
			// Add the transfer to CopyJobPartOrderRequest
			e.addTransfer(common.CopyTransfer{
				Source:           util.stripSASFromBlobUrl(*sourceUrl).String(),
				Destination:      blobLocalPath,
				LastModifiedTime: blobProperties.LastModified(),
				SourceSize:       blobProperties.ContentLength(),
				ContentMD5:       blobProperties.ContentMD5(),
			}, cca)
			// only one transfer for this Job, dispatch the JobPart
			err := e.dispatchFinalPart(cca)
			if err != nil {
				return err
			}
			return nil
		} else {
			handleSingleFileValidationErrorForBlob(err)
		}
	}

	glcm.Info(infoCopyFromContainerDirectoryListOfFiles)
	// Since the given source url doesn't represent an existing blob
	// it is either a container or a virtual directory, so it need to be
	// downloaded to an existing directory
	// Check if the given destination path is a directory or not.
	if !util.isPathALocalDirectory(cca.destination) && !strings.EqualFold(cca.destination, devNull) {
		return errors.New("the destination must be an existing directory in this download scenario")
	}

	literalContainerUrl := util.getContainerUrl(blobUrlParts)
	containerUrl := azblob.NewContainerURL(literalContainerUrl, p)

	// Get the source path without the wildcards
	// This is defined since the files mentioned with exclude flag
	// & include flag are relative to the Source
	// If the source has wildcards, then files are relative to the
	// parent source path which is the path of last directory in the source
	// without wildcards
	// For Example: src = "/home/user/dir1" parentSourcePath = "/home/user/dir1"
	// For Example: src = "/home/user/dir*" parentSourcePath = "/home/user"
	// For Example: src = "/home/*" parentSourcePath = "/home"
	parentSourcePath := blobUrlParts.BlobName
	wcIndex := util.firstIndexOfWildCard(parentSourcePath)
	if wcIndex != -1 {
		parentSourcePath = parentSourcePath[:wcIndex]
		pathSepIndex := strings.LastIndex(parentSourcePath, "/")
		if pathSepIndex == -1 {
			parentSourcePath = ""
		} else {
			parentSourcePath = parentSourcePath[:pathSepIndex]
		}
	}

	// If the user has provided us with a list of files to be copied explicitly
	// then there is no need list using the source and then perform pattern matching.
	if len(cca.listOfFilesToCopy) > 0 {
		for _, blob := range cca.listOfFilesToCopy {
			// copy the blobParts in the temporary blobPart since for each blob mentioned in the listOfFilesToCopy flag
			// blobParts will be modified.
			tempBlobUrlParts := blobUrlParts
			if len(parentSourcePath) > 0 && parentSourcePath[len(parentSourcePath)-1] == common.AZCOPY_PATH_SEPARATOR_CHAR {
				parentSourcePath = parentSourcePath[0 : len(parentSourcePath)-1]
			}
			// Create the blobPath using the given source and blobs mentioned with listOfFile flag.
			// For Example:
			// 1. source = "https://sdksampleperftest.blob.core.windows.net/bigdata" blob = "file1.txt" blobPath= "file1.txt"
			// 2. source = "https://sdksampleperftest.blob.core.windows.net/bigdata/dir-1" blob = "file1.txt" blobPath= "dir-1/file1.txt"
			blobPath := fmt.Sprintf("%s%s%s", parentSourcePath, common.AZCOPY_PATH_SEPARATOR_STRING, blob)
			if len(blobPath) > 0 && blobPath[0] == common.AZCOPY_PATH_SEPARATOR_CHAR {
				blobPath = blobPath[1:]
			}
			tempBlobUrlParts.BlobName = blobPath
			blobURL := azblob.NewBlobURL(tempBlobUrlParts.URL(), p)
			blobProperties, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{})
			if err == nil {
				// If the blob represents a folder as per the conditions mentioned in the
				// api doesBlobRepresentAFolder, then skip the blob.
				if util.doesBlobRepresentAFolder(blobProperties.NewMetadata()) {
					continue
				}
				blobRelativePath := strings.Replace(blobPath, parentSourcePath, "", 1)
				if len(blobRelativePath) > 0 && blobRelativePath[0] == common.AZCOPY_PATH_SEPARATOR_CHAR {
					blobRelativePath = blobRelativePath[1:]
				}
				// check for the special character in blob relative path and get path without special character.
				blobRelativePath = util.blobPathWOSpecialCharacters(blobRelativePath)

				e.addTransfer(common.CopyTransfer{
					Source:           util.stripSASFromBlobUrl(util.createBlobUrlFromContainer(blobUrlParts, blobPath)).String(),
					Destination:      util.generateLocalPath(cca.destination, blobRelativePath),
					LastModifiedTime: blobProperties.LastModified(),
					SourceSize:       blobProperties.ContentLength(),
					ContentMD5:       blobProperties.ContentMD5(),
				}, cca)
				continue
			}
			if !cca.recursive {
				glcm.Info(fmt.Sprintf("error fetching properties of %s. Either it is a directory or getting the blob properties failed. For virtual directories try using the recursive flag", blobPath))
				continue
			}
			// Since the given blob in the listOFFiles flag is not a blob, it can be a virtual directory
			// If the virtual directory doesn't have a path separator at the end of it, then we should append it.
			// This is done to avoid listing blobs which shares the common prefix i.e the virtual directory name.
			// For Example:
			// 1. source = "https://sdksampleperftest.blob.core.windows.net/bigdata" blob="100k". In this case, it is
			// a possibility that we have blobs https://sdksampleperftest.blob.core.windows.net/bigdata/100K and
			// https://sdksampleperftest.blob.core.windows.net/bigdata/100K/f1.txt. So we need to list the blob
			// https://sdksampleperftest.blob.core.windows.net/bigdata/100K/f1.txt
			searchPrefix := tempBlobUrlParts.BlobName
			if len(searchPrefix) > 0 && searchPrefix[len(searchPrefix)-1] != common.AZCOPY_PATH_SEPARATOR_CHAR {
				searchPrefix += common.AZCOPY_PATH_SEPARATOR_STRING
			}
			for marker := (azblob.Marker{}); marker.NotDone(); {
				// look for all blobs that start with the prefix, so that if a blob is under the virtual directory, it will show up
				listBlob, err := containerUrl.ListBlobsFlatSegment(ctx, marker,
					azblob.ListBlobsSegmentOptions{Details: azblob.BlobListingDetails{Metadata: true}, Prefix: searchPrefix})
				if err != nil {
					glcm.Info(fmt.Sprintf("cannot list blobs inside directory %s mentioned.", searchPrefix))
					continue
				}
				// If there was no blob listed inside the directory mentioned in the listOfFilesToCopy flag,
				// report to the user and continue to the next blob mentioned.
				if !listBlob.NextMarker.NotDone() && len(listBlob.Segment.BlobItems) == 0 {
					glcm.Info(fmt.Sprintf("cannot list blobs inside directory %s mentioned.", searchPrefix))
					break
				}
				for _, blobInfo := range listBlob.Segment.BlobItems {
					// If the blob represents a folder as per the conditions mentioned in the
					// api doesBlobRepresentAFolder, then skip the blob.
					if util.doesBlobRepresentAFolder(blobInfo.Metadata) {
						continue
					}
					blobRelativePath := strings.Replace(blobInfo.Name, parentSourcePath, "", 1)
					if len(blobRelativePath) > 0 && blobRelativePath[0] == common.AZCOPY_PATH_SEPARATOR_CHAR {
						blobRelativePath = blobRelativePath[1:]
					}
					//blobRelativePath := util.getRelativePath(parentSourcePath, blobInfo.Name)

					// check for the special character in blob relative path and get path without special character.
					blobRelativePath = util.blobPathWOSpecialCharacters(blobRelativePath)
					e.addTransfer(common.CopyTransfer{
						Source:           util.stripSASFromBlobUrl(util.createBlobUrlFromContainer(blobUrlParts, blobInfo.Name)).String(),
						Destination:      util.generateLocalPath(cca.destination, blobRelativePath),
						LastModifiedTime: blobInfo.Properties.LastModified,
						SourceSize:       *blobInfo.Properties.ContentLength,
						ContentMD5:       blobInfo.Properties.ContentMD5,
					}, cca)
				}
				marker = listBlob.NextMarker
			}
		}
		// If there are no transfer to queue up, exit with message
		if len(e.Transfers) == 0 {
			glcm.Error(fmt.Sprintf("no transfer queued for copying data from %s to %s", cca.source, cca.destination))
			return nil
		}
		// dispatch the JobPart as Final Part of the Job
		err = e.dispatchFinalPart(cca)
		if err != nil {
			return err
		}
		return nil
	}

	// searchPrefix is the used in listing blob inside a container
	// all the blob listed should have the searchPrefix as the prefix
	// blobNamePattern represents the regular expression which the blobName should Match
	searchPrefix, blobNamePattern, isWildcardSearch := srcBlobURLPartsExtension.searchPrefixFromBlobURL()

	// If blobNamePattern is "*", means that all the contents inside the given source url recursively needs to be downloaded
	// It means that source url provided is either a container or a virtual directory
	// All the blobs inside a container or virtual directory will be downloaded only when the recursive flag is set to true
	if blobNamePattern == "*" && !cca.recursive && !isWildcardSearch {
		return fmt.Errorf("cannot download the enitre container / virtual directory. Please use --recursive flag")
	}

	// if downloading entire container, then create a local directory with the container's name
	if blobUrlParts.BlobName == "" {
		cca.destination = util.generateLocalPath(cca.destination, blobUrlParts.ContainerName)
	}

	// perform a list blob with search prefix
	for marker := (azblob.Marker{}); marker.NotDone(); {
		// look for all blobs that start with the prefix, so that if a blob is under the virtual directory, it will show up
		listBlob, err := containerUrl.ListBlobsFlatSegment(ctx, marker,
			azblob.ListBlobsSegmentOptions{Details: azblob.BlobListingDetails{Metadata: true}, Prefix: searchPrefix})
		if err != nil {
			return fmt.Errorf("cannot list blobs for download. Failed with error %s", err.Error())
		}

		// Process the blobs returned in this result segment (if the segment is empty, the loop body won't execute)
		for _, blobInfo := range listBlob.Segment.BlobItems {
			// If the blob represents a folder as per the conditions mentioned in the
			// api doesBlobRepresentAFolder, then skip the blob.
			if util.doesBlobRepresentAFolder(blobInfo.Metadata) {
				continue
			}
			// If the blobName doesn't matches the blob name pattern, then blob is not included
			// queued for transfer
			if !util.matchBlobNameAgainstPattern(blobNamePattern, blobInfo.Name, cca.recursive) {
				continue
			}

			// Check the blob should be included or not
			if !util.resourceShouldBeIncluded(parentSourcePath, e.Include, blobInfo.Name) {
				continue
			}

			// Check the blob should be excluded or not
			if util.resourceShouldBeExcluded(parentSourcePath, e.Exclude, blobInfo.Name) {
				continue
			}

			// If wildcard exists in the source, searchPrefix is the source string till the first wildcard index
			// In case of wildcards in source string, there is no need to create the last virtal directory in the searchPrefix
			// locally.
			// blobRelativePath will be as follow
			// source = https://<container>/<vd-1>/*?<signature> blobName = /vd-1/dir/1.txt
			// blobRelativePath = dir/1.txt
			// source = https://<container>/<vd-1>/dir/*.txt?<signature> blobName = /vd-1/dir/1.txt
			// blobRelativePath = 1.txt
			// source = https://<container>/<vd-1>/dir/*/*.txt?<signature> blobName = /vd-1/dir/dir1/1.txt
			// blobRelativePath = dir1/1.txt
			var blobRelativePath = ""
			if util.firstIndexOfWildCard(blobUrlParts.BlobName) != -1 {
				blobRelativePath = strings.Replace(blobInfo.Name, searchPrefix[:strings.LastIndex(searchPrefix, common.AZCOPY_PATH_SEPARATOR_STRING)+1], "", 1)
			} else {
				blobRelativePath = util.getRelativePath(searchPrefix, blobInfo.Name)
			}
			// check for the special character in blob relative path and get path without special character.
			blobRelativePath = util.blobPathWOSpecialCharacters(blobRelativePath)
			e.addTransfer(common.CopyTransfer{
				Source:           util.stripSASFromBlobUrl(util.createBlobUrlFromContainer(blobUrlParts, blobInfo.Name)).String(),
				Destination:      util.generateLocalPath(cca.destination, blobRelativePath),
				LastModifiedTime: blobInfo.Properties.LastModified,
				SourceSize:       *blobInfo.Properties.ContentLength,
				ContentMD5:       blobInfo.Properties.ContentMD5,
			}, cca)
		}
		marker = listBlob.NextMarker
	}
	// If part number is 0 && number of transfer queued is 0
	// it means that no job part has been dispatched and there are no
	// transfer in Job to dispatch a JobPart.
	if e.PartNum == 0 && len(e.Transfers) == 0 {
		return fmt.Errorf("no transfer queued to download. Please verify the source / destination")
	}
	// dispatch the JobPart as Final Part of the Job
	err = e.dispatchFinalPart(cca)
	if err != nil {
		return err
	}
	return nil
}

func (e *copyDownloadBlobEnumerator) addTransfer(transfer common.CopyTransfer, cca *cookedCopyCmdArgs) error {
	// if we are downloading to dev null, we must point to devNull itself, rather than some file under it
	if strings.EqualFold(e.DestinationRoot, devNull) {
		transfer.Destination = ""
	}
	return addTransfer((*common.CopyJobPartOrderRequest)(e), transfer, cca)
}

func (e *copyDownloadBlobEnumerator) dispatchFinalPart(cca *cookedCopyCmdArgs) error {
	return dispatchFinalPart((*common.CopyJobPartOrderRequest)(e), cca)
}
