package cmd

import (
	"context"
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
)

// -------------------------------------- Traversers -------------------------------------- \\
// these traversers allow us to iterate through different resource types

type blobTraverser struct {
	rawURL    *url.URL
	p         pipeline.Pipeline
	ctx       context.Context
	recursive bool
	isSource  bool
	cca       *cookedSyncCmdArgs
}

func (blobTraverser *blobTraverser) getPropertiesIfSingleBlob() (blobProps *azblob.BlobGetPropertiesResponse, isBlob bool) {
	blobURL := azblob.NewBlobURL(*blobTraverser.rawURL, blobTraverser.p)
	blobProps, blobPropertiesErr := blobURL.GetProperties(blobTraverser.ctx, azblob.BlobAccessConditions{})

	// if there was no problem getting the properties, it means that we are looking at a single blob
	if blobPropertiesErr == nil {
		isBlob = true
		return
	}

	return
}

func (blobTraverser *blobTraverser) traverse(processor entityProcessor, filters []entityFilter) (err error) {
	blobUrlParts := azblob.NewBlobURLParts(*blobTraverser.rawURL)

	// check if the url points to a single blob
	blobProperties, isBlob := blobTraverser.getPropertiesIfSingleBlob()
	if isBlob {
		entity := genericEntity{
			name:             getEntityNameOnly(blobUrlParts.BlobName),
			relativePath:     "", // relative path makes no sense when the full path already points to the file
			lastModifiedTime: blobProperties.LastModified(),
			size:             blobProperties.ContentLength(),
		}
		blobTraverser.incrementEnumerationCounter()
		return processIfPassedFilters(filters, entity, processor)
	}

	// get the container URL so that we can list the blobs
	containerRawURL := copyHandlerUtil{}.getContainerUrl(blobUrlParts)
	containerURL := azblob.NewContainerURL(containerRawURL, blobTraverser.p)

	// get the search prefix to aid in the listing
	searchPrefix := blobUrlParts.BlobName

	// append a slash if it is not already present
	if searchPrefix != "" && !strings.HasSuffix(searchPrefix, common.AZCOPY_PATH_SEPARATOR_STRING) {
		searchPrefix += common.AZCOPY_PATH_SEPARATOR_STRING
	}

	for marker := (azblob.Marker{}); marker.NotDone(); {
		// look for all blobs that start with the prefix
		listBlob, err := containerURL.ListBlobsFlatSegment(blobTraverser.ctx, marker,
			azblob.ListBlobsSegmentOptions{Prefix: searchPrefix})
		if err != nil {
			return fmt.Errorf("cannot list blobs. Failed with error %s", err.Error())
		}

		// process the blobs returned in this result segment
		for _, blobInfo := range listBlob.Segment.BlobItems {
			relativePath := strings.Replace(blobInfo.Name, searchPrefix, "", 1)

			// if recursive
			if !blobTraverser.recursive && strings.Contains(relativePath, common.AZCOPY_PATH_SEPARATOR_STRING) {
				continue
			}

			entity := genericEntity{
				name:             getEntityNameOnly(blobInfo.Name),
				relativePath:     relativePath,
				lastModifiedTime: blobInfo.Properties.LastModified,
				size:             *blobInfo.Properties.ContentLength,
			}
			blobTraverser.incrementEnumerationCounter()
			processErr := processIfPassedFilters(filters, entity, processor)
			if processErr != nil {
				return processErr
			}
		}

		marker = listBlob.NextMarker
	}

	return
}

func (blobTraverser *blobTraverser) incrementEnumerationCounter() {
	var counterAddr *uint64

	if blobTraverser.isSource {
		counterAddr = &blobTraverser.cca.atomicSourceFilesScanned
	} else {
		counterAddr = &blobTraverser.cca.atomicDestinationFilesScanned
	}

	atomic.AddUint64(counterAddr, 1)
}

func newBlobTraverser(cca *cookedSyncCmdArgs, isSource bool) (traverser *blobTraverser, err error) {
	traverser = &blobTraverser{}
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	if isSource {
		traverser.rawURL, err = url.Parse(cca.source)
		if err == nil && cca.sourceSAS != "" {
			copyHandlerUtil{}.appendQueryParamToUrl(traverser.rawURL, cca.sourceSAS)
		}

	} else {
		traverser.rawURL, err = url.Parse(cca.destination)
		if err == nil && cca.destinationSAS != "" {
			copyHandlerUtil{}.appendQueryParamToUrl(traverser.rawURL, cca.destinationSAS)
		}
	}

	if err != nil {
		return
	}

	traverser.p, err = createBlobPipeline(ctx, cca.credentialInfo)
	if err != nil {
		return
	}

	traverser.isSource = isSource
	traverser.ctx = context.TODO()
	traverser.recursive = cca.recursive
	traverser.cca = cca
	return
}

type localTraverser struct {
	fullPath       string
	recursive      bool
	followSymlinks bool
	isSource       bool
	cca            *cookedSyncCmdArgs
}

func (localTraverser *localTraverser) traverse(processor entityProcessor, filters []entityFilter) (err error) {
	singleFileInfo, isSingleFile, err := localTraverser.getInfoIfSingleFile()

	if err != nil {
		return fmt.Errorf("cannot scan the path %s, please verify that it is a valid", localTraverser.fullPath)
	}

	// if the path is a single file, then pass it through the filters and send to processor
	if isSingleFile {
		localTraverser.incrementEnumerationCounter()
		err = processIfPassedFilters(filters, genericEntity{
			name:             singleFileInfo.Name(),
			relativePath:     "", // relative path makes no sense when the full path already points to the file
			lastModifiedTime: singleFileInfo.ModTime(),
			size:             singleFileInfo.Size()}, processor)
		return

	} else {
		if localTraverser.recursive {
			err = filepath.Walk(localTraverser.fullPath, func(filePath string, fileInfo os.FileInfo, fileError error) error {
				if fileError != nil {
					return fileError
				}

				// skip the subdirectories
				if fileInfo.IsDir() {
					return nil
				}

				localTraverser.incrementEnumerationCounter()
				return processIfPassedFilters(filters, genericEntity{
					name:             fileInfo.Name(),
					relativePath:     strings.Replace(filePath, localTraverser.fullPath+common.AZCOPY_PATH_SEPARATOR_STRING, "", 1),
					lastModifiedTime: fileInfo.ModTime(),
					size:             fileInfo.Size()}, processor)
			})

			return
		} else {
			// if recursive is off, we only need to scan the files immediately under the fullPath
			files, err := ioutil.ReadDir(localTraverser.fullPath)
			if err != nil {
				return err
			}

			// go through the files and return if any of them fail to process
			for _, singleFile := range files {
				if singleFile.IsDir() {
					continue
				}

				localTraverser.incrementEnumerationCounter()
				err = processIfPassedFilters(filters, genericEntity{
					name:             singleFile.Name(),
					relativePath:     singleFile.Name(),
					lastModifiedTime: singleFile.ModTime(),
					size:             singleFile.Size()}, processor)

				if err != nil {
					return err
				}
			}
		}
	}

	return
}

func (localTraverser *localTraverser) getInfoIfSingleFile() (os.FileInfo, bool, error) {
	fileInfo, err := os.Stat(localTraverser.fullPath)

	if err != nil {
		return nil, false, err
	}

	if fileInfo.IsDir() {
		return nil, false, nil
	}

	return fileInfo, true, nil
}

func (localTraverser *localTraverser) incrementEnumerationCounter() {
	var counterAddr *uint64

	if localTraverser.isSource {
		counterAddr = &localTraverser.cca.atomicSourceFilesScanned
	} else {
		counterAddr = &localTraverser.cca.atomicDestinationFilesScanned
	}

	atomic.AddUint64(counterAddr, 1)
}

func newLocalTraverser(cca *cookedSyncCmdArgs, isSource bool) *localTraverser {
	traverser := localTraverser{}

	if isSource {
		traverser.fullPath = cca.source
	} else {
		traverser.fullPath = cca.destination
	}

	traverser.isSource = isSource
	traverser.recursive = cca.recursive
	traverser.followSymlinks = cca.followSymlinks
	traverser.cca = cca
	return &traverser
}
