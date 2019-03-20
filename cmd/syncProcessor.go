// Copyright © 2017 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cmd

import (
	"context"
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"net/url"
	"os"
	"path"
	"path/filepath"
)

// extract the right info from cooked arguments and instantiate a generic copy transfer processor from it
func newSyncTransferProcessor(cca *cookedSyncCmdArgs, numOfTransfersPerPart int, isSingleFileSync bool) *copyTransferProcessor {
	copyJobTemplate := &common.CopyJobPartOrderRequest{
		JobID:           cca.jobID,
		CommandString:   cca.commandString,
		FromTo:          cca.fromTo,
		SourceRoot:      replacePathSeparators(cca.source),
		DestinationRoot: replacePathSeparators(cca.destination),

		// authentication related
		CredentialInfo: cca.credentialInfo,
		SourceSAS:      cca.sourceSAS,
		DestinationSAS: cca.destinationSAS,

		// flags
		BlobAttributes: common.BlobTransferAttributes{
			PreserveLastModifiedTime: true, // must be true for sync so that future syncs have this information available
			SuppressUploadMd5:        cca.suppressUploadMd5,
			MD5ValidationOption:      cca.md5ValidationOption,
			BlockSizeInBytes:         cca.blockSize},
		ForceWrite: true, // once we decide to transfer for a sync operation, we overwrite the destination regardless
		LogLevel:   cca.logVerbosity,
	}

	if !isSingleFileSync {
		copyJobTemplate.SourceRoot += common.AZCOPY_PATH_SEPARATOR_STRING
		copyJobTemplate.DestinationRoot += common.AZCOPY_PATH_SEPARATOR_STRING
	}

	reportFirstPart := func() { cca.setFirstPartOrdered() }
	reportFinalPart := func() { cca.isEnumerationComplete = true }

	shouldEncodeSource := cca.fromTo.From().IsRemote()
	shouldEncodeDestination := cca.fromTo.To().IsRemote()

	// note that the source and destination, along with the template are given to the generic processor's constructor
	// this means that given an object with a relative path, this processor already knows how to schedule the right kind of transfers
	return newCopyTransferProcessor(copyJobTemplate, numOfTransfersPerPart, cca.source, cca.destination,
		shouldEncodeSource, shouldEncodeDestination, reportFirstPart, reportFinalPart)
}

// base for delete processors targeting different resources
type interactiveDeleteProcessor struct {
	// the plugged-in deleter that performs the actual deletion
	deleter objectProcessor

	// whether we should ask the user for permission the first time we delete a file
	shouldPromptUser bool

	// note down whether any delete should happen
	shouldDelete bool

	// used for prompt message
	// examples: "blobs", "local files", etc.
	objectTypeToDisplay string

	// used for prompt message
	// examples: a directory path, or url to container
	objectLocationToDisplay string

	// count the deletions that happened
	incrementDeletionCount func()
}

func (d *interactiveDeleteProcessor) removeImmediately(object storedObject) (err error) {
	if d.shouldPromptUser {
		d.shouldDelete = d.promptForConfirmation() // note down the user's decision
		d.shouldPromptUser = false                 // only prompt the first time that this function is called
	}

	if !d.shouldDelete {
		return nil
	}

	err = d.deleter(object)
	if err != nil {
		glcm.Info(fmt.Sprintf("error %s deleting the object %s", err.Error(), object.relativePath))
	}

	if d.incrementDeletionCount != nil {
		d.incrementDeletionCount()
	}
	return
}

func (d *interactiveDeleteProcessor) promptForConfirmation() (shouldDelete bool) {
	shouldDelete = false

	answer := glcm.Prompt(fmt.Sprintf("Sync has discovered %s that are not present at the source, would you like to delete them from the destination(%s)? Please confirm with y/n (default: n): ",
		d.objectTypeToDisplay, d.objectLocationToDisplay))
	if answer == "y" || answer == "yes" {
		shouldDelete = true
		glcm.Info(fmt.Sprintf("Confirmed. The extra %s will be deleted:", d.objectTypeToDisplay))
	} else {
		glcm.Info("No deletions will happen.")
	}
	return
}

func newInteractiveDeleteProcessor(deleter objectProcessor, deleteDestination common.DeleteDestination,
	objectTypeToDisplay string, objectLocationToDisplay string, incrementDeletionCounter func()) *interactiveDeleteProcessor {

	return &interactiveDeleteProcessor{
		deleter:                 deleter,
		objectTypeToDisplay:     objectTypeToDisplay,
		objectLocationToDisplay: objectLocationToDisplay,
		incrementDeletionCount:  incrementDeletionCounter,
		shouldPromptUser:        deleteDestination == common.EDeleteDestination.Prompt(),
		shouldDelete:            deleteDestination == common.EDeleteDestination.True(), // if shouldPromptUser is true, this will start as false, but we will determine its value later
	}
}

func newSyncLocalDeleteProcessor(cca *cookedSyncCmdArgs) *interactiveDeleteProcessor {
	localDeleter := localFileDeleter{rootPath: cca.destination}
	return newInteractiveDeleteProcessor(localDeleter.deleteFile, cca.deleteDestination, "local files", cca.destination, cca.incrementDeletionCount)
}

type localFileDeleter struct {
	rootPath string
}

func (l *localFileDeleter) deleteFile(object storedObject) error {
	glcm.Info("Deleting extra file: " + object.relativePath)
	return os.Remove(filepath.Join(l.rootPath, object.relativePath))
}

func newSyncBlobDeleteProcessor(cca *cookedSyncCmdArgs) (*interactiveDeleteProcessor, error) {
	rawURL, err := url.Parse(cca.destination)
	if err != nil {
		return nil, err
	} else if err == nil && cca.destinationSAS != "" {
		copyHandlerUtil{}.appendQueryParamToUrl(rawURL, cca.destinationSAS)
	}

	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	p, err := createBlobPipeline(ctx, cca.credentialInfo)
	if err != nil {
		return nil, err
	}

	return newInteractiveDeleteProcessor(newBlobDeleter(rawURL, p, ctx).deleteBlob,
		cca.deleteDestination, "blobs", cca.destination, cca.incrementDeletionCount), nil
}

type blobDeleter struct {
	rootURL *url.URL
	p       pipeline.Pipeline
	ctx     context.Context
}

func newBlobDeleter(rawRootURL *url.URL, p pipeline.Pipeline, ctx context.Context) *blobDeleter {
	return &blobDeleter{
		rootURL: rawRootURL,
		p:       p,
		ctx:     ctx,
	}
}

func (b *blobDeleter) deleteBlob(object storedObject) error {
	glcm.Info("Deleting extra blob: " + object.relativePath)

	// construct the blob URL using its relative path
	// the rootURL could be pointing to a container, or a virtual directory
	blobURLParts := azblob.NewBlobURLParts(*b.rootURL)
	blobURLParts.BlobName = path.Join(blobURLParts.BlobName, object.relativePath)

	blobURL := azblob.NewBlobURL(blobURLParts.URL(), b.p)
	_, err := blobURL.Delete(b.ctx, azblob.DeleteSnapshotsOptionInclude, azblob.BlobAccessConditions{})
	return err
}
