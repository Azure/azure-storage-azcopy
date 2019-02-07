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
func newSyncTransferProcessor(cca *cookedSyncCmdArgs, numOfTransfersPerPart int) *copyTransferProcessor {
	copyJobTemplate := &common.CopyJobPartOrderRequest{
		JobID:         cca.jobID,
		CommandString: cca.commandString,
		FromTo:        cca.fromTo,

		// authentication related
		CredentialInfo: cca.credentialInfo,
		SourceSAS:      cca.sourceSAS,
		DestinationSAS: cca.destinationSAS,

		// flags
		BlobAttributes: common.BlobTransferAttributes{
			PreserveLastModifiedTime: true, // must be true for sync so that future syncs have this information available
			MD5ValidationOption:      cca.md5ValidationOption,
			BlockSizeInBytes:         cca.blockSize},
		ForceWrite: true, // once we decide to transfer for a sync operation, we overwrite the destination regardless
		LogLevel:   cca.logVerbosity,
	}

	reportFirstPart := func() { cca.setFirstPartOrdered() }
	reportFinalPart := func() { cca.isEnumerationComplete = true }

	// note that the source and destination, along with the template are given to the generic processor's constructor
	// this means that given an object with a relative path, this processor already knows how to schedule the right kind of transfers
	return newCopyTransferProcessor(copyJobTemplate, numOfTransfersPerPart, cca.source, cca.destination, reportFirstPart, reportFinalPart)
}

// base for delete processors targeting different resources
type interactiveDeleteProcessor struct {
	// the plugged-in deleter that performs the actual deletion
	deleter objectProcessor

	// whether force delete is on
	force bool

	// ask the user for permission the first time we delete a file
	hasPromptedUser bool

	// used for prompt message
	// examples: "blobs", "local files", etc.
	objectType string

	// note down whether any delete should happen
	shouldDelete bool
}

func (d *interactiveDeleteProcessor) removeImmediately(object storedObject) (err error) {
	if !d.hasPromptedUser {
		d.shouldDelete = d.promptForConfirmation()
		d.hasPromptedUser = true
	}

	if !d.shouldDelete {
		return nil
	}

	err = d.deleter(object)
	if err != nil {
		glcm.Info(fmt.Sprintf("error %s deleting the object %s", err.Error(), object.relativePath))
	}

	return
}

func (d *interactiveDeleteProcessor) promptForConfirmation() (shouldDelete bool) {
	shouldDelete = false

	// omit asking if the user has already specified
	if d.force {
		shouldDelete = true
	} else {
		answer := glcm.Prompt(fmt.Sprintf("Sync has discovered %s that are not present at the source, would you like to delete them? Please confirm with y/n: ", d.objectType))
		if answer == "y" || answer == "yes" {
			shouldDelete = true
			glcm.Info(fmt.Sprintf("Confirmed. The extra %s will be deleted:", d.objectType))
		} else {
			glcm.Info("No deletions will happen.")
		}
	}
	return
}

func (d *interactiveDeleteProcessor) wasAnyFileDeleted() bool {
	// we'd have prompted the user if any stored object was passed in
	return d.hasPromptedUser
}

func newSyncLocalDeleteProcessor(cca *cookedSyncCmdArgs) *interactiveDeleteProcessor {
	localDeleter := localFileDeleter{rootPath: cca.destination}
	return &interactiveDeleteProcessor{deleter: localDeleter.deleteFile, force: cca.force, objectType: "local files"}
}

type localFileDeleter struct {
	rootPath string
}

func (l *localFileDeleter) deleteFile(object storedObject) error {
	glcm.Info("Deleting file: " + object.relativePath)
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

	return &interactiveDeleteProcessor{deleter: newBlobDeleter(rawURL, p, ctx).deleteBlob, force: cca.force, objectType: "blobs"}, nil
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
	glcm.Info("Deleting: " + object.relativePath)

	// construct the blob URL using its relative path
	// the rootURL could be pointing to a container, or a virtual directory
	blobURLParts := azblob.NewBlobURLParts(*b.rootURL)
	blobURLParts.BlobName = path.Join(blobURLParts.BlobName, object.relativePath)

	blobURL := azblob.NewBlobURL(blobURLParts.URL(), b.p)
	_, err := blobURL.Delete(b.ctx, azblob.DeleteSnapshotsOptionInclude, azblob.BlobAccessConditions{})
	return err
}
