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
	"net/url"
	"os"
	"path"

	"github.com/Azure/azure-storage-file-go/azfile"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

// extract the right info from cooked arguments and instantiate a generic copy transfer processor from it
func newSyncTransferProcessor(cca *cookedSyncCmdArgs, numOfTransfersPerPart int, fpo common.FolderPropertyOption) *copyTransferProcessor {
	copyJobTemplate := &common.CopyJobPartOrderRequest{
		JobID:           cca.jobID,
		CommandString:   cca.commandString,
		FromTo:          cca.fromTo,
		Fpo:             fpo,
		SourceRoot:      cca.source.CloneWithConsolidatedSeparators(),
		DestinationRoot: cca.destination.CloneWithConsolidatedSeparators(),
		CredentialInfo:  cca.credentialInfo,

		// flags
		BlobAttributes: common.BlobTransferAttributes{
			PreserveLastModifiedTime: true, // must be true for sync so that future syncs have this information available
			PutMd5:                   cca.putMd5,
			MD5ValidationOption:      cca.md5ValidationOption,
			BlockSizeInBytes:         cca.blockSize},
		ForceWrite:                     common.EOverwriteOption.True(), // once we decide to transfer for a sync operation, we overwrite the destination regardless
		ForceIfReadOnly:                cca.forceIfReadOnly,
		LogLevel:                       cca.logVerbosity,
		PreserveSMBPermissions:         cca.preserveSMBPermissions,
		PreserveSMBInfo:                cca.preserveSMBInfo,
		S2SSourceChangeValidation:      true,
		DestLengthValidation:           true,
		S2SGetPropertiesInBackend:      true,
		S2SInvalidMetadataHandleOption: common.EInvalidMetadataHandleOption.RenameIfInvalid(),
		CpkOptions:                     cca.cpkOptions,
		S2SPreserveBlobTags:            cca.s2sPreserveBlobTags,
	}

	reportFirstPart := func(jobStarted bool) { cca.setFirstPartOrdered() } // for compatibility with the way sync has always worked, we don't check jobStarted here
	reportFinalPart := func() { cca.isEnumerationComplete = true }

	// note that the source and destination, along with the template are given to the generic processor's constructor
	// this means that given an object with a relative path, this processor already knows how to schedule the right kind of transfers
	return newCopyTransferProcessor(copyJobTemplate, numOfTransfersPerPart, cca.source, cca.destination,
		reportFirstPart, reportFinalPart, cca.preserveAccessTier)
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
	// examples: "blob", "local file", etc.
	objectTypeToDisplay string

	// used for prompt message
	// examples: a directory path, or url to container
	objectLocationToDisplay string

	// count the deletions that happened
	incrementDeletionCount func()
}

func (d *interactiveDeleteProcessor) removeImmediately(object StoredObject) (err error) {
	if d.shouldPromptUser {
		d.shouldDelete, d.shouldPromptUser = d.promptForConfirmation(object) // note down the user's decision
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

func (d *interactiveDeleteProcessor) promptForConfirmation(object StoredObject) (shouldDelete bool, keepPrompting bool) {
	answer := glcm.Prompt(fmt.Sprintf("The %s '%s' does not exist at the source. "+
		"Do you wish to delete it from the destination(%s)?",
		d.objectTypeToDisplay, object.relativePath, d.objectLocationToDisplay),
		common.PromptDetails{
			PromptType:   common.EPromptType.DeleteDestination(),
			PromptTarget: object.relativePath,
			ResponseOptions: []common.ResponseOption{
				common.EResponseOption.Yes(),
				common.EResponseOption.No(),
				common.EResponseOption.YesForAll(),
				common.EResponseOption.NoForAll()},
		},
	)

	switch answer {
	case common.EResponseOption.Yes():
		// print nothing, since the deleter is expected to log the message when the delete happens
		return true, true
	case common.EResponseOption.YesForAll():
		glcm.Info(fmt.Sprintf("Confirmed. All the extra %ss will be deleted.", d.objectTypeToDisplay))
		return true, false
	case common.EResponseOption.No():
		glcm.Info(fmt.Sprintf("Keeping extra %s: %s", d.objectTypeToDisplay, object.relativePath))
		return false, true
	case common.EResponseOption.NoForAll():
		glcm.Info("No deletions will happen from now onwards.")
		return false, false
	default:
		glcm.Info(fmt.Sprintf("Unrecognizable answer, keeping extra %s: %s.", d.objectTypeToDisplay, object.relativePath))
		return false, true
	}
}

func newInteractiveDeleteProcessor(deleter objectProcessor, deleteDestination common.DeleteDestination,
	objectTypeToDisplay string, objectLocationToDisplay common.ResourceString, incrementDeletionCounter func()) *interactiveDeleteProcessor {

	return &interactiveDeleteProcessor{
		deleter:                 deleter,
		objectTypeToDisplay:     objectTypeToDisplay,
		objectLocationToDisplay: objectLocationToDisplay.Value,
		incrementDeletionCount:  incrementDeletionCounter,
		shouldPromptUser:        deleteDestination == common.EDeleteDestination.Prompt(),
		shouldDelete:            deleteDestination == common.EDeleteDestination.True(), // if shouldPromptUser is true, this will start as false, but we will determine its value later
	}
}

func newSyncLocalDeleteProcessor(cca *cookedSyncCmdArgs) *interactiveDeleteProcessor {
	localDeleter := localFileDeleter{rootPath: cca.destination.ValueLocal()}
	return newInteractiveDeleteProcessor(localDeleter.deleteFile, cca.deleteDestination, "local file", cca.destination, cca.incrementDeletionCount)
}

type localFileDeleter struct {
	rootPath string
}

// As at version 10.4.0, we intentionally don't delete directories in sync,
// even if our folder properties option suggests we should.
// Why? The key difficulties are as follows, and its the third one that we don't currently have a solution for.
// 1. Timing (solvable in theory with FolderDeletionManager)
// 2. Identifying which should be removed when source does not have concept of folders (e.g. BLob)
//    Probably solution is to just respect the folder properties option setting (which we already do in our delete processors)
// 3. In Azure Files case (and to a lesser extent on local disks) users may have ACLS or other properties
//    set on the directories, and wish to retain those even tho the directories are empty. (Perhaps less of an issue
//    when syncing from folder-aware sources that DOES NOT HAVE the directory. But still an issue when syncing from
//    blob. E.g. we delete a folder because there's nothing in it right now, but really user wanted it there,
//    and have set up custom ACLs on it for future use.  If we delete, they lose the custom ACL setup.
// TODO: shall we add folder deletion support at some stage? (In cases where folderPropertiesOption says that folders should be processed)
func shouldSyncRemoveFolders() bool {
	return false
}

func (l *localFileDeleter) deleteFile(object StoredObject) error {
	if object.entityType == common.EEntityType.File() {
		glcm.Info("Deleting extra file: " + object.relativePath)
		return os.Remove(common.GenerateFullPath(l.rootPath, object.relativePath))
	}
	if shouldSyncRemoveFolders() {
		panic("folder deletion enabled but not implemented")
	}
	return nil
}

func newSyncDeleteProcessor(cca *cookedSyncCmdArgs) (*interactiveDeleteProcessor, error) {
	rawURL, err := cca.destination.FullURL()
	if err != nil {
		return nil, err
	}

	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	p, err := InitPipeline(ctx, cca.fromTo.To(), cca.credentialInfo, cca.logVerbosity.ToPipelineLogLevel())
	if err != nil {
		return nil, err
	}

	return newInteractiveDeleteProcessor(newRemoteResourceDeleter(rawURL, p, ctx, cca.fromTo.To()).delete,
		cca.deleteDestination, cca.fromTo.To().String(), cca.destination, cca.incrementDeletionCount), nil
}

type remoteResourceDeleter struct {
	rootURL        *url.URL
	p              pipeline.Pipeline
	ctx            context.Context
	targetLocation common.Location
}

func newRemoteResourceDeleter(rawRootURL *url.URL, p pipeline.Pipeline, ctx context.Context, targetLocation common.Location) *remoteResourceDeleter {
	return &remoteResourceDeleter{
		rootURL:        rawRootURL,
		p:              p,
		ctx:            ctx,
		targetLocation: targetLocation,
	}
}

func (b *remoteResourceDeleter) delete(object StoredObject) error {
	if object.entityType == common.EEntityType.File() {
		// TODO: use b.targetLocation.String() in the next line, instead of "object", if we can make it come out as string
		glcm.Info("Deleting extra object: " + object.relativePath)
		switch b.targetLocation {
		case common.ELocation.Blob():
			blobURLParts := azblob.NewBlobURLParts(*b.rootURL)
			blobURLParts.BlobName = path.Join(blobURLParts.BlobName, object.relativePath)
			blobURL := azblob.NewBlobURL(blobURLParts.URL(), b.p)
			_, err := blobURL.Delete(b.ctx, azblob.DeleteSnapshotsOptionInclude, azblob.BlobAccessConditions{})
			return err
		case common.ELocation.File():
			fileURLParts := azfile.NewFileURLParts(*b.rootURL)
			fileURLParts.DirectoryOrFilePath = path.Join(fileURLParts.DirectoryOrFilePath, object.relativePath)
			fileURL := azfile.NewFileURL(fileURLParts.URL(), b.p)
			_, err := fileURL.Delete(b.ctx)
			return err
		default:
			panic("not implemented, check your code")
		}
	} else {
		if shouldSyncRemoveFolders() {
			panic("folder deletion enabled but not implemented")
		}
		return nil
	}
}
