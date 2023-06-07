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
	"encoding/json"
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-storage-azcopy/v10/azbfs"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/Azure/azure-storage-file-go/azfile"
	"net/url"
	"os"
	"path"
	"runtime"
	"strings"
)

// extract the right info from cooked arguments and instantiate a generic copy transfer processor from it
func newSyncTransferProcessor(cca *cookedSyncCmdArgs, numOfTransfersPerPart int, fpo common.FolderPropertyOption) *copyTransferProcessor {
	copyJobTemplate := &common.CopyJobPartOrderRequest{
		JobID:               cca.jobID,
		CommandString:       cca.commandString,
		FromTo:              cca.fromTo,
		Fpo:                 fpo,
		SymlinkHandlingType: cca.symlinkHandling,
		SourceRoot:          cca.source.CloneWithConsolidatedSeparators(),
		DestinationRoot:     cca.destination.CloneWithConsolidatedSeparators(),
		CredentialInfo:      cca.credentialInfo,

		// flags
		BlobAttributes: common.BlobTransferAttributes{
			PreserveLastModifiedTime: cca.preserveSMBInfo, // true by default for sync so that future syncs have this information available
			PutMd5:                   cca.putMd5,
			MD5ValidationOption:      cca.md5ValidationOption,
			BlockSizeInBytes:         cca.blockSize},
		ForceWrite:                     common.EOverwriteOption.True(), // once we decide to transfer for a sync operation, we overwrite the destination regardless
		ForceIfReadOnly:                cca.forceIfReadOnly,
		LogLevel:                       azcopyLogVerbosity,
		PreserveSMBPermissions:         cca.preservePermissions,
		PreserveSMBInfo:                cca.preserveSMBInfo,
		PreservePOSIXProperties:        cca.preservePOSIXProperties,
		S2SSourceChangeValidation:      true,
		DestLengthValidation:           true,
		S2SGetPropertiesInBackend:      true,
		S2SInvalidMetadataHandleOption: common.EInvalidMetadataHandleOption.RenameIfInvalid(),
		CpkOptions:                     cca.cpkOptions,
		S2SPreserveBlobTags:            cca.s2sPreserveBlobTags,

		S2SSourceCredentialType: cca.s2sSourceCredentialType,
		FileAttributes: common.FileTransferAttributes{
			TrailingDot: cca.trailingDot,
		},
	}

	reportFirstPart := func(jobStarted bool) { cca.setFirstPartOrdered() } // for compatibility with the way sync has always worked, we don't check jobStarted here
	reportFinalPart := func() { cca.isEnumerationComplete = true }

	// note that the source and destination, along with the template are given to the generic processor's constructor
	// this means that given an object with a relative path, this processor already knows how to schedule the right kind of transfers
	return newCopyTransferProcessor(copyJobTemplate, numOfTransfersPerPart, cca.source, cca.destination,
		reportFirstPart, reportFinalPart, cca.preserveAccessTier, cca.dryrunMode)
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

	// dryrunMode
	dryrunMode bool
}

func newDeleteTransfer(object StoredObject) (newDeleteTransfer common.CopyTransfer) {
	return common.CopyTransfer{
		Source:             object.relativePath,
		EntityType:         object.entityType,
		LastModifiedTime:   object.lastModifiedTime,
		SourceSize:         object.size,
		ContentType:        object.contentType,
		ContentEncoding:    object.contentEncoding,
		ContentDisposition: object.contentDisposition,
		ContentLanguage:    object.contentLanguage,
		CacheControl:       object.cacheControl,
		Metadata:           object.Metadata,
		BlobType:           object.blobType,
		BlobVersionID:      object.blobVersionID,
		BlobTags:           object.blobTags,
	}
}

func (d *interactiveDeleteProcessor) removeImmediately(object StoredObject) (err error) {
	if d.shouldPromptUser {
		d.shouldDelete, d.shouldPromptUser = d.promptForConfirmation(object) // note down the user's decision
	}

	if !d.shouldDelete {
		return nil
	}

	if d.dryrunMode {
		glcm.Dryrun(func(format common.OutputFormat) string {
			if format == common.EOutputFormat.Json() {
				jsonOutput, err := json.Marshal(newDeleteTransfer(object))
				common.PanicIfErr(err)
				return string(jsonOutput)
			} else {                                       // remove for sync
				if d.objectTypeToDisplay == "local file" { // removing from local src
					dryrunValue := fmt.Sprintf("DRYRUN: remove %v", common.ToShortPath(d.objectLocationToDisplay))
					if runtime.GOOS == "windows" {
						dryrunValue += "\\" + strings.ReplaceAll(object.relativePath, "/", "\\")
					} else { // linux and mac
						dryrunValue += "/" + object.relativePath
					}
					return dryrunValue
				}
				return fmt.Sprintf("DRYRUN: remove %v/%v",
					d.objectLocationToDisplay,
					object.relativePath)
			}
		})
		return nil
	}

	err = d.deleter(object)
	if err != nil {
		msg := fmt.Sprintf("error %s deleting the object %s", err.Error(), object.relativePath)
		glcm.Info(msg + "; check the scanning log file for more details")
		if azcopyScanningLogger != nil {
			azcopyScanningLogger.Log(pipeline.LogError, msg + ": " + err.Error())
		}
	}

	if d.incrementDeletionCount != nil {
		d.incrementDeletionCount()
	}
	return nil // Missing a file is an error, but it's not show-stopping. We logged it earlier; that's OK.
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
	objectTypeToDisplay string, objectLocationToDisplay common.ResourceString, incrementDeletionCounter func(), dryrun bool) *interactiveDeleteProcessor {

	return &interactiveDeleteProcessor{
		deleter:                 deleter,
		objectTypeToDisplay:     objectTypeToDisplay,
		objectLocationToDisplay: objectLocationToDisplay.Value,
		incrementDeletionCount:  incrementDeletionCounter,
		shouldPromptUser:        deleteDestination == common.EDeleteDestination.Prompt(),
		shouldDelete:            deleteDestination == common.EDeleteDestination.True(), // if shouldPromptUser is true, this will start as false, but we will determine its value later
		dryrunMode:              dryrun,
	}
}

func newSyncLocalDeleteProcessor(cca *cookedSyncCmdArgs, fpo common.FolderPropertyOption) *interactiveDeleteProcessor {
	localDeleter := localFileDeleter{rootPath: cca.destination.ValueLocal(), fpo: fpo, folderManager: common.NewFolderDeletionManager(context.Background(), fpo, azcopyScanningLogger)}
	return newInteractiveDeleteProcessor(localDeleter.deleteFile, cca.deleteDestination, "local file", cca.destination, cca.incrementDeletionCount, cca.dryrunMode)
}

type localFileDeleter struct {
	rootPath string
	fpo common.FolderPropertyOption
	folderManager common.FolderDeletionManager
}

func (l *localFileDeleter) getObjectURL(object StoredObject) *url.URL {
	return &url.URL{
		Scheme: "local",
		Path:   "/" + strings.ReplaceAll(object.relativePath, "\\", "/"), // consolidate to forward slashes
	}
}

func (l *localFileDeleter) deleteFile(object StoredObject) error {
	objectURI := l.getObjectURL(object)
	l.folderManager.RecordChildExists(objectURI)

	if object.entityType == common.EEntityType.File() {
		msg := "Deleting extra file: " + object.relativePath
		glcm.Info(msg)
		if azcopyScanningLogger != nil {
			azcopyScanningLogger.Log(pipeline.LogInfo, msg)
		}
		err := os.Remove(common.GenerateFullPath(l.rootPath, object.relativePath))
		l.folderManager.RecordChildDeleted(objectURI)
		return err
	} else if object.entityType == common.EEntityType.Folder() && l.fpo != common.EFolderPropertiesOption.NoFolders() {
		msg := "Deleting extra folder: " + object.relativePath
		glcm.Info(msg)
		if azcopyScanningLogger != nil {
			azcopyScanningLogger.Log(pipeline.LogInfo, msg)
		}

		l.folderManager.RequestDeletion(objectURI, func(ctx context.Context, logger common.ILogger) bool {
			return os.Remove(common.GenerateFullPath(l.rootPath, object.relativePath)) == nil
		})
	}

	return nil
}

func newSyncDeleteProcessor(cca *cookedSyncCmdArgs, fpo common.FolderPropertyOption) (*interactiveDeleteProcessor, error) {
	rawURL, err := cca.destination.FullURL()
	if err != nil {
		return nil, err
	}

	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	p, err := InitPipeline(ctx, cca.fromTo.To(), cca.credentialInfo, azcopyLogVerbosity.ToPipelineLogLevel(), cca.trailingDot)
	if err != nil {
		return nil, err
	}
	clientOptions := createClientOptions(azcopyLogVerbosity.ToPipelineLogLevel())

	return newInteractiveDeleteProcessor(newRemoteResourceDeleter(rawURL, p, cca.credentialInfo, clientOptions, ctx, cca.fromTo.To(), fpo, cca.forceIfReadOnly).delete,
		cca.deleteDestination, cca.fromTo.To().String(), cca.destination, cca.incrementDeletionCount, cca.dryrunMode), nil
}

type remoteResourceDeleter struct {
	rootURL        *url.URL
	p              pipeline.Pipeline
	credInfo       common.CredentialInfo
	clientOptions  azcore.ClientOptions
	ctx            context.Context
	targetLocation common.Location
	folderManager common.FolderDeletionManager
	folderOption  common.FolderPropertyOption
	forceIfReadOnly bool
}

func newRemoteResourceDeleter(rawRootURL *url.URL, p pipeline.Pipeline, credInfo common.CredentialInfo, clientOptions azcore.ClientOptions, ctx context.Context, targetLocation common.Location, fpo common.FolderPropertyOption, forceIfReadOnly bool) *remoteResourceDeleter {
	return &remoteResourceDeleter{
		rootURL:        rawRootURL,
		p:              p,
		credInfo:       credInfo,
		clientOptions:  clientOptions,
		ctx:            ctx,
		targetLocation: targetLocation,
		folderManager:  common.NewFolderDeletionManager(ctx, fpo, azcopyScanningLogger),
		folderOption: fpo,
		forceIfReadOnly: forceIfReadOnly,
	}
}

func (b *remoteResourceDeleter) getObjectURL(object StoredObject) (url url.URL) {
	switch b.targetLocation {
	case common.ELocation.Blob():
		blobURLParts, err := blob.ParseURL(b.rootURL.String())
		if err != nil {
			panic(err)
		}
		blobURLParts.BlobName = path.Join(blobURLParts.BlobName, object.relativePath)
		u, err := url.Parse(blobURLParts.String())
		if err != nil {
			panic(err)
		}
		url = *u
	case common.ELocation.File():
		fileURLParts := azfile.NewFileURLParts(*b.rootURL)
		fileURLParts.DirectoryOrFilePath = path.Join(fileURLParts.DirectoryOrFilePath, object.relativePath)
		url = fileURLParts.URL()
	case common.ELocation.BlobFS():
		blobFSURLParts := azbfs.NewBfsURLParts(*b.rootURL)
		blobFSURLParts.DirectoryOrFilePath = path.Join(blobFSURLParts.DirectoryOrFilePath, object.relativePath)
		url = blobFSURLParts.URL()
	default:
		panic("unexpected location")
	}
	return
}

func (b *remoteResourceDeleter) delete(object StoredObject) error {
	if object.entityType == common.EEntityType.File() {
		// TODO: use b.targetLocation.String() in the next line, instead of "object", if we can make it come out as string
		msg := "Deleting extra object: " + object.relativePath
		glcm.Info(msg)
		if azcopyScanningLogger != nil {
			azcopyScanningLogger.Log(pipeline.LogInfo, msg)
		}

		objectURL := b.getObjectURL(object)
		b.folderManager.RecordChildExists(&objectURL)
		defer b.folderManager.RecordChildDeleted(&objectURL)

		var err error
		switch b.targetLocation {
		case common.ELocation.Blob():
			blobURLParts, err := blob.ParseURL(b.rootURL.String())
			if err != nil {
				return err
			}
			blobURLParts.BlobName = path.Join(blobURLParts.BlobName, object.relativePath)

			blobClient := common.CreateBlobClient(blobURLParts.String(), b.credInfo, nil, b.clientOptions)
			_, err = blobClient.Delete(b.ctx, nil)
			return err
		case common.ELocation.File():
			fileURLParts := azfile.NewFileURLParts(*b.rootURL)
			fileURLParts.DirectoryOrFilePath = path.Join(fileURLParts.DirectoryOrFilePath, object.relativePath)

			fileURL := azfile.NewFileURL(fileURLParts.URL(), b.p)

			_, err = fileURL.Delete(b.ctx)

			if stgErr, ok := err.(azfile.StorageError); b.forceIfReadOnly && ok && stgErr.ServiceCode() == azfile.ServiceCodeReadOnlyAttribute {
				msg := fmt.Sprintf("read-only attribute detected, removing it before deleting the file %s", object.relativePath)
				if azcopyScanningLogger != nil {
					azcopyScanningLogger.Log(pipeline.LogInfo, msg)
				}

				// if the file is read-only, we need to remove the read-only attribute before we can delete it
				noAttrib := azfile.FileAttributeNone
				_, err = fileURL.SetHTTPHeaders(b.ctx, azfile.FileHTTPHeaders{SMBProperties: azfile.SMBProperties{FileAttributes: &noAttrib}})
				if err == nil {
					_, err = fileURL.Delete(b.ctx)
				} else {
					msg := fmt.Sprintf("error %s removing the read-only attribute from the file %s", err.Error(), object.relativePath)
					glcm.Info(msg + "; check the scanning log file for more details")
					if azcopyScanningLogger != nil {
						azcopyScanningLogger.Log(pipeline.LogError, msg + ": " + err.Error())
					}
				}
			}
		case common.ELocation.BlobFS():
			bfsURLParts := azbfs.NewBfsURLParts(*b.rootURL)
			bfsURLParts.DirectoryOrFilePath = path.Join(bfsURLParts.DirectoryOrFilePath, object.relativePath)
			fileURL := azbfs.NewFileURL(bfsURLParts.URL(), b.p)
			_, err = fileURL.Delete(b.ctx)
		default:
			panic("not implemented, check your code")
		}

		if err != nil {
			msg := fmt.Sprintf("error %s deleting the object %s", err.Error(), object.relativePath)
			glcm.Info(msg + "; check the scanning log file for more details")
			if azcopyScanningLogger != nil {
				azcopyScanningLogger.Log(pipeline.LogError, msg + ": " + err.Error())
			}
		}

		return nil
	} else {
		if b.folderOption == common.EFolderPropertiesOption.NoFolders() {
			return nil
		}

		objectURL := b.getObjectURL(object)
		b.folderManager.RecordChildExists(&objectURL)

		b.folderManager.RequestDeletion(&objectURL, func(ctx context.Context, logger common.ILogger) bool {
			var err error
			switch b.targetLocation {
			case common.ELocation.Blob():
				blobClient := common.CreateBlobClient(objectURL.String(), b.credInfo, nil, b.clientOptions)
				// HNS endpoint doesn't like delete snapshots on a directory
				_, err = blobClient.Delete(b.ctx, nil)
			case common.ELocation.File():
				dirURL := azfile.NewDirectoryURL(objectURL, b.p)
				_, err = dirURL.Delete(ctx)

				if stgErr, ok := err.(azfile.StorageError); b.forceIfReadOnly && ok && stgErr.ServiceCode() == azfile.ServiceCodeReadOnlyAttribute {
					msg := fmt.Sprintf("read-only attribute detected, removing it before deleting the file %s", object.relativePath)
					if azcopyScanningLogger != nil {
						azcopyScanningLogger.Log(pipeline.LogInfo, msg)
					}

					// if the file is read-only, we need to remove the read-only attribute before we can delete it
					noAttrib := azfile.FileAttributeNone
					_, err = dirURL.SetProperties(b.ctx, azfile.SMBProperties{FileAttributes: &noAttrib})
					if err == nil {
						_, err = dirURL.Delete(b.ctx)
					} else {
						msg := fmt.Sprintf("error %s removing the read-only attribute from the file %s", err.Error(), object.relativePath)
						glcm.Info(msg + "; check the scanning log file for more details")
						if azcopyScanningLogger != nil {
							azcopyScanningLogger.Log(pipeline.LogError, msg + ": " + err.Error())
						}
					}
				}
			case common.ELocation.BlobFS():
				dirURL := azbfs.NewDirectoryURL(objectURL, b.p)
				_, err = dirURL.Delete(ctx, nil, false)
			default:
				panic("not implemented, check your code")
			}

			return err == nil
		})

		return nil
	}
}