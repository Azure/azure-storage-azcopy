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
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
)

// extract the right info from cooked arguments and instantiate a generic copy transfer processor from it
func newSyncTransferProcessor(cca *cookedSyncCmdArgs,
	numOfTransfersPerPart int,
	fpo common.FolderPropertyOption,
	copyJobTemplate *common.CopyJobPartOrderRequest) *copyTransferProcessor {
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
	deleter traverser.ObjectProcessor

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

func (d *interactiveDeleteProcessor) removeImmediately(object traverser.StoredObject) (err error) {
	if d.shouldPromptUser {
		d.shouldDelete, d.shouldPromptUser = d.promptForConfirmation(object) // note down the user's decision
	}

	if !d.shouldDelete {
		return nil
	}

	if d.dryrunMode {
		glcm.Dryrun(func(format common.OutputFormat) string {
			if format == common.EOutputFormat.Json() {
				deleteTarget := common.ELocation.Local()
				if d.objectTypeToDisplay != LocalFileObjectType {
					_ = deleteTarget.Parse(d.objectTypeToDisplay)
				}

				tx := DryrunTransfer{
					Source:     common.GenerateFullPath(d.objectLocationToDisplay, object.RelativePath),
					BlobType:   common.FromBlobType(object.BlobType),
					EntityType: object.EntityType,
					FromTo:     common.FromToValue(deleteTarget, common.ELocation.Unknown()),
				}

				jsonOutput, err := json.Marshal(tx)
				common.PanicIfErr(err)
				return string(jsonOutput)
			} else { // remove for sync
				return fmt.Sprintf("DRYRUN: remove %v",
					common.GenerateFullPath(d.objectLocationToDisplay, object.RelativePath))
			}
		})
		return nil
	}

	err = d.deleter(object)
	if err != nil {
		msg := fmt.Sprintf("error %s deleting the object %s", err.Error(), object.RelativePath)
		glcm.Info(msg + "; check the scanning log file for more details")
		if common.AzcopyScanningLogger != nil {
			common.AzcopyScanningLogger.Log(common.LogError, msg+": "+err.Error())
		}
	}

	if d.incrementDeletionCount != nil {
		d.incrementDeletionCount()
	}
	return nil // Missing a file is an error, but it's not show-stopping. We logged it earlier; that's OK.
}

func (d *interactiveDeleteProcessor) promptForConfirmation(object traverser.StoredObject) (shouldDelete bool, keepPrompting bool) {
	answer := glcm.Prompt(fmt.Sprintf("The %s '%s' does not exist at the source. "+
		"Do you wish to delete it from the destination(%s)?",
		d.objectTypeToDisplay, object.RelativePath, d.objectLocationToDisplay),
		common.PromptDetails{
			PromptType:   common.EPromptType.DeleteDestination(),
			PromptTarget: object.RelativePath,
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
		glcm.Info(fmt.Sprintf("Keeping extra %s: %s", d.objectTypeToDisplay, object.RelativePath))
		return false, true
	case common.EResponseOption.NoForAll():
		glcm.Info("No deletions will happen from now onwards.")
		return false, false
	default:
		glcm.Info(fmt.Sprintf("Unrecognizable answer, keeping extra %s: %s.", d.objectTypeToDisplay, object.RelativePath))
		return false, true
	}
}

func newInteractiveDeleteProcessor(deleter traverser.ObjectProcessor, deleteDestination common.DeleteDestination,
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

const LocalFileObjectType = "local file"

func newSyncLocalDeleteProcessor(cca *cookedSyncCmdArgs, fpo common.FolderPropertyOption) *interactiveDeleteProcessor {
	localDeleter := localFileDeleter{rootPath: cca.destination.ValueLocal(), fpo: fpo, folderManager: common.NewFolderDeletionManager(context.Background(), fpo, common.AzcopyScanningLogger)}
	return newInteractiveDeleteProcessor(localDeleter.deleteFile, cca.deleteDestination, LocalFileObjectType, cca.destination, cca.incrementDeletionCount, cca.dryrunMode)
}

type localFileDeleter struct {
	rootPath      string
	fpo           common.FolderPropertyOption
	folderManager common.FolderDeletionManager
}

func (l *localFileDeleter) getObjectURL(object traverser.StoredObject) *url.URL {
	return &url.URL{
		Scheme: "local",
		Path:   "/" + strings.ReplaceAll(object.RelativePath, "\\", "/"), // consolidate to forward slashes
	}
}

func (l *localFileDeleter) deleteFile(object traverser.StoredObject) error {
	objectURI := l.getObjectURL(object)
	l.folderManager.RecordChildExists(objectURI)

	if object.EntityType == common.EEntityType.File() {
		msg := "Deleting extra file: " + object.RelativePath
		glcm.Info(msg)
		if common.AzcopyScanningLogger != nil {
			common.AzcopyScanningLogger.Log(common.LogInfo, msg)
		}
		err := os.Remove(common.GenerateFullPath(l.rootPath, object.RelativePath))
		l.folderManager.RecordChildDeleted(objectURI)
		return err
	} else if object.EntityType == common.EEntityType.Folder() && l.fpo != common.EFolderPropertiesOption.NoFolders() {
		msg := "Deleting extra folder: " + object.RelativePath
		glcm.Info(msg)
		if common.AzcopyScanningLogger != nil {
			common.AzcopyScanningLogger.Log(common.LogInfo, msg)
		}

		l.folderManager.RequestDeletion(objectURI, func(ctx context.Context, logger common.ILogger) bool {
			return os.Remove(common.GenerateFullPath(l.rootPath, object.RelativePath)) == nil
		})
	}

	return nil
}

func newSyncDeleteProcessor(cca *cookedSyncCmdArgs, fpo common.FolderPropertyOption, dstClient *common.ServiceClient) (*interactiveDeleteProcessor, error) {
	rawURL, err := cca.destination.FullURL()
	if err != nil {
		return nil, err
	}

	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	deleter, err := newRemoteResourceDeleter(ctx, dstClient, rawURL, cca.fromTo.To(), fpo, cca.forceIfReadOnly)
	if err != nil {
		return nil, err
	}

	return newInteractiveDeleteProcessor(deleter.delete, cca.deleteDestination, cca.fromTo.To().String(), cca.destination, cca.incrementDeletionCount, cca.dryrunMode), nil
}

type remoteResourceDeleter struct {
	remoteClient    *common.ServiceClient
	containerName   string // name of target container/share/filesystem
	rootPath        string
	ctx             context.Context
	targetLocation  common.Location
	folderManager   common.FolderDeletionManager
	folderOption    common.FolderPropertyOption
	forceIfReadOnly bool
}

func newRemoteResourceDeleter(ctx context.Context, remoteClient *common.ServiceClient, rawRootURL *url.URL, targetLocation common.Location, fpo common.FolderPropertyOption, forceIfReadOnly bool) (*remoteResourceDeleter, error) {
	containerName, rootPath, err := common.SplitContainerNameFromPath(rawRootURL.String())
	if err != nil {
		return nil, err
	}
	return &remoteResourceDeleter{
		containerName:   containerName,
		rootPath:        rootPath,
		remoteClient:    remoteClient,
		ctx:             ctx,
		targetLocation:  targetLocation,
		folderManager:   common.NewFolderDeletionManager(ctx, fpo, common.AzcopyScanningLogger),
		folderOption:    fpo,
		forceIfReadOnly: forceIfReadOnly,
	}, nil
}

func (b *remoteResourceDeleter) getObjectURL(objectURL string) (*url.URL, error) {
	u, err := url.Parse(objectURL)
	if err != nil {
		return nil, err
	}
	return u, nil
}

func (b *remoteResourceDeleter) delete(object traverser.StoredObject) error {
	/* knarasim: This needs to be taken care of
	if b.targetLocation == common.ELocation.BlobFS() && object.EntityType == common.EEntityType.Folder() {
		b.clientOptions.PerCallPolicies = append([]policy.Policy{common.NewRecursivePolicy()}, b.clientOptions.PerCallPolicies...)
	}
	*/
	objectPath := path.Join(b.rootPath, object.RelativePath)
	if object.RelativePath == "\x00" && b.targetLocation != common.ELocation.Blob() {
		return nil // Do nothing, we don't want to accidentally delete the root.
	} else if object.RelativePath == "\x00" { // this is acceptable on blob, though. Dir stubs are a thing, and they aren't necessary for normal function.
		objectPath = b.rootPath
	}

	if strings.HasSuffix(object.RelativePath, "/") && !strings.HasSuffix(objectPath, "/") && b.targetLocation == common.ELocation.Blob() {
		// If we were targeting a directory, we still need to be. path.join breaks that.
		// We also want to defensively code around this, and make sure we are not putting folder// or trying to put a weird URI in to an endpoint that can't do this.
		objectPath += "/"
	}

	sc := b.remoteClient
	if object.EntityType == common.EEntityType.File() {
		// TODO: use b.targetLocation.String() in the next line, instead of "object", if we can make it come out as string
		msg := "Deleting extra object: " + object.RelativePath
		glcm.Info(msg)
		if common.AzcopyScanningLogger != nil {
			common.AzcopyScanningLogger.Log(common.LogInfo, msg)
		}

		var err error
		var objURL *url.URL

		switch b.targetLocation {
		case common.ELocation.Blob():
			bsc, _ := sc.BlobServiceClient()
			var blobClient *blob.Client = bsc.NewContainerClient(b.containerName).NewBlobClient(objectPath)

			objURL, err = b.getObjectURL(blobClient.URL())
			if err != nil {
				break
			}
			b.folderManager.RecordChildExists(objURL)
			defer b.folderManager.RecordChildDeleted(objURL)

			_, err = blobClient.Delete(b.ctx, nil)
		case common.ELocation.File(), common.ELocation.FileNFS():
			fsc, _ := sc.FileServiceClient()
			fileClient := fsc.NewShareClient(b.containerName).NewRootDirectoryClient().NewFileClient(objectPath)

			objURL, err = b.getObjectURL(fileClient.URL())
			if err != nil {
				break
			}
			b.folderManager.RecordChildExists(objURL)
			defer b.folderManager.RecordChildDeleted(objURL)

			err = common.DoWithOverrideReadOnlyOnAzureFiles(b.ctx, func() (interface{}, error) {
				return fileClient.Delete(b.ctx, nil)
			}, fileClient, b.forceIfReadOnly)
		case common.ELocation.BlobFS():
			dsc, _ := sc.DatalakeServiceClient()
			fileClient := dsc.NewFileSystemClient(b.containerName).NewFileClient(objectPath)

			objURL, err = b.getObjectURL(fileClient.DFSURL())
			if err != nil {
				break
			}
			b.folderManager.RecordChildExists(objURL)
			defer b.folderManager.RecordChildDeleted(objURL)

			_, err = fileClient.Delete(b.ctx, nil)
		default:
			panic("not implemented, check your code")
		}

		if err != nil {
			msg := fmt.Sprintf("error %s deleting the object %s", err.Error(), object.RelativePath)
			glcm.Info(msg + "; check the scanning log file for more details")
			if common.AzcopyScanningLogger != nil {
				common.AzcopyScanningLogger.Log(common.LogError, msg+": "+err.Error())
			}

			return err
		}

		return nil
	} else if object.EntityType == common.EEntityType.Symlink() {
		msg := "Deleting extra object: " + object.RelativePath
		glcm.Info(msg)
		if common.AzcopyScanningLogger != nil {
			common.AzcopyScanningLogger.Log(common.LogInfo, msg)
		}

		var err error
		var objURL *url.URL

		switch b.targetLocation {
		case common.ELocation.FileNFS():
			fsc, _ := sc.FileServiceClient()
			fileClient := fsc.NewShareClient(b.containerName).NewRootDirectoryClient().NewFileClient(objectPath)

			objURL, err = b.getObjectURL(fileClient.URL())
			if err != nil {
				break
			}
			b.folderManager.RecordChildExists(objURL)
			defer b.folderManager.RecordChildDeleted(objURL)

			err = common.DoWithOverrideReadOnlyOnAzureFiles(b.ctx, func() (interface{}, error) {
				return fileClient.Delete(b.ctx, nil)
			}, fileClient, b.forceIfReadOnly)
		default:
			panic("not implemented, check your code")
		}

		if err != nil {
			msg := fmt.Sprintf("error %s deleting the object %s", err.Error(), object.RelativePath)
			glcm.Info(msg + "; check the scanning log file for more details")
			if common.AzcopyScanningLogger != nil {
				common.AzcopyScanningLogger.Log(common.LogError, msg+": "+err.Error())
			}

			return err
		}

		return nil
	} else {
		if b.folderOption == common.EFolderPropertiesOption.NoFolders() {
			return nil
		}

		var deleteFunc func(ctx context.Context, logger common.ILogger) bool
		var objURL *url.URL
		var err error
		switch b.targetLocation {
		case common.ELocation.Blob():
			bsc, _ := sc.BlobServiceClient()
			blobClient := bsc.NewContainerClient(b.containerName).NewBlobClient(objectPath)
			// HNS endpoint doesn't like delete snapshots on a directory
			objURL, err = b.getObjectURL(blobClient.URL())
			if err != nil {
				return err
			}

			deleteFunc = func(ctx context.Context, logger common.ILogger) bool {
				_, err = blobClient.Delete(b.ctx, nil)
				return (err == nil)
			}
		case common.ELocation.File(), common.ELocation.FileNFS():
			fsc, _ := sc.FileServiceClient()
			dirClient := fsc.NewShareClient(b.containerName).NewDirectoryClient(objectPath)
			objURL, err = b.getObjectURL(dirClient.URL())
			if err != nil {
				return err
			}

			deleteFunc = func(ctx context.Context, logger common.ILogger) bool {
				err = common.DoWithOverrideReadOnlyOnAzureFiles(b.ctx, func() (interface{}, error) {
					return dirClient.Delete(b.ctx, nil)
				}, dirClient, b.forceIfReadOnly)
				return (err == nil)
			}
		case common.ELocation.BlobFS():
			dsc, _ := sc.DatalakeServiceClient()
			directoryClient := dsc.NewFileSystemClient(b.containerName).NewDirectoryClient(objectPath)
			objURL, err = b.getObjectURL(directoryClient.DFSURL())
			if err != nil {
				return err
			}

			deleteFunc = func(ctx context.Context, logger common.ILogger) bool {
				recursiveContext := common.WithRecursive(b.ctx, false)
				_, err = directoryClient.Delete(recursiveContext, nil)
				return (err == nil)
			}
		default:
			panic("not implemented, check your code")
		}

		b.folderManager.RecordChildExists(objURL)
		b.folderManager.RequestDeletion(objURL, deleteFunc)

		return nil
	}
}
