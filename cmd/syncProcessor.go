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
