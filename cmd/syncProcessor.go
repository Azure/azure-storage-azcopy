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
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/filesystem"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/directory"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
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
				deleteTarget := common.ELocation.Local()
				if d.objectTypeToDisplay != LocalFileObjectType {
					_ = deleteTarget.Parse(d.objectTypeToDisplay)
				}

				tx := DryrunTransfer{
					Source:     common.GenerateFullPath(d.objectLocationToDisplay, object.relativePath),
					BlobType:   common.FromBlobType(object.blobType),
					EntityType: object.entityType,
					FromTo:     common.FromToValue(deleteTarget, common.ELocation.Unknown()),
				}

				jsonOutput, err := json.Marshal(tx)
				common.PanicIfErr(err)
				return string(jsonOutput)
			} else { // remove for sync
				return fmt.Sprintf("DRYRUN: remove %v",
					common.GenerateFullPath(d.objectLocationToDisplay, object.relativePath))
			}
		})
		return nil
	}

	err = d.deleter(object)
	if err != nil {
		msg := fmt.Sprintf("error %s deleting the object %s", err.Error(), object.relativePath)
		glcm.Info(msg + "; check the scanning log file for more details")
		if azcopyScanningLogger != nil {
			azcopyScanningLogger.Log(common.LogError, msg+": "+err.Error())
		}
	}

	if d.incrementDeletionCount != nil {
		// XDM: Why are we incrementing even in case of an error?
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

const LocalFileObjectType = "local file"

func newSyncLocalDeleteProcessor(cca *cookedSyncCmdArgs, fpo common.FolderPropertyOption) *interactiveDeleteProcessor {
	localDeleter := localFileDeleter{
		rootPath:               cca.destination.ValueLocal(),
		fpo:                    fpo,
		folderManager:          common.NewFolderDeletionManager(context.Background(), fpo, azcopyScanningLogger),
		incrementDeletionCount: cca.incrementDeletionCount,
	}

	return newInteractiveDeleteProcessor(
		localDeleter.deleteFile,
		cca.deleteDestination,
		LocalFileObjectType,
		cca.destination,
		cca.incrementDeletionCount,
		cca.dryrunMode)
}

type localFileDeleter struct {
	rootPath      string
	fpo           common.FolderPropertyOption
	folderManager common.FolderDeletionManager

	// count the deletions that happened
	incrementDeletionCount func()
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
			azcopyScanningLogger.Log(common.LogInfo, msg)
		}
		err := os.Remove(common.GenerateFullPath(l.rootPath, object.relativePath))
		l.folderManager.RecordChildDeleted(objectURI)
		return err
	} else if object.entityType == common.EEntityType.Folder() && l.fpo != common.EFolderPropertiesOption.NoFolders() {
		msg := "Deleting extra folder: " + object.relativePath
		glcm.Info(msg)
		if azcopyScanningLogger != nil {
			azcopyScanningLogger.Log(common.LogInfo, msg)
		}

		l.folderManager.RequestDeletion(objectURI, func(ctx context.Context, logger common.ILogger) bool {
			return os.Remove(common.GenerateFullPath(l.rootPath, object.relativePath)) == nil
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

	deleter, err := newRemoteResourceDeleter(ctx, dstClient, rawURL, cca.fromTo.To(), fpo, cca.forceIfReadOnly, cca.incrementDeletionCount)
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

	// count the deletions that happened
	incrementDeletionCount func()

	recursive bool // whether to delete recursively or not
}

func newRemoteResourceDeleter(
	ctx context.Context,
	remoteClient *common.ServiceClient,
	rawRootURL *url.URL,
	targetLocation common.Location,
	fpo common.FolderPropertyOption,
	forceIfReadOnly bool,
	incrementDeleteCounter func()) (*remoteResourceDeleter, error) {
	containerName, rootPath, err := common.SplitContainerNameFromPath(rawRootURL.String())
	if err != nil {
		return nil, err
	}
	deleter := remoteResourceDeleter{
		containerName:          containerName,
		rootPath:               rootPath,
		remoteClient:           remoteClient,
		ctx:                    ctx,
		targetLocation:         targetLocation,
		folderManager:          common.NewFolderDeletionManager(ctx, fpo, azcopyScanningLogger),
		folderOption:           fpo,
		forceIfReadOnly:        forceIfReadOnly,
		incrementDeletionCount: incrementDeleteCounter,
	}
	// If we are using SyncOrchestrator, we want to delete non-empty folder recursively.
	deleter.recursive = UseSyncOrchestrator
	return &deleter, nil
}

func (b *remoteResourceDeleter) getObjectURL(objectURL string) (*url.URL, error) {
	u, err := url.Parse(objectURL)
	if err != nil {
		return nil, err
	}
	return u, nil
}

func (b *remoteResourceDeleter) delete(object StoredObject) error {
	/* knarasim: This needs to be taken care of
	if b.targetLocation == common.ELocation.BlobFS() && object.entityType == common.EEntityType.Folder() {
		b.clientOptions.PerCallPolicies = append([]policy.Policy{common.NewRecursivePolicy()}, b.clientOptions.PerCallPolicies...)
	}
	*/
	objectPath := path.Join(b.rootPath, object.relativePath)
	if object.relativePath == "\x00" && b.targetLocation != common.ELocation.Blob() {
		return nil // Do nothing, we don't want to accidentally delete the root.
	} else if object.relativePath == "\x00" { // this is acceptable on blob, though. Dir stubs are a thing, and they aren't necessary for normal function.
		objectPath = b.rootPath
	}

	if strings.HasSuffix(object.relativePath, "/") && !strings.HasSuffix(objectPath, "/") && b.targetLocation == common.ELocation.Blob() {
		// If we were targeting a directory, we still need to be. path.join breaks that.
		// We also want to defensively code around this, and make sure we are not putting folder// or trying to put a weird URI in to an endpoint that can't do this.
		objectPath += "/"
	}

	sc := b.remoteClient
	if object.entityType == common.EEntityType.File() {
		// TODO: use b.targetLocation.String() in the next line, instead of "object", if we can make it come out as string
		msg := "Deleting extra object: " + object.relativePath
		glcm.Info(msg)
		if azcopyScanningLogger != nil {
			azcopyScanningLogger.Log(common.LogInfo, msg)
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
			msg := fmt.Sprintf("error %s deleting the object %s", err.Error(), object.relativePath)
			glcm.Info(msg + "; check the scanning log file for more details")
			if azcopyScanningLogger != nil {
				azcopyScanningLogger.Log(common.LogError, msg+": "+err.Error())
			}

			return err
		}
	} else {
		if !b.recursive && b.folderOption == common.EFolderPropertiesOption.NoFolders() {
			// If deletion is not recursive and secondary location is not folder aware
			// we just return without any work
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

			if b.recursive {
				cc := bsc.NewContainerClient(b.containerName)
				if err := b.RegisterFolderContentsForDeletion(
					cc,
					objectPath,
					b.folderManager,
					b.remoteClient,
					b.containerName); err != nil {
					return fmt.Errorf("failed to register folder contents for deletion: %v", err)
				}
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

			if b.recursive {
				shareClient := fsc.NewShareClient(b.containerName)
				if err := b.RegisterFileFolderContentsForDeletion(
					shareClient,
					objectPath,
					b.folderManager,
					b.remoteClient,
					b.containerName); err != nil {
					return fmt.Errorf("failed to register Azure Files folder contents for deletion: %v", err)
				}
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

			// The above deletion function deletes the whole directory along with its contents.
			// but we still count this as 1 deletion. We can potentially count the number of objects
			// under this directory, but that would require an additional enumeration and doesn't seem
			// worth the overhead in this context.
			// the function countBlobFSItems below can be used to count the items in the directory
		default:
			panic("not implemented, check your code")
		}

		b.folderManager.RecordChildExists(objURL)
		b.folderManager.RequestDeletion(objURL, deleteFunc)
	}

	return nil
}

// #region Recurive folder deletion for Azure Blobs

const (
	MaxDeletionWorkers    = 50  // Limit concurrent deletions
	DeletionChannelBuffer = 200 // Buffer for work items
)

type DeletionTask struct {
	BlobURL      *url.URL
	DeletionFunc func()
}

// Replace the direct deletion approach with folder manager registration
func (b *remoteResourceDeleter) RegisterFolderContentsForDeletion(
	containerClient *container.Client,
	folderPrefix string,
	folderManager common.FolderDeletionManager,
	remoteClient *common.ServiceClient,
	containerName string) error {

	ctx := context.Background()
	return b.enumerateAndRegisterBlobs(ctx, containerClient, folderPrefix, folderManager, remoteClient, containerName)
}

func (b *remoteResourceDeleter) deletionWorker(ctx context.Context, taskChan <-chan DeletionTask,
	folderManager common.FolderDeletionManager, wg *sync.WaitGroup) {

	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case task, ok := <-taskChan:
			if !ok {
				return // Channel closed, no more work
			}

			// Process the deletion
			task.DeletionFunc()
			folderManager.RecordChildDeleted(task.BlobURL)
		}
	}
}

func (b *remoteResourceDeleter) enumerateAndRegisterBlobs(
	ctx context.Context,
	containerClient *container.Client,
	folderPrefix string,
	folderManager common.FolderDeletionManager,
	remoteClient *common.ServiceClient,
	containerName string) error {

	// Create buffered channel for work distribution
	taskChan := make(chan DeletionTask, DeletionChannelBuffer)

	// Start fixed number of worker goroutines
	var workerWg sync.WaitGroup
	for i := 0; i < MaxDeletionWorkers; i++ {
		workerWg.Add(1)
		go b.deletionWorker(ctx, taskChan, folderManager, &workerWg)
	}

	// Enumerate and queue work (this is the producer)
	go func() {
		defer close(taskChan) // Signal workers to stop when done

		// Use iterative approach to avoid deep recursion
		foldersToProcess := []string{folderPrefix}
		processedCount := 0

		for len(foldersToProcess) > 0 {
			currentFolder := foldersToProcess[0]
			foldersToProcess = foldersToProcess[1:]

			// List blobs hierarchically
			pager := containerClient.NewListBlobsHierarchyPager("/",
				&container.ListBlobsHierarchyOptions{
					Prefix:     &currentFolder,
					MaxResults: &[]int32{1000}[0],
				})

			for pager.More() {
				page, err := pager.NextPage(ctx)
				if err != nil {
					// return fmt.Errorf("Enumeration failed (recursive blob directory delete) %s: %v", currentFolder, err)
					// We don't want to stop the entire process if one folder fails, so we log and continue
					// Idea is that we will catch it in a later sync run. This is in context of a mover sync job
				}

				// Register subdirectories for processing and with folder manager
				for _, blobPrefix := range page.Segment.BlobPrefixes {
					subFolderName := *blobPrefix.Name
					foldersToProcess = append(foldersToProcess, subFolderName)

					// Register the subdirectory with folder manager
					if err := b.registerDirectoryForDeletion(subFolderName, folderManager,
						remoteClient, containerName); err != nil {
						// return err
						// Skip this subdirectory and continue processing others
					}
				}

				// Register blobs with folder manager
				for _, blob := range page.Segment.BlobItems {
					blobName := *blob.Name

					// Create URL for the blob
					bsc, _ := remoteClient.BlobServiceClient()
					blobClient := bsc.NewContainerClient(containerName).NewBlobClient(blobName)
					blobURL, err := url.Parse(blobClient.URL())
					if err != nil {
						continue
						// return fmt.Errorf("failed to parse blob URL %s: %v", blobName, err)
						// Skip this blob and do not stop the entire process
					}

					// Register with folder manager
					folderManager.RecordChildExists(blobURL)

					// Create a deletion function for this specific blob
					deletionFunc := b.createBlobDeletionFunc(blobClient, blobName)

					// Queue work instead of creating unlimited goroutines
					select {
					case taskChan <- DeletionTask{BlobURL: blobURL, DeletionFunc: deletionFunc}:
						processedCount++
					case <-ctx.Done():
						return
					}
				}
			}
		}

		// Progress logging
		if processedCount > 0 && processedCount%1000 == 0 {
			msg := fmt.Sprintf("Registered %d items for deletion, %d folders remaining",
				processedCount, len(foldersToProcess))
			if azcopyScanningLogger != nil {
				azcopyScanningLogger.Log(common.LogError, msg)
			}
		}

	}()

	// Wait for all workers to complete
	workerWg.Wait()
	return nil
}

func (b *remoteResourceDeleter) createBlobDeletionFunc(blobClient *blob.Client, blobName string) func() {
	return func() {
		ctx := context.Background()

		if _, err := blobClient.Delete(ctx, nil); err != nil {
			msg := fmt.Sprintf("Failed to delete blob %s: %v", blobName, err)
			if azcopyScanningLogger != nil {
				azcopyScanningLogger.Log(common.LogError, msg)
			}
		}

		if b.incrementDeletionCount != nil {
			b.incrementDeletionCount()
		}
	}
}

func (b *remoteResourceDeleter) registerDirectoryForDeletion(dirPath string, folderManager common.FolderDeletionManager,
	remoteClient *common.ServiceClient, containerName string) error {

	// Create URL for the directory
	bsc, _ := remoteClient.BlobServiceClient()
	blobClient := bsc.NewContainerClient(containerName).NewBlobClient(dirPath)
	dirURL, err := url.Parse(blobClient.URL())
	if err != nil {
		return fmt.Errorf("failed to parse directory URL %s: %v", dirPath, err)
	}

	// Register directory existence
	folderManager.RecordChildExists(dirURL)

	// Create deletion function for the directory
	deleteFunc := func(ctx context.Context, logger common.ILogger) bool {

		// For blob directories, try to delete the directory marker blob
		if _, err := blobClient.Delete(ctx, nil); err != nil {
			// Directory markers might not exist, which is fine
		}

		if b.folderOption != common.EFolderPropertiesOption.NoFolders() &&
			b.incrementDeletionCount != nil {
			b.incrementDeletionCount()
		}
		return true // Always return true as directory "deletion" is best effort
	}

	// Register deletion request
	folderManager.RequestDeletion(dirURL, deleteFunc)

	return nil
}

// #endregion Recurive folder deletion for Azure Blobs

// #region Recurive folder deletion for Azure Files
const (
	MaxFileDeletionWorkers    = 30  // Slightly lower than blob due to Files API limits
	FileDeletionChannelBuffer = 150 // Buffer for file deletion work items
)

type FileDeletionTask struct {
	FileURL      *url.URL
	DeletionFunc func()
	IsDirectory  bool
}

// Separate implementation for Azure Files folder deletion
func (b *remoteResourceDeleter) RegisterFileFolderContentsForDeletion(
	shareClient *share.Client,
	folderPrefix string,
	folderManager common.FolderDeletionManager,
	remoteClient *common.ServiceClient,
	shareName string) error {

	ctx := context.Background()
	return b.enumerateAndRegisterFiles(ctx, shareClient, folderPrefix, folderManager, remoteClient, shareName)
}

func (b *remoteResourceDeleter) fileDeletionWorker(
	ctx context.Context,
	taskChan <-chan FileDeletionTask,
	folderManager common.FolderDeletionManager,
	wg *sync.WaitGroup) {

	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case task, ok := <-taskChan:
			if !ok {
				return // Channel closed, no more work
			}

			// Process the deletion
			task.DeletionFunc()
			if !task.IsDirectory {
				// Only record file deletions immediately
				// Directory deletions are handled by folder manager
				folderManager.RecordChildDeleted(task.FileURL)
			}
		}
	}
}

func (b *remoteResourceDeleter) enumerateAndRegisterFiles(
	ctx context.Context,
	shareClient *share.Client,
	folderPrefix string,
	folderManager common.FolderDeletionManager,
	remoteClient *common.ServiceClient,
	shareName string) error {

	// Create buffered channel for work distribution
	taskChan := make(chan FileDeletionTask, FileDeletionChannelBuffer)

	// Start fixed number of worker goroutines
	var workerWg sync.WaitGroup
	for i := 0; i < MaxFileDeletionWorkers; i++ {
		workerWg.Add(1)
		go b.fileDeletionWorker(ctx, taskChan, folderManager, &workerWg)
	}

	// Enumerate and queue work (this is the producer)
	go func() {
		defer close(taskChan) // Signal workers to stop when done

		// Use iterative approach to avoid deep recursion
		foldersToProcess := []string{folderPrefix}
		processedFileCount := 0
		processedDirCount := 0

		for len(foldersToProcess) > 0 {
			currentFolder := foldersToProcess[0]
			foldersToProcess = foldersToProcess[1:]

			// Create directory client for current folder
			dirClient := shareClient.NewDirectoryClient(currentFolder)

			// List files and directories in current folder
			pager := dirClient.NewListFilesAndDirectoriesPager(&directory.ListFilesAndDirectoriesOptions{
				MaxResults: &[]int32{1000}[0],
			})

			for pager.More() {
				page, err := pager.NextPage(ctx)
				if err != nil {
					// Log error but continue processing other folders
					msg := fmt.Sprintf("Failed to enumerate Azure Files directory %s: %v", currentFolder, err)
					if azcopyScanningLogger != nil {
						azcopyScanningLogger.Log(common.LogError, msg)
					}
					break // Break from this folder's pagination, continue with next folder
				}

				// Process subdirectories first
				for _, dirInfo := range page.Segment.Directories {
					if dirInfo.Name != nil {
						subDirPath := path.Join(currentFolder, *dirInfo.Name)
						foldersToProcess = append(foldersToProcess, subDirPath)

						// Register the subdirectory with folder manager
						if err := b.registerFileDirectoryForDeletion(subDirPath, folderManager,
							remoteClient, shareName); err != nil {
							// Log error but continue processing
							msg := fmt.Sprintf("Failed to register directory %s for deletion: %v", subDirPath, err)
							if azcopyScanningLogger != nil {
								azcopyScanningLogger.Log(common.LogError, msg)
							}
						} else {
							processedDirCount++
						}
					}
				}

				// Process files in current directory
				for _, fileInfo := range page.Segment.Files {
					if fileInfo.Name != nil {
						filePath := path.Join(currentFolder, *fileInfo.Name)

						// Create file client
						fileClient := shareClient.NewDirectoryClient(currentFolder).NewFileClient(*fileInfo.Name)
						fileURL, err := url.Parse(fileClient.URL())
						if err != nil {
							// Log error but continue processing
							msg := fmt.Sprintf("Failed to parse file URL for %s: %v", filePath, err)
							if azcopyScanningLogger != nil {
								azcopyScanningLogger.Log(common.LogError, msg)
							}
							continue
						}

						// Register with folder manager
						folderManager.RecordChildExists(fileURL)
						// Create a deletion function for this specific file
						deletionFunc := b.createFileDeletionFunc(fileClient, filePath)

						// Queue work
						select {
						case taskChan <- FileDeletionTask{
							FileURL:      fileURL,
							DeletionFunc: deletionFunc,
							IsDirectory:  false,
						}:
							processedFileCount++
						case <-ctx.Done():
							return
						}
					}
				}
			}

			// Progress logging every 1000 items
			totalProcessed := processedFileCount + processedDirCount
			if totalProcessed > 0 && totalProcessed%1000 == 0 {
				msg := fmt.Sprintf("Azure Files: Registered %d files and %d directories for deletion, %d folders remaining",
					processedFileCount, processedDirCount, len(foldersToProcess))
				if azcopyScanningLogger != nil {
					azcopyScanningLogger.Log(common.LogError, msg)
				}
			}
		}

		msg := fmt.Sprintf("Azure Files: Completed registration - %d files and %d directories queued for deletion",
			processedFileCount, processedDirCount)
		if azcopyScanningLogger != nil {
			azcopyScanningLogger.Log(common.LogError, msg)
		}
	}()

	// Wait for all workers to complete
	workerWg.Wait()
	return nil
}

func (b *remoteResourceDeleter) createFileDeletionFunc(fileClient *file.Client, filePath string) func() {
	return func() {
		ctx := context.Background()

		if _, err := fileClient.Delete(ctx, nil); err != nil {
			msg := fmt.Sprintf("Failed to delete file %s: %v", filePath, err)
			if azcopyScanningLogger != nil {
				azcopyScanningLogger.Log(common.LogError, msg)
			}
		}

		// Increment deletion count
		if b.incrementDeletionCount != nil {
			b.incrementDeletionCount()
		}
	}
}

func (b *remoteResourceDeleter) registerFileDirectoryForDeletion(dirPath string, folderManager common.FolderDeletionManager,
	remoteClient *common.ServiceClient, shareName string) error {

	// Get the Azure Files service client
	fsc, err := remoteClient.FileServiceClient()
	if err != nil {
		return fmt.Errorf("failed to get Azure Files service client: %v", err)
	}

	// Create URL for the directory
	shareClient := fsc.NewShareClient(shareName)
	dirClient := shareClient.NewDirectoryClient(dirPath)
	dirURL, err := url.Parse(dirClient.URL())
	if err != nil {
		return fmt.Errorf("failed to parse directory URL %s: %v", dirPath, err)
	}

	// Register directory existence
	folderManager.RecordChildExists(dirURL)

	// Create deletion function for the directory
	deleteFunc := func(ctx context.Context, logger common.ILogger) bool {
		// Get fresh service client for the deletion
		freshFsc, err := remoteClient.FileServiceClient()
		if err != nil {
			msg := fmt.Sprintf("Failed to get Azure Files service client for deleting directory %s: %v", dirPath, err)
			if logger != nil {
				logger.Log(common.LogError, msg)
			}
			return false
		}

		// Create fresh directory client for deletion
		freshShareClient := freshFsc.NewShareClient(shareName)
		freshDirClient := freshShareClient.NewDirectoryClient(dirPath)

		// Use the same read-only override logic as the main delete function
		err = common.DoWithOverrideReadOnlyOnAzureFiles(ctx, func() (interface{}, error) {
			return freshDirClient.Delete(ctx, nil)
		}, freshDirClient, b.forceIfReadOnly)

		if err != nil {
			// Log the error but don't fail the overall operation
			msg := fmt.Sprintf("Failed to delete Azure Files directory %s: %v", dirPath, err)
			if logger != nil {
				logger.Log(common.LogError, msg)
			}
			return false
		}

		// Increment deletion count for directories
		if b.folderOption != common.EFolderPropertiesOption.NoFolders() &&
			b.incrementDeletionCount != nil {
			b.incrementDeletionCount()
		}
		return true
	}

	// Register deletion request
	folderManager.RequestDeletion(dirURL, deleteFunc)

	return nil
}

// #endregion Recurive folder deletion for Azure Files
// #region Recurive folder deletion for Blob FileSystem
// Count Blob FS items
func (b *remoteResourceDeleter) countBlobFSItems(
	ctx context.Context,
	serviceClient *common.ServiceClient,
	folderPrefix string,
	containerName string) (int64, error) {

	var counts int64 = 0

	datalakeServiceClient, err := serviceClient.DatalakeServiceClient()
	if err != nil {
		return counts, fmt.Errorf("failed to get Blob FS service client: %v", err)
	}

	filesystemClient := datalakeServiceClient.NewFileSystemClient(containerName)

	// List all paths under the folder
	pager := filesystemClient.NewListPathsPager(true, &filesystem.ListPathsOptions{
		Prefix:     &folderPrefix,
		MaxResults: &[]int32{5000}[0], // Higher limit for counting
	})

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return counts, fmt.Errorf("failed to list Blob FS paths: %v", err)
		}

		for _, pathInfo := range page.Paths {
			if pathInfo.Name == nil {
				continue
			}

			// Skip the folder itself
			if *pathInfo.Name == folderPrefix {
				continue
			}

			// if pathInfo.IsDirectory != nil && *pathInfo.IsDirectory {
			// 	counts.Directories++
			// } else {
			// 	counts.Files++
			// }

			counts++ // Count all items (both files and directories)
		}
	}

	return counts, nil
}

// #endregion Recurive folder deletion for Blob FileSystem
