package azcopy

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
)

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

func (s *syncer) newRemoteDeleter() (*interactiveDeleter, error) {
	rawURL, err := s.opts.destination.String()
	if err != nil {
		return nil, err
	}

	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	deleter, err := newRemoteResourceDeleter(ctx, s.opts.destServiceClient, rawURL, s.opts.FromTo.To(), s.opts.folderPropertyOption, s.opts.ForceIfReadOnly)
	if err != nil {
		return nil, err
	}

	return newInteractiveDeleter(deleter.delete, s.opts.DeleteDestination, s.opts.FromTo.To().String(), s.opts.destination, s), nil
}

func newRemoteResourceDeleter(ctx context.Context, remoteClient *common.ServiceClient, rawRootURL string, targetLocation common.Location, fpo common.FolderPropertyOption, forceIfReadOnly bool) (*remoteResourceDeleter, error) {
	containerName, rootPath, err := common.SplitContainerNameFromPath(rawRootURL)
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
	if b.targetLocation == common.ELocation.BlobFS() && object.entityType == common.EEntityType.Folder() {
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
		common.GetLifecycleMgr().Info(msg)
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
			common.GetLifecycleMgr().Info(msg + "; check the scanning log file for more details")
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
