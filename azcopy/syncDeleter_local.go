package azcopy

import (
	"context"
	"net/url"
	"os"
	"strings"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
)

const LocalFileObjectType = "local file"

func (s *syncer) newLocalDeleter() *interactiveDeleter {
	localDeleter := localFileDeleter{rootPath: s.opts.destination.ValueLocal(), fpo: s.opts.folderPropertyOption, folderManager: common.NewFolderDeletionManager(context.Background(), s.opts.folderPropertyOption, common.AzcopyScanningLogger)}
	return newInteractiveDeleter(localDeleter.deleteFile, s.opts.DeleteDestination, LocalFileObjectType, s.opts.destination, s)
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
		common.GetLifecycleMgr().Info(msg)
		if common.AzcopyScanningLogger != nil {
			common.AzcopyScanningLogger.Log(common.LogInfo, msg)
		}
		err := os.Remove(common.GenerateFullPath(l.rootPath, object.RelativePath))
		l.folderManager.RecordChildDeleted(objectURI)
		return err
	} else if object.EntityType == common.EEntityType.Folder() && l.fpo != common.EFolderPropertiesOption.NoFolders() {
		msg := "Deleting extra folder: " + object.RelativePath
		common.GetLifecycleMgr().Info(msg)
		if common.AzcopyScanningLogger != nil {
			common.AzcopyScanningLogger.Log(common.LogInfo, msg)
		}

		l.folderManager.RequestDeletion(objectURI, func(ctx context.Context, logger common.ILogger) bool {
			return os.Remove(common.GenerateFullPath(l.rootPath, object.RelativePath)) == nil
		})
	}

	return nil
}
