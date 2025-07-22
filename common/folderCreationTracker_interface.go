package common

import "context"

// folderCreationTracker is used to ensure than in an overwrite=false situation we
// only set folder properties on folders which were created by the current job. (To be consistent
// with the fact that when overwrite == false, we only set file properties on files created
// by the current job)
type FolderCreationTracker interface {
	CreateFolder(ctx context.Context, folder string, doCreation func() error) error
	ShouldSetProperties(folder string, overwrite OverwriteOption, prompter Prompter) bool
	StopTracking(folder string)
}

type Prompter interface {
	ShouldOverwrite(objectPath string, objectType EntityType) bool
}
