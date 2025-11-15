package common

// folderCreationTracker is used to ensure than in an overwrite=false situation we
// only set folder properties on folders which were created by the current job. (To be consistent
// with the fact that when overwrite == false, we only set file properties on files created
// by the current job)
type FolderCreationTracker interface {
	CreateFolder(folder string, doCreation func() error) error
	ShouldSetProperties(folder string, overwrite OverwriteOption, prompter Prompter) bool
	StopTracking(folder string)
}

type Prompter interface {
	ShouldOverwrite(objectPath string, objectType EntityType) bool
}

// FolderCreationErrorFolderAlreadyExists is a signalling error that should be
// returned by doCreation on FolderCreationTracker.CreateFolder for supported folder-creators.
// This will inform the folder creation tracker to _not_ try to create the folder.
type FolderCreationErrorFolderAlreadyExists struct{}

func (f FolderCreationErrorFolderAlreadyExists) Error() string {
	return "not a real error"
}
