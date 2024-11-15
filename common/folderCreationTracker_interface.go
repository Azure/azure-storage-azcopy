package common

// folderCreationTracker is used to ensure than in an overwrite=false situation we
// only set folder properties on folders which were created by the current job. (To be consistent
// with the fact that when overwrite == false, we only set file properties on files created
// by the current job)
type FolderCreationTracker interface {
	// created is rarely used but useful for debugging
	CreateFolder(folder string, doCreation func() error, doValidate func() error) (err error, created bool)
	ShouldSetProperties(folder string, overwrite OverwriteOption, prompter Prompter) bool
	StopTracking(folder string)
}

var FolderCreationErrorAlreadyExists = folderCreationErrorAlreadyExists{}

type folderCreationErrorAlreadyExists struct{}           // Returned by doCreation, signals to the creator that the folder already existed.
func (e folderCreationErrorAlreadyExists) Error() string { return "folder already exists" }

type Prompter interface {
	ShouldOverwrite(objectPath string, objectType EntityType) bool
}
