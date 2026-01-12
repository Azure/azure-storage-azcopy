package ste

import (
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/buildmode"
)

type FolderCreationTracker common.FolderCreationTracker

type JPPTCompatibleFolderCreationTracker interface {
	FolderCreationTracker
	RegisterPropertiesTransfer(folder string, transferIndex uint32)
}

func NewFolderCreationTracker(fpo common.FolderPropertyOption, plan *JobPartPlanHeader) FolderCreationTracker {
	skipFolderCreationLock := buildmode.IsMover && (plan.FromTo.From() == common.ELocation.Local() || plan.FromTo.From() == common.ELocation.BlobFS()) && // Added BlobFS source
		(plan.FromTo.To() == common.ELocation.File() ||
			plan.FromTo.To() == common.ELocation.Blob() ||
			plan.FromTo.To() == common.ELocation.BlobFS())
	return NewFolderCreationTrackerInt(fpo, plan, !skipFolderCreationLock)
}

func NewFolderCreationTrackerInt(fpo common.FolderPropertyOption, plan *JobPartPlanHeader, lockFolderCreation bool) FolderCreationTracker {
	switch fpo {
	case common.EFolderPropertiesOption.AllFolders(),
		common.EFolderPropertiesOption.AllFoldersExceptRoot():
		return &jpptFolderTracker{ // This prevents a dependency cycle. Reviewers: Are we OK with this? Can you think of a better way to do it?
			plan:                   plan,
			mu:                     &sync.RWMutex{},
			contents:               make(map[string]uint32),
			unregisteredButCreated: make(map[string]struct{}),
			lockFolderCreation:     lockFolderCreation,
		}
	case common.EFolderPropertiesOption.NoFolders():
		// can't use simpleFolderTracker here, because when no folders are processed,
		// then StopTracking will never be called, so we'll just use more and more memory for the map
		return &nullFolderTracker{}
	default:
		panic("unknown folderPropertiesOption")
	}
}

type nullFolderTracker struct{}

func (f *nullFolderTracker) CreateFolder(folder string, doCreation func() error) error {
	// no-op (the null tracker doesn't track anything)
	return doCreation()
}

func (f *nullFolderTracker) ShouldSetProperties(folder string, overwrite common.OverwriteOption, prompter common.Prompter) bool {
	// There's no way this should ever be called, because we only create the nullTracker if we are
	// NOT transferring folder info.
	panic("wrong type of folder tracker has been instantiated. This type does not do any tracking")
}

func (f *nullFolderTracker) StopTracking(folder string) {
	// noop (because we don't track anything)
}

type jpptFolderTracker struct {
	plan                   IJobPartPlanHeader
	mu                     *sync.RWMutex
	contents               map[string]uint32
	unregisteredButCreated map[string]struct{}
	lockFolderCreation     bool
}

// Public interface - safe for external callers
func (f *jpptFolderTracker) IsFolderAlreadyCreated(folder string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.isFolderAlreadyCreatedUnSafe(folder)
}

func (f *jpptFolderTracker) isFolderAlreadyCreatedUnSafe(folder string) bool {

	if idx, ok := f.contents[folder]; ok &&
		f.plan.Transfer(idx).TransferStatus() == (common.ETransferStatus.FolderCreated()) {
		return true
	}

	if _, ok := f.unregisteredButCreated[folder]; ok {
		return true
	}

	return false
}

func (f *jpptFolderTracker) RegisterPropertiesTransfer(folder string, transferIndex uint32) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if folder == common.Dev_Null {
		return // Never persist to dev-null
	}

	f.contents[folder] = transferIndex

	// We created it before it was enumerated-- Let's register that now.
	if _, ok := f.unregisteredButCreated[folder]; ok {
		f.plan.Transfer(transferIndex).SetTransferStatus(common.ETransferStatus.FolderCreated(), false)

		delete(f.unregisteredButCreated, folder)
	}
}

func (f *jpptFolderTracker) CreateFolder(folder string, doCreation func() error) error {

	if folder == common.Dev_Null {
		return nil // Never persist to dev-null
	}

	if f.lockFolderCreation {
		f.mu.Lock()
		defer f.mu.Unlock()

		// If the folder was created while we were waiting for the lock, we need to account for that.
		if f.isFolderAlreadyCreatedUnSafe(folder) {
			return nil
		}
	}

	err := doCreation()
	if err != nil {
		return err
	}

	if !f.lockFolderCreation {
		f.mu.Lock()
		defer f.mu.Unlock()

		// If the folder was created while we were waiting for the lock, we need to account for that.
		if f.isFolderAlreadyCreatedUnSafe(folder) {
			return nil
		}
	}

	if idx, ok := f.contents[folder]; ok {
		// overwrite it's transfer status
		f.plan.Transfer(idx).SetTransferStatus(common.ETransferStatus.FolderCreated(), false)
	} else {
		// A folder hasn't been hit in traversal yet.
		// Recording it in memory is OK, because we *cannot* resume a job that hasn't finished traversal.
		f.unregisteredButCreated[folder] = struct{}{}
	}

	return nil
}

func (f *jpptFolderTracker) ShouldSetProperties(folder string, overwrite common.OverwriteOption, prompter common.Prompter) bool {
	if folder == common.Dev_Null {
		return false // Never persist to dev-null
	}

	switch overwrite {
	case common.EOverwriteOption.True():
		return true
	case common.EOverwriteOption.Prompt(),
		common.EOverwriteOption.IfSourceNewer(),
		common.EOverwriteOption.False():

		f.mu.RLock()
		defer f.mu.RUnlock()

		var created bool
		if idx, ok := f.contents[folder]; ok {
			created = f.plan.Transfer(idx).TransferStatus() == common.ETransferStatus.FolderCreated()
		} else {
			// This should not happen, ever.
			// Folder property jobs register with the tracker before they start getting processed.
			panic(common.NewAzCopyLogSanitizer().SanitizeLogMessage("folder " + folder + " was not registered when properties persistence occurred"))
		}

		// prompt only if we didn't create this folder
		if overwrite == common.EOverwriteOption.Prompt() && !created {
			cleanedFolderPath := folder

			// if it's a local Windows path, skip since it doesn't have SAS and won't parse correctly as an URL
			if !strings.HasPrefix(folder, common.EXTENDED_PATH_PREFIX) {
				// get rid of SAS before prompting
				parsedURL, _ := url.Parse(folder)

				// really make sure that it's not a local path
				if parsedURL.Scheme != "" && parsedURL.Host != "" {
					parsedURL.RawQuery = ""
					cleanedFolderPath = parsedURL.String()
				}
			}
			return prompter.ShouldOverwrite(cleanedFolderPath, common.EEntityType.Folder())
		}

		return created
	default:
		panic("unknown overwrite option")
	}
}

func (f *jpptFolderTracker) StopTracking(folder string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if folder == common.Dev_Null {
		return // Not possible to track this
	}

	// no-op, because tracking is now handled by jppt, anyway.
	if _, ok := f.contents[folder]; ok {
		delete(f.contents, folder)
	} else {
		currentContents := ""

		for k, v := range f.contents {
			currentContents += fmt.Sprintf("K: %s V: %d\n", k, v)
		}

		// double should never be hit, but *just in case*.
		panic(common.NewAzCopyLogSanitizer().SanitizeLogMessage("Folder " + folder + " shouldn't finish tracking until it's been recorded\nCurrent Contents:\n" + currentContents))
	}
}
