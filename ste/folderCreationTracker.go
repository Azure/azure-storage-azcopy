package ste

import (
	"net/url"
	"strings"
	"sync"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type FolderCreationTracker common.FolderCreationTracker

type JPPTCompatibleFolderCreationTracker interface {
	FolderCreationTracker
	RegisterPropertiesTransfer(folder string, transferIndex uint32)
}

func NewFolderCreationTracker(fpo common.FolderPropertyOption, plan *JobPartPlanHeader) FolderCreationTracker {
	switch fpo {
	case common.EFolderPropertiesOption.AllFolders(),
		common.EFolderPropertiesOption.AllFoldersExceptRoot():
		return &jpptFolderTracker{ // This prevents a dependency cycle. Reviewers: Are we OK with this? Can you think of a better way to do it?
			plan:                   plan,
			mu:                     &sync.Mutex{},
			contents:               make(map[string]uint32),
			unregisteredButCreated: make(map[string]struct{}),
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

func (f *nullFolderTracker) RecordCreation(folder string) {
	// no-op (the null tracker doesn't track anything)
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
	plan *JobPartPlanHeader
	mu *sync.Mutex
	contents map[string]uint32
	unregisteredButCreated map[string]struct{}
}

func (f *jpptFolderTracker) RegisterPropertiesTransfer(folder string, transferIndex uint32) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.contents[folder] = transferIndex

	// We created it before it was enumerated-- Let's register that now.
	if _, ok := f.unregisteredButCreated[folder]; ok {
		f.plan.Transfer(transferIndex).SetTransferStatus(common.ETransferStatus.FolderCreated(), false)

		delete(f.unregisteredButCreated, folder)
	}
}

func (f *jpptFolderTracker) RecordCreation(folder string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if idx, ok := f.contents[folder]; ok {
		// overwrite it's transfer status
		f.plan.Transfer(idx).SetTransferStatus(common.ETransferStatus.FolderCreated(), false)
	} else {
		// A folder hasn't been hit in traversal yet.
		// Recording it in memory is OK, because we *cannot* resume a job that hasn't finished traversal.
		f.unregisteredButCreated[folder] = struct{}{}
	}
}

func (f *jpptFolderTracker) ShouldSetProperties(folder string, overwrite common.OverwriteOption, prompter common.Prompter) bool {
	switch overwrite {
	case common.EOverwriteOption.True():
		return true
	case common.EOverwriteOption.Prompt(),
		common.EOverwriteOption.IfSourceNewer(),
		common.EOverwriteOption.False():

		f.mu.Lock()
		defer f.mu.Unlock()

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
	// no-op, because tracking is now handled by jppt, anyway.
	if _, ok := f.contents[folder]; ok {
		delete(f.contents, folder)
	} else {
		// double should never be hit, but *just in case*.
		panic(common.NewAzCopyLogSanitizer().SanitizeLogMessage("Folder " + folder + " shouldn't finish tracking until it's been recorded"))
	}
}