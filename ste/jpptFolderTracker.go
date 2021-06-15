package ste

import (
	"net/url"
	"strings"
	"sync"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

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
			panic("folder was not registered when properties persistence occurred")
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
		panic("Folder shouldn't finish tracking until it's been recorded")
	}
}