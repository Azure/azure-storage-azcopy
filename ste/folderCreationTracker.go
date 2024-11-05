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
			plan:     plan,
			mu:       &sync.Mutex{},
			contents: common.NewTrie(),
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

type jpptFolderTracker struct {
	plan     IJobPartPlanHeader
	mu       *sync.Mutex
	contents *common.Trie
}

func (f *jpptFolderTracker) RegisterPropertiesTransfer(folder string, transferIndex uint32) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if folder == common.Dev_Null {
		return // Never persist to dev-null
	}

	fNode, _ := f.contents.InsertDirNode(folder)
	fNode.TransferIndex = transferIndex

	// We created it before it was enumerated-- Let's register that now.
	if fNode.UnregisteredButCreated {
		f.plan.Transfer(transferIndex).SetTransferStatus(common.ETransferStatus.FolderCreated(), false)
		fNode.UnregisteredButCreated = false

	}
}

func (f *jpptFolderTracker) CreateFolder(folder string, doCreation func() error) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if folder == common.Dev_Null {
		return nil // Never persist to dev-null
	}

	// If the folder has already been created, then we don't need to create it again
	fNode, addedToTrie := f.contents.InsertDirNode(folder)

	if !addedToTrie && (f.plan.Transfer(fNode.TransferIndex).TransferStatus() == common.ETransferStatus.FolderCreated() ||
		f.plan.Transfer(fNode.TransferIndex).TransferStatus() == common.ETransferStatus.Success()) {
		return nil
	}

	if fNode.UnregisteredButCreated {
		return nil
	}

	err := doCreation()
	if err != nil {
		return err
	}

	if !addedToTrie {
		// overwrite it's transfer status
		f.plan.Transfer(fNode.TransferIndex).SetTransferStatus(common.ETransferStatus.FolderCreated(), false)
	} else {
		// A folder hasn't been hit in traversal yet.
		// Recording it in memory is OK, because we *cannot* resume a job that hasn't finished traversal.
		// We set the value to 0 as we just want to record it in memory
		fNode.UnregisteredButCreated = true
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

		f.mu.Lock()
		defer f.mu.Unlock()

		var created bool
		if fNode, ok := f.contents.GetDirNode(folder); ok {
			created = f.plan.Transfer(fNode.TransferIndex).TransferStatus() == common.ETransferStatus.FolderCreated() ||
				f.plan.Transfer(fNode.TransferIndex).TransferStatus() == common.ETransferStatus.Success()
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
