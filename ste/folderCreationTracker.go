package ste

import (
	"context"
	"fmt"
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
			contents:               &sync.Map{}, // Lock-free concurrent map
			unregisteredButCreated: &sync.Map{}, // Lock-free concurrent map
			folderLocks:            &sync.Map{}, // Per-folder locks
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

func (f *nullFolderTracker) CreateFolder(ctx context.Context, folder string, doCreation func() error) error {
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
	contents               *sync.Map // folder path → transfer index
	unregisteredButCreated *sync.Map // folder path → struct{}
	folderLocks            *sync.Map // folder path → *sync.Mutex
}

func (f *jpptFolderTracker) RegisterPropertiesTransfer(folder string, transferIndex uint32) {
	if folder == common.Dev_Null {
		return
	}

	// Lock-free store - no global synchronization!
	f.contents.Store(folder, transferIndex)

	// Check if folder was created before registration (lock-free)
	if _, wasCreated := f.unregisteredButCreated.LoadAndDelete(folder); wasCreated {
		f.plan.Transfer(transferIndex).SetTransferStatus(common.ETransferStatus.FolderCreated(), false)
	}
}

func (f *jpptFolderTracker) CreateFolder(ctx context.Context, folder string, doCreation func() error) error {
	if folder == common.Dev_Null {
		return nil
	}

	// Check cancellation upfront
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Get or create per-folder mutex (lock-free operation)
	folderLockInterface, _ := f.folderLocks.LoadOrStore(folder, &sync.Mutex{})
	folderLock := folderLockInterface.(*sync.Mutex)

	// Try to acquire lock with timeout/cancellation
	acquired := make(chan struct{})
	go func() {
		folderLock.Lock()
		close(acquired)
	}()

	select {
	case <-acquired:
		defer folderLock.Unlock()
	case <-ctx.Done():
		return ctx.Err()
	}

	// Check cancellation after acquiring lock
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Fast path check (lock-free read)
	if idx, exists := f.contents.Load(folder); exists {
		if f.plan.Transfer(idx.(uint32)).TransferStatus() == common.ETransferStatus.FolderCreated() {
			return nil
		}
	}

	// Check unregistered cache (lock-free read)
	if _, wasCreated := f.unregisteredButCreated.Load(folder); wasCreated {
		return nil
	}

	// Final cancellation check before network operation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Perform actual folder creation
	err := doCreation()
	if err != nil {
		return err
	}

	// Update status (lock-free operations)
	if idx, exists := f.contents.Load(folder); exists {
		f.plan.Transfer(idx.(uint32)).SetTransferStatus(common.ETransferStatus.FolderCreated(), false)
	} else {
		f.unregisteredButCreated.Store(folder, struct{}{})
	}

	return nil
}

func (f *jpptFolderTracker) ShouldSetProperties(folder string, overwrite common.OverwriteOption, prompter common.Prompter) bool {
	if folder == common.Dev_Null {
		return false
	}

	switch overwrite {
	case common.EOverwriteOption.True():
		return true
	case common.EOverwriteOption.Prompt(),
		common.EOverwriteOption.IfSourceNewer(),
		common.EOverwriteOption.False():

		// Lock-free read - no global synchronization!
		var created bool
		if idx, exists := f.contents.Load(folder); exists {
			created = f.plan.Transfer(idx.(uint32)).TransferStatus() == common.ETransferStatus.FolderCreated()
		} else {
			panic(common.NewAzCopyLogSanitizer().SanitizeLogMessage("folder " + folder + " was not registered when properties persistence occurred"))
		}

		if overwrite == common.EOverwriteOption.Prompt() && !created {
			cleanedFolderPath := folder
			if !strings.HasPrefix(folder, common.EXTENDED_PATH_PREFIX) {
				if parsedURL, _ := url.Parse(folder); parsedURL.Scheme != "" && parsedURL.Host != "" {
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
	if folder == common.Dev_Null {
		return
	}

	// Lock-free delete
	if _, exists := f.contents.LoadAndDelete(folder); !exists {
		// Debugging: collect current contents
		var currentContents strings.Builder
		f.contents.Range(func(key, value interface{}) bool {
			currentContents.WriteString(fmt.Sprintf("K: %s V: %d\n", key.(string), value.(uint32)))
			return true
		})

		panic(common.NewAzCopyLogSanitizer().SanitizeLogMessage("Folder " + folder + " shouldn't finish tracking until it's been recorded\nCurrent Contents:\n" + currentContents.String()))
	}

	// Clean up per-folder lock to prevent memory leaks
	f.folderLocks.Delete(folder)
}
