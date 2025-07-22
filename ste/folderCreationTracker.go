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

// FolderTrackerMode defines the locking strategy for folder creation
type FolderTrackerMode int

const (
	FolderTrackerModeGlobalLock FolderTrackerMode = iota // Original global lock approach
	FolderTrackerModeLockFree                            // Lock-free concurrent approach
)

func NewFolderCreationTracker(fpo common.FolderPropertyOption, plan *JobPartPlanHeader) FolderCreationTracker {
	if buildmode.IsMover && plan.FromTo.From() == common.ELocation.Local() &&
		(plan.FromTo.To() == common.ELocation.File() ||
			plan.FromTo.To() == common.ELocation.Blob() ||
			plan.FromTo.To() == common.ELocation.BlobFS()) {
		return NewFolderCreationTrackerWithMode(fpo, plan, FolderTrackerModeLockFree)
	}
	return NewFolderCreationTrackerWithMode(fpo, plan, FolderTrackerModeGlobalLock)
}

func NewFolderCreationTrackerWithMode(fpo common.FolderPropertyOption, plan *JobPartPlanHeader, mode FolderTrackerMode) FolderCreationTracker {
	switch fpo {
	case common.EFolderPropertiesOption.AllFolders(),
		common.EFolderPropertiesOption.AllFoldersExceptRoot():
		switch mode {
		case FolderTrackerModeGlobalLock:
			return &jpptFolderTracker{
				plan:                   plan,
				mu:                     &sync.Mutex{},
				contents:               make(map[string]uint32),
				unregisteredButCreated: make(map[string]struct{}),
			}
		case FolderTrackerModeLockFree:
			return &jpptFolderTrackerLockFree{
				plan:                   plan,
				contents:               &sync.Map{},
				unregisteredButCreated: &sync.Map{},
			}
		default:
			panic("unknown folder tracker mode")
		}
	case common.EFolderPropertiesOption.NoFolders():
		return &nullFolderTracker{}
	default:
		panic("unknown folderPropertiesOption")
	}
}

type nullFolderTracker struct{}

func (f *nullFolderTracker) CreateFolder(folder string, doCreation func() error) error {
	return doCreation()
}

func (f *nullFolderTracker) ShouldSetProperties(folder string, overwrite common.OverwriteOption, prompter common.Prompter) bool {
	panic("wrong type of folder tracker has been instantiated. This type does not do any tracking")
}

func (f *nullFolderTracker) StopTracking(folder string) {
	// noop
}

// =============================================================================
// Shared Utilities Module - Only meaningful repetitions
// =============================================================================

// shouldSetPropertiesCore implements the complex overwrite logic shared by both implementations
func shouldSetPropertiesCore(created bool, overwrite common.OverwriteOption, folder string, prompter common.Prompter) bool {
	switch overwrite {
	case common.EOverwriteOption.True():
		return true
	case common.EOverwriteOption.Prompt(),
		common.EOverwriteOption.IfSourceNewer(),
		common.EOverwriteOption.False():

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

// buildDebugContents creates debug string for panic messages - used by both implementations
func buildDebugContentsFromMap(contents map[string]uint32) string {
	var result strings.Builder
	for k, v := range contents {
		result.WriteString(fmt.Sprintf("K: %s V: %d\n", k, v))
	}
	return result.String()
}

func buildDebugContentsFromSyncMap(contents *sync.Map) string {
	var result strings.Builder
	contents.Range(func(key, value interface{}) bool {
		result.WriteString(fmt.Sprintf("K: %s V: %d\n", key.(string), value.(uint32)))
		return true
	})
	return result.String()
}

// =============================================================================
// Global Lock Implementation
// =============================================================================

type jpptFolderTracker struct {
	plan                   IJobPartPlanHeader
	mu                     *sync.Mutex
	contents               map[string]uint32
	unregisteredButCreated map[string]struct{}
}

func (f *jpptFolderTracker) RegisterPropertiesTransfer(folder string, transferIndex uint32) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if folder == common.Dev_Null {
		return
	}

	f.contents[folder] = transferIndex

	if _, ok := f.unregisteredButCreated[folder]; ok {
		f.plan.Transfer(transferIndex).SetTransferStatus(common.ETransferStatus.FolderCreated(), false)
		delete(f.unregisteredButCreated, folder)
	}
}

func (f *jpptFolderTracker) CreateFolder(folder string, doCreation func() error) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if folder == common.Dev_Null {
		return nil
	}

	if idx, ok := f.contents[folder]; ok &&
		f.plan.Transfer(idx).TransferStatus() == (common.ETransferStatus.FolderCreated()) {
		return nil
	}

	if _, ok := f.unregisteredButCreated[folder]; ok {
		return nil
	}

	err := doCreation()
	if err != nil {
		return err
	}

	if idx, ok := f.contents[folder]; ok {
		f.plan.Transfer(idx).SetTransferStatus(common.ETransferStatus.FolderCreated(), false)
	} else {
		f.unregisteredButCreated[folder] = struct{}{}
	}

	return nil
}

func (f *jpptFolderTracker) ShouldSetProperties(folder string, overwrite common.OverwriteOption, prompter common.Prompter) bool {
	if folder == common.Dev_Null {
		return false
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	var created bool
	if idx, ok := f.contents[folder]; ok {
		created = f.plan.Transfer(idx).TransferStatus() == common.ETransferStatus.FolderCreated()
	} else {
		panic(common.NewAzCopyLogSanitizer().SanitizeLogMessage("folder " + folder + " was not registered when properties persistence occurred"))
	}

	return shouldSetPropertiesCore(created, overwrite, folder, prompter)
}

func (f *jpptFolderTracker) StopTracking(folder string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if folder == common.Dev_Null {
		return
	}

	if _, ok := f.contents[folder]; ok {
		delete(f.contents, folder)
	} else {
		currentContents := buildDebugContentsFromMap(f.contents)
		panic(common.NewAzCopyLogSanitizer().SanitizeLogMessage("Folder " + folder + " shouldn't finish tracking until it's been recorded\nCurrent Contents:\n" + currentContents))
	}
}

// =============================================================================
// Lock-Free Implementation
// =============================================================================

type jpptFolderTrackerLockFree struct {
	plan                   IJobPartPlanHeader
	contents               *sync.Map // folder path → transfer index
	unregisteredButCreated *sync.Map // folder path → struct{}
}

func (f *jpptFolderTrackerLockFree) RegisterPropertiesTransfer(folder string, transferIndex uint32) {
	if folder == common.Dev_Null {
		return
	}

	// Lock-free store
	f.contents.Store(folder, transferIndex)

	// Check if folder was created before registration
	if _, wasCreated := f.unregisteredButCreated.LoadAndDelete(folder); wasCreated {
		f.plan.Transfer(transferIndex).SetTransferStatus(common.ETransferStatus.FolderCreated(), false)
	}
}

func (f *jpptFolderTrackerLockFree) CreateFolder(folder string, doCreation func() error) error {
	if folder == common.Dev_Null {
		return nil
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

	// Perform actual folder creation - multiple threads may execute this concurrently
	// but storage operations are idempotent, so this is safe
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

func (f *jpptFolderTrackerLockFree) ShouldSetProperties(folder string, overwrite common.OverwriteOption, prompter common.Prompter) bool {
	if folder == common.Dev_Null {
		return false
	}

	// Lock-free read
	var created bool
	if idx, exists := f.contents.Load(folder); exists {
		created = f.plan.Transfer(idx.(uint32)).TransferStatus() == common.ETransferStatus.FolderCreated()
	} else {
		panic(common.NewAzCopyLogSanitizer().SanitizeLogMessage("folder " + folder + " was not registered when properties persistence occurred"))
	}

	return shouldSetPropertiesCore(created, overwrite, folder, prompter)
}

func (f *jpptFolderTrackerLockFree) StopTracking(folder string) {
	if folder == common.Dev_Null {
		return
	}

	// Lock-free delete
	if _, exists := f.contents.LoadAndDelete(folder); !exists {
		currentContents := buildDebugContentsFromSyncMap(f.contents)
		panic(common.NewAzCopyLogSanitizer().SanitizeLogMessage("Folder " + folder + " shouldn't finish tracking until it's been recorded\nCurrent Contents:\n" + currentContents))
	}
}
