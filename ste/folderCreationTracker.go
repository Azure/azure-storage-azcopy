package ste

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type FolderCreationTracker common.FolderCreationTracker

type JPPTCompatibleFolderCreationTracker interface {
	FolderCreationTracker
	RegisterPropertiesTransfer(folder string, partNum PartNumber, transferIndex uint32)
}

type TransferFetcher = func(index JpptFolderIndex) *JobPartPlanTransfer

func NewTransferFetcher(jobMgr IJobMgr) TransferFetcher {
	return func(index JpptFolderIndex) *JobPartPlanTransfer {
		mgr, ok := jobMgr.JobPartMgr(index.PartNum)
		if !ok {
			panic(fmt.Errorf("sanity check: failed to fetch job part manager %d", index.PartNum))
		}

		return mgr.Plan().Transfer(index.TransferIndex)
	}
}

// NewFolderCreationTracker creates a folder creation tracker taking in a TransferFetcher (typically created by NewTransferFetcher)
// A TransferFetcher is used in place of an IJobMgr to make testing easier to implement.
func NewFolderCreationTracker(fpo common.FolderPropertyOption, fetcher TransferFetcher, fromTo common.FromTo) FolderCreationTracker {
	switch {
	// create a folder tracker when we're persisting properties
	case fpo == common.EFolderPropertiesOption.AllFolders(),
		fpo == common.EFolderPropertiesOption.AllFoldersExceptRoot(),
		// create a folder tracker for destinations where we don't want to spam the service with create requests
		// on folders that already exist
		fromTo.To().IsFile(),
		fromTo.To() == common.ELocation.BlobFS():
		return &jpptFolderTracker{ // This prevents a dependency cycle. Reviewers: Are we OK with this? Can you think of a better way to do it?
			fetchTransfer: fetcher,
			mu:            &sync.Mutex{},
			contents:      make(map[string]*JpptFolderTrackerState),
		}
	case fpo == common.EFolderPropertiesOption.NoFolders():
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
	// fetchTransfer is used instead of a IJobMgr reference to support testing
	fetchTransfer func(index JpptFolderIndex) *JobPartPlanTransfer
	mu            *sync.Mutex

	contents map[string]*JpptFolderTrackerState
}

func (f *jpptFolderTracker) RegisterPropertiesTransfer(folder string, partNum PartNumber, transferIndex uint32) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if folder == common.Dev_Null {
		return // Never persist to dev-null
	}

	state, present := f.contents[folder]

	if !present {
		state = &JpptFolderTrackerState{}
	}

	state.Index = &JpptFolderIndex{
		PartNum:       partNum,
		TransferIndex: transferIndex,
	}

	// if our transfer already has a created status, we should adopt that.
	if ts := f.fetchTransfer(*state.Index).TransferStatus(); ts == ts.FolderCreated() {
		state.Status = EJpptFolderTrackerStatus.FolderCreated()
	} else {
		// otherwise, we map onto it whatever we have. This puts the statuses in alignment.
		switch state.Status {
		case EJpptFolderTrackerStatus.FolderExisted():
			f.fetchTransfer(*state.Index).SetTransferStatus(common.ETransferStatus.FolderExisted(), false)
		case EJpptFolderTrackerStatus.FolderCreated():
			f.fetchTransfer(*state.Index).SetTransferStatus(common.ETransferStatus.FolderCreated(), false)
		}
	}

	f.debugCheckState(*state)

	if !present {
		f.contents[folder] = state
	}
}

func (f *jpptFolderTracker) CreateFolder(folder string, doCreation func() error) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if folder == common.Dev_Null {
		return nil // Never persist to dev-null
	}

	state, ok := f.contents[folder]

	if ok {
		if state.Index != nil {
			ts := f.fetchTransfer(*state.Index).TransferStatus()

			if ts == common.ETransferStatus.FolderCreated() ||
				ts == common.ETransferStatus.FolderExisted() {
				return nil // do not re-create an existing folder
			}
		} else {
			if state.Status != EJpptFolderTrackerStatus.Unseen() {
				return nil
			}
		}
	}

	err := doCreation()
	if err != nil && !errors.Is(err, common.FolderCreationErrorAlreadyExists{}) {
		return err
	}

	if state == nil {
		state = &JpptFolderTrackerState{}
	}

	if err == nil { // first, update our internal status, then,
		state.Status = EJpptFolderTrackerStatus.FolderCreated()
	} else if errors.Is(err, common.FolderCreationErrorAlreadyExists{}) {
		state.Status = EJpptFolderTrackerStatus.FolderExisted()
	}

	if state.Index != nil {
		// commit the state if needbe.
		switch state.Status {
		case EJpptFolderTrackerStatus.FolderCreated():
			f.fetchTransfer(*state.Index).SetTransferStatus(common.ETransferStatus.FolderCreated(), false)
		case EJpptFolderTrackerStatus.FolderExisted():
			f.fetchTransfer(*state.Index).SetTransferStatus(common.ETransferStatus.FolderExisted(), false)
		}
	}

	f.debugCheckState(*state)

	return nil
}

// debugCheckState validates that, if there is a plan file entry, that the plan file state matches the tracker's state.
// this should be called any time state gets used, to ensure it doesn't go out of sync.
func (f *jpptFolderTracker) debugCheckState(state JpptFolderTrackerState) {
	if state.Index == nil {
		return // nothing to do, current status is in table.
	}

	ts := f.fetchTransfer(*state.Index).TransferStatus()
	passed := true

	switch state.Status {
	case EJpptFolderTrackerStatus.FolderCreated():
		passed = ts == common.ETransferStatus.FolderCreated()
	case EJpptFolderTrackerStatus.FolderExisted():
		passed = ts == common.ETransferStatus.FolderExisted()
	}

	if !passed {
		panic(fmt.Sprintf("internal folder state didn't match plan state: (internal: %v) (plan: %v)", state.Status, ts))
	}
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

		if state, ok := f.contents[folder]; ok {
			if state.Index == nil {
				// This should not happen, ever.
				// Folder property jobs register with the tracker before they start getting processed.
				panic(common.NewAzCopyLogSanitizer().SanitizeLogMessage("folder " + folder + " was not registered when properties persistence occurred"))
			}

			// status should, at this point, be aligned with the job plan status.
			created = state.Status == EJpptFolderTrackerStatus.FolderCreated()
			f.debugCheckState(*state)
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
			currentContents += fmt.Sprintf("K: %s V: %v\n", k, v)
		}

		// double should never be hit, but *just in case*.
		panic(common.NewAzCopyLogSanitizer().SanitizeLogMessage("Folder " + folder + " shouldn't finish tracking until it's been recorded\nCurrent Contents:\n" + currentContents))
	}
}

// ===== Supporting Types =====

type JpptFolderTrackerStatus uint
type eJpptFolderTrackerStatus struct{}

var EJpptFolderTrackerStatus eJpptFolderTrackerStatus

// Unseen - brand new folder
func (eJpptFolderTrackerStatus) Unseen() JpptFolderTrackerStatus {
	return 0
}

// FolderExisted - the folder was here before we got to it
func (eJpptFolderTrackerStatus) FolderExisted() JpptFolderTrackerStatus {
	return 1
}

// FolderCreated - the folder was created by us.
func (eJpptFolderTrackerStatus) FolderCreated() JpptFolderTrackerStatus {
	return 2
}

type JpptFolderIndex struct {
	PartNum       PartNumber
	TransferIndex uint32
}

type JpptFolderTrackerState struct {
	Index  *JpptFolderIndex
	Status JpptFolderTrackerStatus
}
