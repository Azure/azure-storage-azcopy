package azcopy

import (
	"fmt"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
)

type DeleteCounter interface {
	IncrementDeletionCount()
	GetDeletionCount() uint32
}

type interactiveDeleter struct {
	// the actual function that does the deletion
	deleter traverser.ObjectProcessor

	shouldPromptUser        bool
	shouldDelete            bool
	objectTypeToDisplay     string
	objectLocationToDisplay string
	
	counter DeleteCounter

	// TODO: dryrun can probably be its own deleter?
}

func newInteractiveDeleter(deleter traverser.ObjectProcessor, deleteDestination common.DeleteDestination,
	objectTypeToDisplay string, objectLocationToDisplay common.ResourceString, counter DeleteCounter) *interactiveDeleter {

	return &interactiveDeleter{
		deleter:                 deleter,
		objectTypeToDisplay:     objectTypeToDisplay,
		objectLocationToDisplay: objectLocationToDisplay.Value,
		counter:                 counter,
		shouldPromptUser:        deleteDestination == common.EDeleteDestination.Prompt(),
		shouldDelete:            deleteDestination == common.EDeleteDestination.True(), // if shouldPromptUser is true, this will start as false, but we will determine its value later
	}
}

func (d *interactiveDeleter) removeImmediately(object traverser.StoredObject) error {
	if d.shouldPromptUser {
		d.shouldDelete, d.shouldPromptUser = d.promptForConfirmation(object) // note down the user's decision
	}
	if d.shouldDelete {
		err := d.deleter(object)
		if err != nil {
			msg := fmt.Sprintf("error %s deleting the object %s", err.Error(), object.RelativePath)
			common.GetLifecycleMgr().Info(msg + "; check the scanning log file for more details")
			if common.AzcopyScanningLogger != nil {
				common.AzcopyScanningLogger.Log(common.LogError, msg+": "+err.Error())
			}
		}
		if d.counter != nil {
			d.counter.IncrementDeletionCount()
		}
	}
	return nil
}

func (d *interactiveDeleter) promptForConfirmation(object traverser.StoredObject) (shouldDelete bool, keepPrompting bool) {
	answer := common.GetLifecycleMgr().Prompt(fmt.Sprintf("The %s '%s' does not exist at the source. "+
		"Do you wish to delete it from the destination(%s)?",
		d.objectTypeToDisplay, object.RelativePath, d.objectLocationToDisplay),
		common.PromptDetails{
			PromptType:   common.EPromptType.DeleteDestination(),
			PromptTarget: object.RelativePath,
			ResponseOptions: []common.ResponseOption{
				common.EResponseOption.Yes(),
				common.EResponseOption.No(),
				common.EResponseOption.YesForAll(),
				common.EResponseOption.NoForAll()},
		},
	)

	switch answer {
	case common.EResponseOption.Yes():
		// print nothing, since the deleter is expected to log the message when the delete happens
		return true, true
	case common.EResponseOption.YesForAll():
		common.GetLifecycleMgr().Info(fmt.Sprintf("Confirmed. All the extra %ss will be deleted.", d.objectTypeToDisplay))
		return true, false
	case common.EResponseOption.No():
		common.GetLifecycleMgr().Info(fmt.Sprintf("Keeping extra %s: %s", d.objectTypeToDisplay, object.RelativePath))
		return false, true
	case common.EResponseOption.NoForAll():
		common.GetLifecycleMgr().Info("No deletions will happen from now onwards.")
		return false, false
	default:
		common.GetLifecycleMgr().Info(fmt.Sprintf("Unrecognizable answer, keeping extra %s: %s.", d.objectTypeToDisplay, object.RelativePath))
		return false, true
	}
}
