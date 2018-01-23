package handlers

import (
	"github.com/Azure/azure-storage-azcopy/common"
	"time"
	"strconv"
)

// handles the list command
// dispatches the list order to the storage engine
func HandleListCommand(commandLineInput common.ListCmdArgsAndFlags) {
	listOrder := common.ListJobPartsTransfers{}
	ApplyListCommandFlags(&commandLineInput, &listOrder)

	listOrder.JobId =  common.JobID(commandLineInput.JobId)
	partNum, err := strconv.ParseUint(commandLineInput.PartNum, 10, 32)
	if err != nil{
		panic(err)
	}
	listOrder.PartNum = uint32(partNum)
	//coordinatorScheduleFunc := generateCoordinatorScheduleFunc()

}

func ApplyListCommandFlags(commandLineInput *common.ListCmdArgsAndFlags, listOrdertofill *common.ListJobPartsTransfers)  {

	listOrdertofill. ListOnlyActiveJobs = commandLineInput.ListOnlyActiveJobs
	listOrdertofill.ListOnlyActiveTransfers = commandLineInput.ListOnlyActiveTransfers
	listOrdertofill.ListOnlyFailedTransfers = commandLineInput.ListOnlyFailedTransfers
	listOrdertofill.ListOnlyCompletedTransfers = commandLineInput.ListOnlyCompletedTransfers
}