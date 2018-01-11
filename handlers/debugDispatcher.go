package handlers

import (
	"github.com/Azure/azure-storage-azcopy/ste"
	"github.com/Azure/azure-storage-azcopy/common"
	"encoding/json"
	"fmt"
)

type coordinatorScheduleFunc func(*common.CopyJobPartOrder)

func generateCoordinatorScheduleFunc() coordinatorScheduleFunc{
	coordinatorChannel, execEngineChannels := ste.InitializedChannels()
	ste.InitializeExecutionEngine(execEngineChannels)

	return func(jobPartOrder *common.CopyJobPartOrder) {
		marshalAndPrintJobPartOrder(jobPartOrder)
		ste.ExecuteNewCopyJobPartOrder(*jobPartOrder, coordinatorChannel)
	}
}

func marshalAndPrintJobPartOrder(jobPartOrder *common.CopyJobPartOrder)  {
	order, _ := json.MarshalIndent(jobPartOrder, "", "  ")
	fmt.Println("=============================================================")
	fmt.Println("The following job part order was generated:")
	fmt.Println(string(order))
}