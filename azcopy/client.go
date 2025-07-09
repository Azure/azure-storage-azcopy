package azcopy

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"log"
	"runtime"
)

type Client struct {
	CurrentJobID common.JobID // TODO (gapra): In future this should only be set when there is a current job running. On complete, this should be cleared. It can also behave as something we can check to see if a current job is running
}

type ClientOptions struct {
	CapMbps float64
}

func NewClient(opts ClientOptions) (Client, error) {
	c := Client{}
	common.InitializeFolders()
	configureGoMaxProcs()
	// Perform os specific initialization
	azcopyMaxFileAndSocketHandles, err := processOSSpecificInitialization()
	if err != nil {
		log.Fatalf("initialization failed: %v", err)
	}
	// startup of the STE happens here, so that the startup can access the values of command line parameters that are defined for "root" command
	concurrencySettings := ste.NewConcurrencySettings(azcopyMaxFileAndSocketHandles)
	err = jobsAdmin.MainSTE(concurrencySettings, opts.CapMbps)
	if err != nil {
		return c, err
	}
	return c, nil
}

// Ensure we always have more than 1 OS thread running goroutines, since there are issues with having just 1.
// (E.g. version check doesn't happen at login time, if have only one go proc. Not sure why that happens if have only one
// proc. Is presumably due to the high CPU usage we see on login if only 1 CPU, even tho can't see any busy-wait in that code)
func configureGoMaxProcs() {
	isOnlyOne := runtime.GOMAXPROCS(0) == 1
	if isOnlyOne {
		runtime.GOMAXPROCS(2)
	}
}
