package azcopy

import (
	"fmt"
	"sync"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

var registerStatsMonitorCloseOnce sync.Once

// StartSystemStatsMonitor starts the system stats monitor (see common.StartSystemStatsMonitor) and returns its stop function.
// It also arranges for the monitor to stop when the installed lifecycle manager runs its close functions,
// so the CLI needs no explicit stop. Hosts that never run those functions must call the returned stop themselves.
func StartSystemStatsMonitor(jobID common.JobID) (stop func(), err error) {
	stop, err = common.StartSystemStatsMonitor(jobID)
	if err != nil {
		common.GetLifecycleMgr().Warn(fmt.Sprintf("Could not start the system stats monitor: %v", err))
		return stop, err
	}
	// One registration per process, so per-job restarts don't grow the manager's close chain.
	registerStatsMonitorCloseOnce.Do(func() {
		common.GetLifecycleMgr().RegisterCloseFunc(common.StopSystemStatsMonitor)
	})
	return stop, nil
}
