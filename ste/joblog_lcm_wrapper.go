package ste

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type JobLogLCMWrapper struct {
	JobManager IJobMgr
	common.LifecycleMgr
}

func (j JobLogLCMWrapper) Progress(builder common.OutputBuilder) {
	builderWrapper := func(format common.OutputFormat) string {
		if format != common.EOutputFormat.Text() {
			j.JobManager.Log(common.LogInfo, builder(common.EOutputFormat.Text()))
			return builder(format)
		} else {
			output := builder(common.EOutputFormat.Text())
			j.JobManager.Log(common.LogInfo, output)
			return output
		}
	}

	j.LifecycleMgr.Progress(builderWrapper)
}
