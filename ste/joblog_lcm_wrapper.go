package ste

import "github.com/Azure/azure-storage-azcopy/v10/common"

type JobLogLCMWrapper struct {
	JobManager IJobMgr
	common.LifecycleMgr
}

func (j JobLogLCMWrapper) Progress(builder common.OutputBuilder) {
	builderWrapper := func(format common.OutputFormat) string {
		text := builder(common.EOutputFormat.Text())
		j.JobManager.Log(common.LogInfo, text)
		if format == common.EOutputFormat.Text() {
			return text
		}
		return builder(format)
	}
	j.LifecycleMgr.Progress(builderWrapper)
}
