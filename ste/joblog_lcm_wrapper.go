package ste

import (
	"github.com/Azure/azure-pipeline-go/pipeline"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type JobLogLCMWrapper struct {
	JobManager IJobMgr
	common.LifecycleMgr
}

func (j JobLogLCMWrapper) Progress(builder common.OutputBuilder) {
	builderWrapper := func(format common.OutputFormat) string {
		if format != common.EOutputFormat.Text() {
			j.JobManager.Log(pipeline.LogInfo, builder(common.EOutputFormat.Text()))
			return builder(format)
		} else {
			output := builder(common.EOutputFormat.Text())
			j.JobManager.Log(pipeline.LogInfo, output)
			return output
		}
	}

	j.LifecycleMgr.Progress(builderWrapper)
}
