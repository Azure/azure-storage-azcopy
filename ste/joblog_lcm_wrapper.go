package ste

import (
	"github.com/Azure/azure-pipeline-go/pipeline"

	"github.com/Azure/azure-storage-azcopy/common"
)

type jobLogLCMWrapper struct {
	jobManager IJobMgr
	common.LifecycleMgr
}

func (j jobLogLCMWrapper) Progress(builder common.OutputBuilder) {
	builderWrapper := func(format common.OutputFormat) string {
		if format != common.EOutputFormat.Text() {
			j.jobManager.Log(pipeline.LogInfo, builder(common.EOutputFormat.Text()))
			return builder(format)
		} else {
			output := builder(common.EOutputFormat.Text())
			j.jobManager.Log(pipeline.LogInfo, output)
			return output
		}
	}

	j.LifecycleMgr.Progress(builderWrapper)
}
