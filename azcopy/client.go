package azcopy

import (
	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type Options struct {
	CapMbps          float64
	OutputFormat     common.OutputFormat
	OutputLevel      common.OutputVerbosity
	LogLevel         common.LogLevel
	TrustedSuffixes  string
	SkipVersionCheck bool
}

type Client struct {
	Options
}

func (cc Client) initialize(resumeJobID common.JobID, isBench bool) error {
	// Set AzCopy azcopy options
	cmd.CapMbps = cc.CapMbps
	cmd.OutputFormat = cc.OutputFormat
	cmd.OutputLevel = cc.OutputLevel
	cmd.LogLevel = cc.LogLevel
	cmd.TrustedSuffixes = cc.TrustedSuffixes
	cmd.SkipVersionCheck = cc.SkipVersionCheck
	return cmd.Initialize(resumeJobID, isBench)
}
