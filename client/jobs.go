package client

import "github.com/Azure/azure-storage-azcopy/v10/common"

type JobsCleanOptions struct {
	WithStatus common.JobStatus
}

func (cc Client) JobsClean(options JobsCleanOptions) error {
	return nil
}

type JobsListOptions struct {
	WithStatus common.JobStatus
}

func (cc Client) JobsList(options JobsListOptions) error {
	return nil
}

type JobsRemoveOptions struct {
}

func (cc Client) JobsRemove(JobID common.JobID, options JobsRemoveOptions) error {
	return nil
}

type JobsResumeOptions struct {
}

func (cc Client) JobsResume(JobID common.JobID, options JobsResumeOptions) error {
	return nil
}

type JobsShowOptions struct {
	WithStatus common.TransferStatus
}

func (cc Client) JobsShow(JobID common.JobID, options JobsShowOptions) error {
	return nil
}
