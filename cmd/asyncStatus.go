// Copyright © 2017 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cmd

import (
	"context"
	"errors"
	"fmt"
	pipeline2 "github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/spf13/cobra"
)

type rawAsyncStatusCmdArgs struct {
	// obtained from argument
	jobID          string
	DestinationSAS string
}

func (rsa rawAsyncStatusCmdArgs) process() error {
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	credentialInfo := common.CredentialInfo{}

	// parsing the given JobId to validate its format correctness
	jobID, err := common.ParseJobID(rsa.jobID)
	if err != nil {
		// If parsing gives an error, hence it is not a valid JobId format
		return fmt.Errorf("error parsing the jobId %s. Failed with error %s", rsa.jobID, err.Error())
	}

	// isSource is rather misnomer for canBePublic. We can list public containers, and hence isSource=true
	computeAuth := func(src common.ResourceString) {
		if credentialInfo.CredentialType, err = getCredentialType(ctx, rawFromToInfo{
			fromTo:    common.EFromTo.BlobTrash(),
			source:    src.Value,
			sourceSAS: rsa.DestinationSAS,
		}, common.CpkOptions{}); err != nil {
			panic(err)
		} else if credentialInfo.CredentialType == common.ECredentialType.OAuthToken() {
			uotm := GetUserOAuthTokenManagerInstance()
			// Get token from env var or cache.
			if tokenInfo, err := uotm.GetTokenInfo(ctx); err != nil {
				panic(err)
			} else {
				credentialInfo.OAuthTokenInfo = *tokenInfo
			}
		}
	}

	if !jobsAdmin.JobsAdmin.ResurrectJob(jobID, "", rsa.DestinationSAS) {
		return fmt.Errorf("couldn't resurrect job %s, make sure its plan file exists", jobID)
	}

	jobMgr, _ := jobsAdmin.JobsAdmin.JobMgr(jobID)
	_, found := jobMgr.JobPartMgr(0)
	if !found {
		return fmt.Errorf("JobID=%v, Part#=0 not found", jobID)
	}

	jobMgr.IterateJobParts(true, func(partNum common.PartNumber, jpm ste.IJobPartMgr) {
		jpp := jpm.Plan()
		// Iterate through this job part's transfers
		for t := uint32(0); t < jpp.NumTransfers; t++ {
			_, dst, _ := jpp.TransferSrcDstStrings(t)

			if jpp.Transfer(t).TransferStatus() != common.ETransferStatus.Success() {
				glcm.Info(dst + " skipping (failed when called async copy)")
				continue
			}

			source := common.ResourceString{Value: dst, SAS: rsa.DestinationSAS}
			computeAuth(source)

			_, _, _, _, _, _, _, _, _, _, _, _, planFileCopyID := jpp.TransferSrcPropertiesAndMetadata(t)

			traverser, err := InitResourceTraverser(source, common.ELocation.Blob(), &ctx, &credentialInfo, nil, nil, true, false, false, common.EPermanentDeleteOption.None(), func(common.EntityType) {}, nil, false, pipeline2.LogNone, common.CpkOptions{}, true)
			if err != nil {
				panic(fmt.Errorf("failed to initialize traverser: %s", err.Error()))
			}

			processor := func(object StoredObject) error {
				path := object.name
				if object.entityType == common.EEntityType.Folder() {
					path += "/" // TODO: reviewer: same questions as for jobs status: OK to hard code direction of slash? OK to use trailing slash to distinguish dirs from files?
				}

				if object.copyID != planFileCopyID {
					return fmt.Errorf("the destination CopyID doesn't match CopyID of transfer, perhaps because a new copy operation was run")
				}

				properties := ": " + "copyStatus: " + string(object.copyStatus)
				objectSummary := path + properties
				glcm.Info(objectSummary)

				return nil
			}

			err = traverser.Traverse(nil, processor, nil)
			if err != nil {
				panic(fmt.Errorf("failed to traverse container: %s", err.Error()))
			}
		}
	})

	return nil
}

func init() {
	raw := rawAsyncStatusCmdArgs{}
	// listContainerCmd represents the list container command
	// listContainer list the blobs inside the container or virtual directory inside the container
	statusCmd := &cobra.Command{
		Use:     "status [Job ID]",
		Aliases: []string{},
		Short:   listCmdShortDescription, // TODO tiverma change
		Long:    listCmdLongDescription,
		Example: listCmdExample,
		Args: func(cmd *cobra.Command, args []string) error {
			// the listContainer command requires necessarily to have an argument

			// If no argument is passed then it is not valid
			// lsc expects the container path / virtual directory
			if len(args) != 1 {
				return errors.New("this command only requires JobID")
			}
			raw.jobID = args[0]
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			err := raw.process()
			if err != nil {
				glcm.Error(fmt.Sprintf("failed to perform resume command due to error: %s", err.Error()))
			}

			err = cooked.HandleListContainerCommand()
			if err == nil {
				glcm.Exit(nil, common.EExitCode.Success())
			} else {
				glcm.Error(err.Error())
			}
		},
	}
	statusCmd.PersistentFlags().StringVar(&raw.DestinationSAS, "ds", "", "Exclude files where the name matches the pattern list. For example: *.jpg;*.pdf;exactName")

	asyncCmd.AddCommand(statusCmd)
}
