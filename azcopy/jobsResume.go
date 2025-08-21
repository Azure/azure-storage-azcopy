// Copyright © 2025 Microsoft <wastore@microsoft.com>
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

package azcopy

import (
	"context"
	"errors"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
)

type ResumeJobOptions struct {
	SourceSAS      string
	DestinationSAS string
}

// ResumeJob resumes a job with the specified JobID.
func (c *Client) ResumeJob(jobID common.JobID, opts ResumeJobOptions) (err error) {
	if jobID.IsEmpty() {
		return errors.New("resume job requires the JobID")
	}

	// TODO : resume init, set glcm

	// if no logging, set this empty so that we don't display the log location
	if c.GetLogLevel() == common.LogNone {
		common.LogPathFolder = ""
	}

	// Get fromTo info, so we can decide what's the proper credential type to use.
	jobDetails := jobsAdmin.GetJobDetails(common.GetJobDetailsRequest{JobID: jobID})
	if jobDetails.ErrorMsg != "" {
		return errors.New(jobDetails.ErrorMsg)
	}
	if jobDetails.FromTo.From() == common.ELocation.Benchmark() ||
		jobDetails.FromTo.To() == common.ELocation.Benchmark() {
		// Doesn't make sense to resume a benchmark job.
		// It's not tested, and wouldn't report progress correctly and wouldn't clean up after itself properly
		return errors.New("resuming benchmark jobs is not supported")
	}

	sourceSAS := normalizeSAS(opts.SourceSAS)
	destinationSAS := normalizeSAS(opts.DestinationSAS)

	srcResourceString, err := SplitResourceString(jobDetails.Source, jobDetails.FromTo.From())
	if err != nil {
		return fmt.Errorf("error parsing source resource string: %w", err)
	}
	srcResourceString.SAS = sourceSAS
	dstResourceString, err := SplitResourceString(jobDetails.Destination, jobDetails.FromTo.To())
	if err != nil {
		return fmt.Errorf("error parsing destination resource string: %w", err)
	}
	dstResourceString.SAS = destinationSAS

	// TODO: Replace context with root context
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	srcServiceClient, dstServiceClient, err := getSourceAndDestinationServiceClients(
		ctx,
		srcResourceString,
		dstResourceString,
		jobDetails,
		c.GetUserOAuthTokenManagerInstance(),
	)
	if err != nil {
		return fmt.Errorf("cannot resume job with JobId %s, could not create service clients %v", jobID, err.Error())
	}

	// Send resume job request.
	resumeJobResponse := jobsAdmin.ResumeJobOrder(common.ResumeJobRequest{
		JobID:            jobID,
		SourceSAS:        sourceSAS,
		DestinationSAS:   destinationSAS,
		SrcServiceClient: srcServiceClient,
		DstServiceClient: dstServiceClient,
	})

	if !resumeJobResponse.CancelledPauseResumed {
		return errors.New(resumeJobResponse.ErrorMsg)
	}

	return nil
}

// normalizeSAS ensures the SAS token starts with "?" if non-empty.
func normalizeSAS(sas string) string {
	if sas != "" && sas[0] != '?' {
		return "?" + sas
	}
	return sas
}

func getSourceAndDestinationServiceClients(
	ctx context.Context,
	source common.ResourceString,
	destination common.ResourceString,
	jobDetails common.GetJobDetailsResponse,
	uotm *common.UserOAuthTokenManager,
) (*common.ServiceClient, *common.ServiceClient, error) {
	fromTo := jobDetails.FromTo
	srcCredType, isSrcPublic, err := GetCredentialTypeForLocation(ctx,
		fromTo.From(),
		source,
		true,
		uotm,
		common.CpkOptions{})
	if err != nil {
		return nil, nil, err
	}

	var errorMsg = ""

	// For an Azure source, if there is no SAS, the cred type is Anonymous and the resource is not Azure public blob, tell the user they need to pass a new SAS.
	if fromTo.From().IsAzure() && srcCredType == common.ECredentialType.Anonymous() && source.SAS == "" {
		if !(fromTo.From() == common.ELocation.Blob() && isSrcPublic) {
			errorMsg += "source-sas"
		}
	}

	dstCredType, isDstPublic, err := GetCredentialTypeForLocation(ctx,
		fromTo.To(),
		destination,
		false,
		uotm,
		common.CpkOptions{})
	if err != nil {
		return nil, nil, err
	}

	if fromTo.To().IsAzure() && dstCredType == common.ECredentialType.Anonymous() && destination.SAS == "" {
		if !(fromTo.To() == common.ELocation.Blob() && isDstPublic) {
			if errorMsg == "" {
				errorMsg = "destination-sas"
			} else {
				errorMsg += " and destination-sas"
			}
		}
	}
	if errorMsg != "" {
		return nil, nil, fmt.Errorf("the %s switch must be provided to resume the job", errorMsg)
	}

	var tc azcore.TokenCredential
	if srcCredType.IsAzureOAuth() || dstCredType.IsAzureOAuth() {
		// Get token from env var or cache.
		tokenInfo, err := uotm.GetTokenInfo(ctx)
		if err != nil {
			return nil, nil, err
		}

		tc, err = tokenInfo.GetTokenCredential()
		if err != nil {
			return nil, nil, err
		}
	}

	var reauthTok *common.ScopedAuthenticator
	if at, ok := tc.(common.AuthenticateToken); ok { // We don't need two different tokens here since it gets passed in just the same either way.
		// This will cause a reauth with StorageScope, which is fine, that's the original Authenticate call as it stands.
		reauthTok = (*common.ScopedAuthenticator)(common.NewScopedCredential(at, common.ECredentialType.OAuthToken()))
	}
	// But we don't want to supply a reauth token if we're not using OAuth. That could cause problems if say, a SAS is invalid.
	options := CreateClientOptions(common.AzcopyCurrentJobLogger, nil, common.Iff(srcCredType.IsAzureOAuth(), reauthTok, nil))

	var fileSrcClientOptions any
	if fromTo.From() == common.ELocation.File() || fromTo.From() == common.ELocation.FileNFS() {
		fileSrcClientOptions = &common.FileClientOptions{
			AllowTrailingDot: jobDetails.TrailingDot.IsEnabled(), //Access the trailingDot option of the job
		}
	}
	srcServiceClient, err := common.GetServiceClientForLocation(fromTo.From(), source, srcCredType, tc, &options, fileSrcClientOptions)
	if err != nil {
		return nil, nil, err
	}

	var srcCred *common.ScopedToken
	if fromTo.IsS2S() && srcCredType.IsAzureOAuth() {
		srcCred = common.NewScopedCredential(tc, srcCredType)
	}
	options = CreateClientOptions(common.AzcopyCurrentJobLogger, srcCred, common.Iff(dstCredType.IsAzureOAuth(), reauthTok, nil))
	var fileClientOptions any
	if fromTo.To() == common.ELocation.File() || fromTo.To() == common.ELocation.FileNFS() {
		fileClientOptions = &common.FileClientOptions{
			AllowSourceTrailingDot: jobDetails.TrailingDot.IsEnabled() && fromTo.From() == common.ELocation.File(),
			AllowTrailingDot:       jobDetails.TrailingDot.IsEnabled(),
		}
	}
	dstServiceClient, err := common.GetServiceClientForLocation(fromTo.To(), destination, dstCredType, tc, &options, fileClientOptions)
	if err != nil {
		return nil, nil, err
	}
	return srcServiceClient, dstServiceClient, nil
}
