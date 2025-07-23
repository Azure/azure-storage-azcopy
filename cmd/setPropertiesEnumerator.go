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
	"strings"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
)

// provides an enumerator that lists a given resource and schedules setProperties on them

func setPropertiesEnumerator(cca *CookedCopyCmdArgs) (enumerator *CopyEnumerator, err error) {
	var sourceTraverser ResourceTraverser

	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	var srcCredInfo common.CredentialInfo

	if srcCredInfo, _, err = GetCredentialInfoForLocation(ctx, cca.FromTo.From(), cca.Source, true, cca.CpkOptions); err != nil {
		return nil, err
	}
	if cca.FromTo == common.EFromTo.FileNone() && (srcCredInfo.CredentialType == common.ECredentialType.Anonymous() && cca.Source.SAS == "") {
		return nil, errors.New("a SAS token (or S3 access key) is required as a part of the input for set-properties on File Storage")
	}

	// Include-path is handled by ListOfFilesChannel.
	sourceTraverser, err = InitResourceTraverser(cca.Source, cca.FromTo.From(), ctx, InitResourceTraverserOptions{
		Credential: &cca.credentialInfo,

		ListOfFiles:      cca.ListOfFilesChannel,
		ListOfVersionIDs: cca.ListOfVersionIDsChannel,

		CpkOptions: cca.CpkOptions,

		SymlinkHandling:   common.ESymlinkHandlingType.Preserve(),
		PermanentDelete:   cca.permanentDeleteOption,
		TrailingDotOption: cca.trailingDot,

		Recursive:             cca.Recursive,
		IncludeDirectoryStubs: cca.IncludeDirectoryStubs,
		StripTopDir:           cca.StripTopDir,

		ExcludeContainers: cca.excludeContainer,
		HardlinkHandling:  cca.hardlinks,
	})

	// report failure to create traverser
	if err != nil {
		return nil, err
	}

	includeFilters := buildIncludeFilters(cca.IncludePatterns)
	excludeFilters := buildExcludeFilters(cca.ExcludePatterns, false)
	excludePathFilters := buildExcludeFilters(cca.ExcludePathPatterns, true)
	includeSoftDelete := buildIncludeSoftDeleted(cca.permanentDeleteOption)

	// set up the filters in the right order
	filters := append(includeFilters, excludeFilters...)
	filters = append(filters, excludePathFilters...)
	filters = append(filters, includeSoftDelete...)

	fpo, message := NewFolderPropertyOption(cca.FromTo, cca.Recursive, cca.StripTopDir, filters, false, false, false, strings.EqualFold(cca.Destination.Value, common.Dev_Null), cca.IncludeDirectoryStubs)
	// do not print Info message if in dry run mode
	if !cca.dryrunMode {
		glcm.Info(message)
	}
	if jobsAdmin.JobsAdmin != nil {
		jobsAdmin.JobsAdmin.LogToJobLog(message, common.LogInfo)
	}

	var reauthTok *common.ScopedAuthenticator
	if at, ok := cca.credentialInfo.OAuthTokenInfo.TokenCredential.(common.AuthenticateToken); ok { // We don't need two different tokens here since it gets passed in just the same either way.
		// This will cause a reauth with StorageScope, which is fine, that's the original Authenticate call as it stands.
		reauthTok = (*common.ScopedAuthenticator)(common.NewScopedCredential(at, common.ECredentialType.OAuthToken()))
	}

	options := createClientOptions(common.AzcopyCurrentJobLogger, nil, reauthTok)
	var fileClientOptions any
	if IsFileEndpoint(cca.FromTo.From()) {
		fileClientOptions = &common.FileClientOptions{AllowTrailingDot: cca.trailingDot.IsEnabled()}
	}

	targetServiceClient, err := common.GetServiceClientForLocation(
		cca.FromTo.From(),
		cca.Source,
		cca.credentialInfo.CredentialType,
		cca.credentialInfo.OAuthTokenInfo.TokenCredential,
		&options,
		fileClientOptions,
	)
	if err != nil {
		return nil, err
	}

	transferScheduler := setPropertiesTransferProcessor(cca, NumOfFilesPerDispatchJobPart, fpo, targetServiceClient)

	finalize := func() error {
		jobInitiated, err := transferScheduler.dispatchFinalPart()
		if err != nil {
			if cca.dryrunMode {
				return nil
			} else if err == NothingScheduledError {
				// No log file needed. Logging begins as a part of awaiting job completion.
				return ErrNothingToRemove
			}

			return err
		}

		if !jobInitiated {
			if cca.isCleanupJob {
				glcm.Error("Cleanup completed (nothing needed to be deleted)")
			} else {
				glcm.Error("Nothing to delete. Please verify that recursive flag is set properly if targeting a directory.")
			}
		}

		return nil
	}
	return NewCopyEnumerator(sourceTraverser, filters, transferScheduler.scheduleCopyTransfer, finalize), nil
}
