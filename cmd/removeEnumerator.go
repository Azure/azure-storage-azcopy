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
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/filesystem"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/service"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
)

var NothingToRemoveError = errors.New("nothing found to remove")

// provide an enumerator that lists a given resource (Blob, File)
// and schedule delete transfers to remove them
// TODO: Make this merge into the other copy refactor code
// TODO: initEnumerator is significantly more verbose at this point, evaluate the impact of switching over
func newRemoveEnumerator(cca *CookedCopyCmdArgs) (enumerator *CopyEnumerator, err error) {
	var sourceTraverser ResourceTraverser

	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	// Include-path is handled by ListOfFilesChannel.
	sourceTraverser, err = InitResourceTraverser(cca.Source, cca.FromTo.From(), &ctx, &cca.credentialInfo, common.ESymlinkHandlingType.Skip(), cca.ListOfFilesChannel, cca.Recursive, true, cca.IncludeDirectoryStubs, cca.permanentDeleteOption, func(common.EntityType) {}, cca.ListOfVersionIDs, false, common.ESyncHashType.None(), common.EPreservePermissionsOption.None(), azcopyLogVerbosity, cca.CpkOptions, nil, cca.StripTopDir, cca.trailingDot, nil, cca.excludeContainer)

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
	if cca.IncludeBefore != nil {
		filters = append(filters, &IncludeBeforeDateFilter{Threshold: *cca.IncludeBefore})
	}

	if cca.IncludeAfter != nil {
		filters = append(filters, &IncludeAfterDateFilter{Threshold: *cca.IncludeAfter})
	}

	// decide our folder transfer strategy
	// (Must enumerate folders when deleting from a folder-aware location. Can't do folder deletion just based on file
	// deletion, because that would not handle folders that were empty at the start of the job).
	// isHNStoHNS is IGNORED here, because BlobFS locations don't take this route currently.
	fpo, message := NewFolderPropertyOption(cca.FromTo, cca.Recursive, cca.StripTopDir, filters, false, false, false, false, cca.IncludeDirectoryStubs)
	// do not print Info message if in dry run mode
	if !cca.dryrunMode {
		glcm.Info(message)
	}
	if jobsAdmin.JobsAdmin != nil {
		jobsAdmin.JobsAdmin.LogToJobLog(message, common.LogInfo)
	}

	targetURL, _ := cca.Source.String()
	from := cca.FromTo.From()
	if !from.SupportsTrailingDot() {
		cca.trailingDot = common.ETrailingDotOption.Disable()
	}
	options := createClientOptions(common.AzcopyCurrentJobLogger, nil)
	var fileClientOptions any
	if cca.FromTo.From() == common.ELocation.File() {
		fileClientOptions = &common.FileClientOptions{AllowTrailingDot: cca.trailingDot == common.ETrailingDotOption.Enable()}
	}
	targetServiceClient, err := common.GetServiceClientForLocation(
		cca.FromTo.From(),
		targetURL,
		cca.credentialInfo.CredentialType,
		cca.credentialInfo.OAuthTokenInfo.TokenCredential,
		&options,
		fileClientOptions,
	)
	if err != nil {
		return nil, err
	}
	transferScheduler := newRemoveTransferProcessor(cca, NumOfFilesPerDispatchJobPart, fpo, targetServiceClient)

	finalize := func() error {
		jobInitiated, err := transferScheduler.dispatchFinalPart()
		if err != nil {
			if cca.dryrunMode {
				return nil
			} else if err == NothingScheduledError {
				// No log file needed. Logging begins as a part of awaiting job completion.
				return NothingToRemoveError
			}

			return err
		}

		// TODO: this appears to be obsolete due to the above err == NothingScheduledError. Review/discuss.
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

// TODO move after ADLS/Blob interop goes public
// TODO this simple remove command is only here to support the scenario temporarily
// Ultimately, this code can be merged into the newRemoveEnumerator
func removeBfsResources(cca *CookedCopyCmdArgs) (err error) {
	ctx := context.WithValue(context.Background(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	sourceURL, _ := cca.Source.String()
	options := createClientOptions(common.AzcopyCurrentJobLogger, nil)
	
	targetServiceClient, err := common.GetServiceClientForLocation(cca.FromTo.From(), sourceURL, cca.credentialInfo.CredentialType, cca.credentialInfo.OAuthTokenInfo.TokenCredential, &options, nil)
	if err != nil {
		return err
	}

	dsc, _ := targetServiceClient.DatalakeServiceClient() // We've just created client above, need not verify error here.

	transferProcessor := newRemoveTransferProcessor(cca, NumOfFilesPerDispatchJobPart, common.EFolderPropertiesOption.AllFolders(), targetServiceClient)

	// return an error if the unsupported options are passed in
	if len(cca.InitModularFilters()) > 0 {
		return errors.New("filter options, such as include/exclude, are not supported for this destination")
		// because we just ignore them and delete the root
	}

	// patterns are not supported
	if strings.Contains(cca.Source.Value, "*") {
		return errors.New("pattern matches are not supported in this command")
	}

	// parse the given source URL into parts, which separates the filesystem name and directory/file path
	datalakeURLParts, err := azdatalake.ParseURL(sourceURL)
	if err != nil {
		return err
	}

	if cca.ListOfFilesChannel == nil {
		if cca.dryrunMode {
			return dryrunRemoveSingleDFSResource(ctx, dsc, datalakeURLParts, cca.Recursive)
		} else {
			err := transferProcessor.scheduleCopyTransfer(newStoredObject(
				nil,
				path.Base(datalakeURLParts.PathName),
				"",
				common.EEntityType.File(), // blobfs deleter doesn't differentiate
				time.Now(),
				0,
				noContentProps,
				noContentProps,
				nil,
				"",
			))

			if err != nil {
				return err
			}
		}
	} else {
		// list of files is given, record the parent path
		parentPath := datalakeURLParts.PathName

		// read from the list of files channel to find out what needs to be deleted.
		childPath, ok := <-cca.ListOfFilesChannel
		for ; ok; childPath, ok = <-cca.ListOfFilesChannel {
			//remove the child path
			datalakeURLParts.PathName = common.GenerateFullPath(parentPath, childPath)
			if cca.dryrunMode {
				return dryrunRemoveSingleDFSResource(ctx, dsc, datalakeURLParts, cca.Recursive)
			} else {
				err := transferProcessor.scheduleCopyTransfer(newStoredObject(
					nil,
					path.Base(datalakeURLParts.PathName),
					childPath,
					common.EEntityType.File(), // blobfs deleter doesn't differentiate
					time.Now(),
					0,
					noContentProps,
					noContentProps,
					nil,
					"",
				))

				if err != nil {
					return err
				}
			}
		}
	}

	_, err = transferProcessor.dispatchFinalPart()
	return err
}

func dryrunRemoveSingleDFSResource(ctx context.Context, dsc *service.Client, datalakeURLParts azdatalake.URLParts, recursive bool) error {
	//deleting a filesystem
	if datalakeURLParts.PathName == "" {
		glcm.Dryrun(func(_ common.OutputFormat) string {
			return fmt.Sprintf("DRYRUN: remove filesystem %s", datalakeURLParts.FileSystemName)
		})
		return nil
	}
	// we do not know if the source is a file or a directory
	// we assume it is a directory and get its properties
	directoryClient := dsc.NewFileSystemClient(datalakeURLParts.FileSystemName).NewDirectoryClient(datalakeURLParts.PathName)
	var respFromCtx *http.Response
	ctxWithResp := runtime.WithCaptureResponse(ctx, &respFromCtx)
	_, err := directoryClient.GetProperties(ctxWithResp, nil)
	if err != nil {
		return fmt.Errorf("cannot verify resource due to error: %s", err)
	}

	// if the source URL is actually a file
	// then we should short-circuit and simply remove that file
	resourceType := respFromCtx.Header.Get("x-ms-resource-type")
	if strings.EqualFold(resourceType, "file") {
		glcm.Dryrun(func(_ common.OutputFormat) string {
			return fmt.Sprintf("DRYRUN: remove file %s", datalakeURLParts.PathName)
		})
		return nil
	}

	pathName := datalakeURLParts.PathName
	datalakeURLParts.PathName = ""
	pager := dsc.NewFileSystemClient(datalakeURLParts.FileSystemName).NewListPathsPager(recursive, &filesystem.ListPathsOptions{Prefix: &pathName})
	for pager.More() {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			return err
		}

		for _, v := range resp.Paths {
			entityType := "directory"
			if v.IsDirectory == nil || !*v.IsDirectory {
				entityType = "file"
			}

			glcm.Dryrun(func(_ common.OutputFormat) string {
				return fmt.Sprintf("DRYRUN: remove %s %s", entityType, *v.Name)
			})
		}
	}
	return nil
}
