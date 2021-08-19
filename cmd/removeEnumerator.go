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
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/Azure/azure-pipeline-go/pipeline"

	"github.com/Azure/azure-storage-azcopy/v10/azbfs"
	"github.com/Azure/azure-storage-azcopy/v10/common"
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
	sourceTraverser, err = InitResourceTraverser(cca.Source, cca.FromTo.From(), &ctx, &cca.credentialInfo,
		nil, cca.ListOfFilesChannel, cca.Recursive, false, cca.IncludeDirectoryStubs,
		func(common.EntityType) {}, cca.ListOfVersionIDs, false,
		cca.LogVerbosity.ToPipelineLogLevel(), cca.CpkOptions)

	// report failure to create traverser
	if err != nil {
		return nil, err
	}

	includeFilters := buildIncludeFilters(cca.IncludePatterns)
	excludeFilters := buildExcludeFilters(cca.ExcludePatterns, false)
	excludePathFilters := buildExcludeFilters(cca.ExcludePathPatterns, true)

	// set up the filters in the right order
	filters := append(includeFilters, excludeFilters...)
	filters = append(filters, excludePathFilters...)

	// decide our folder transfer strategy
	// (Must enumerate folders when deleting from a folder-aware location. Can't do folder deletion just based on file
	// deletion, because that would not handle folders that were empty at the start of the job).
	fpo, message := newFolderPropertyOption(cca.FromTo, cca.Recursive, cca.StripTopDir, filters, false, false)
	glcm.Info(message)
	if ste.JobsAdmin != nil {
		ste.JobsAdmin.LogToJobLog(message, pipeline.LogInfo)
	}

	transferScheduler := newRemoveTransferProcessor(cca, NumOfFilesPerDispatchJobPart, fpo)

	finalize := func() error {
		jobInitiated, err := transferScheduler.dispatchFinalPart()
		if err != nil {
			if err == NothingScheduledError {
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
	ctx := context.Background()

	// return an error if the unsupported options are passed in
	if len(cca.InitModularFilters()) > 0 {
		return errors.New("filter options, such as include/exclude, are not supported for this destination")
		// because we just ignore them and delete the root
	}

	// patterns are not supported
	if strings.Contains(cca.Source.Value, "*") {
		return errors.New("pattern matches are not supported in this command")
	}

	// create bfs pipeline
	p, err := createBlobFSPipeline(ctx, cca.credentialInfo, cca.LogVerbosity.ToPipelineLogLevel())
	if err != nil {
		return err
	}

	// attempt to parse the source url
	sourceURL, err := cca.Source.FullURL()
	if err != nil {
		return errors.New("cannot parse source URL")
	}

	// parse the given source URL into parts, which separates the filesystem name and directory/file path
	urlParts := azbfs.NewBfsURLParts(*sourceURL)

	if cca.ListOfFilesChannel == nil {
		successMsg, err := removeSingleBfsResource(urlParts, p, ctx, cca.Recursive)
		if err != nil {
			return err
		}

		glcm.Exit(func(format common.OutputFormat) string {
			if format == common.EOutputFormat.Json() {
				summary := common.ListJobSummaryResponse{
					JobStatus:      common.EJobStatus.Completed(),
					TotalTransfers: 1,
					// It's not meaningful to set FileTransfers or FolderPropertyTransfers because even if its a folder, its not really folder _properties_ which is what the name is
					TransfersCompleted: 1,
					PercentComplete:    100,
				}
				jsonOutput, err := json.Marshal(summary)
				common.PanicIfErr(err)
				return string(jsonOutput)
			}

			return successMsg
		}, common.EExitCode.Success())

		// explicitly exit, since in our tests Exit might be mocked away
		return nil
	}

	// list of files is given, record the parent path
	parentPath := urlParts.DirectoryOrFilePath
	successCount := uint32(0)
	failedTransfers := make([]common.TransferDetail, 0)

	// read from the list of files channel to find out what needs to be deleted.
	childPath, ok := <-cca.ListOfFilesChannel
	for ; ok; childPath, ok = <-cca.ListOfFilesChannel {
		// remove the child path
		urlParts.DirectoryOrFilePath = common.GenerateFullPath(parentPath, childPath)
		successMessage, err := removeSingleBfsResource(urlParts, p, ctx, cca.Recursive)
		if err != nil {
			// the specific error is not included in the details, since it doesn't have a field for full error message
			failedTransfers = append(failedTransfers, common.TransferDetail{Src: childPath, TransferStatus: common.ETransferStatus.Failed()})
			glcm.Info(fmt.Sprintf("Skipping %s due to error %s", childPath, err))
		} else {
			glcm.Info(successMessage)
			successCount += 1
		}
	}

	glcm.Exit(func(format common.OutputFormat) string {
		if format == common.EOutputFormat.Json() {
			status := common.EJobStatus.Completed()
			if len(failedTransfers) > 0 {
				status = common.EJobStatus.CompletedWithErrors()

				// if nothing got deleted
				if successCount == 0 {
					status = common.EJobStatus.Failed()
				}
			}

			summary := common.ListJobSummaryResponse{
				JobStatus:          status,
				TotalTransfers:     successCount + uint32(len(failedTransfers)),
				TransfersCompleted: successCount,
				TransfersFailed:    uint32(len(failedTransfers)),
				PercentComplete:    100,
				FailedTransfers:    failedTransfers,
			}
			jsonOutput, err := json.Marshal(summary)
			common.PanicIfErr(err)
			return string(jsonOutput)
		}

		return fmt.Sprintf("Successfully removed %v entities.", successCount)
	}, common.EExitCode.Success())

	return nil
}

// TODO move after ADLS/Blob interop goes public
// TODO this simple remove command is only here to support the scenario temporarily
func removeSingleBfsResource(urlParts azbfs.BfsURLParts, p pipeline.Pipeline, ctx context.Context, recursive bool) (successMessage string, err error) {
	// deleting a filesystem
	if urlParts.DirectoryOrFilePath == "" {
		fsURL := azbfs.NewFileSystemURL(urlParts.URL(), p)
		_, err := fsURL.Delete(ctx)
		return "Successfully removed the filesystem " + urlParts.FileSystemName, err
	}

	// we do not know if the source is a file or a directory
	// we assume it is a directory and get its properties
	directoryURL := azbfs.NewDirectoryURL(urlParts.URL(), p)
	props, err := directoryURL.GetProperties(ctx)
	if err != nil {
		return "", fmt.Errorf("cannot verify resource due to error: %s", err)
	}

	// if the source URL is actually a file
	// then we should short-circuit and simply remove that file
	if strings.EqualFold(props.XMsResourceType(), "file") {
		fileURL := directoryURL.NewFileUrl()
		_, err := fileURL.Delete(ctx)

		if err == nil {
			return "Successfully removed file: " + urlParts.DirectoryOrFilePath, nil
		}

		return "", err
	}

	// otherwise, remove the directory and follow the continuation token if necessary
	// initialize an empty continuation marker
	marker := ""

	// remove the directory
	// loop will continue until the marker received in the response is empty
	for {
		removeResp, err := directoryURL.Delete(ctx, &marker, recursive)
		if err != nil {
			return "", fmt.Errorf("cannot remove the given resource due to error: %s", err)
		}

		// update the continuation token for the next call
		marker = removeResp.XMsContinuation()

		// determine whether listing should be done
		if marker == "" {
			break
		}
	}

	return "Successfully removed directory: " + urlParts.DirectoryOrFilePath, nil
}
