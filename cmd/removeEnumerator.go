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
	"path"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/filesystem"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/service"
	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
)

var ErrNothingToRemove = errors.New("nothing found to remove")

// provide an enumerator that lists a given resource (Blob, File)
// and schedule delete transfers to remove them
// TODO: Make this merge into the other copy refactor code
// TODO: initEnumerator is significantly more verbose at this point, evaluate the impact of switching over
func newRemoveEnumerator(cca *CookedCopyCmdArgs) (enumerator *traverser.CopyEnumerator, err error) {
	var sourceTraverser traverser.ResourceTraverser

	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	from := cca.FromTo.From()
	if !from.SupportsTrailingDot() {
		cca.trailingDot = common.ETrailingDotOption.Disable()
	}

	var reauthTok *common.ScopedAuthenticator
	if at, ok := cca.credentialInfo.OAuthTokenInfo.TokenCredential.(common.AuthenticateToken); ok { // We don't need two different tokens here since it gets passed in just the same either way.
		// This will cause a reauth with StorageScope, which is fine, that's the original Authenticate call as it stands.
		reauthTok = (*common.ScopedAuthenticator)(common.NewScopedCredential(at, common.ECredentialType.OAuthToken()))
	}

	options := traverser.CreateClientOptions(common.AzcopyCurrentJobLogger, nil, reauthTok)
	var fileClientOptions any
	if cca.FromTo.From().IsFile() {
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

	// Include-path is handled by ListOfFilesChannel.
	sourceTraverser, err = traverser.InitResourceTraverser(cca.Source, cca.FromTo.From(), ctx, traverser.InitResourceTraverserOptions{
		Client:         targetServiceClient,
		CredentialType: cca.credentialInfo.CredentialType,

		ListOfFiles:      cca.ListOfFilesChannel,
		ListOfVersionIDs: cca.ListOfVersionIDsChannel,

		CpkOptions: cca.CpkOptions,

		PermanentDelete:   cca.permanentDeleteOption,
		TrailingDotOption: cca.trailingDot,

		Recursive:               cca.Recursive,
		IncludeDirectoryStubs:   cca.IncludeDirectoryStubs,
		GetPropertiesInFrontend: true,
		StripTopDir:             cca.StripTopDir,

		ExcludeContainers: cca.excludeContainer,
		HardlinkHandling:  common.EHardlinkHandlingType.Follow(),
	})

	// report failure to create traverser
	if err != nil {
		return nil, err
	}

	includeFilters := traverser.BuildIncludeFilters(cca.IncludePatterns)
	excludeFilters := traverser.BuildExcludeFilters(cca.ExcludePatterns, false)
	excludePathFilters := traverser.BuildExcludeFilters(cca.ExcludePathPatterns, true)
	includeSoftDelete := traverser.BuildIncludeSoftDeleted(cca.permanentDeleteOption)

	// set up the filters in the right order
	filters := append(includeFilters, excludeFilters...)
	filters = append(filters, excludePathFilters...)
	filters = append(filters, includeSoftDelete...)
	if cca.IncludeBefore != nil {
		filters = append(filters, &traverser.IncludeBeforeDateFilter{Threshold: *cca.IncludeBefore})
	}

	if cca.IncludeAfter != nil {
		filters = append(filters, &traverser.IncludeAfterDateFilter{Threshold: *cca.IncludeAfter})
	}

	// decide our folder transfer strategy
	// (Must enumerate folders when deleting from a folder-aware location. Can't do folder deletion just based on file
	// deletion, because that would not handle folders that were empty at the start of the job).
	// isHNStoHNS is IGNORED here, because BlobFS locations don't take this route currently.
	fpo, message := azcopy.NewFolderPropertyOption(cca.FromTo, cca.Recursive, cca.StripTopDir, filters, false, false, false, false, cca.IncludeDirectoryStubs)
	// do not print Info message if in dry run mode
	if !cca.dryrunMode {
		glcm.Info(message)
	}
	common.LogToJobLogWithPrefix(message, common.LogInfo)

	if err != nil {
		return nil, err
	}
	transferScheduler := newRemoveTransferProcessor(cca, azcopy.NumOfFilesPerDispatchJobPart, fpo, targetServiceClient)

	finalize := func() error {
		jobInitiated, err := transferScheduler.DispatchFinalPart()
		if err != nil {
			if cca.dryrunMode {
				return nil
			} else if err == azcopy.NothingScheduledError {
				// No log file needed. Logging begins as a part of awaiting job completion.
				return ErrNothingToRemove
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

	return traverser.NewCopyEnumerator(sourceTraverser, filters, transferScheduler.ScheduleCopyTransfer, finalize), nil
}

// TODO move after ADLS/Blob interop goes public
// TODO this simple remove command is only here to support the scenario temporarily
// Ultimately, this code can be merged into the newRemoveEnumerator
func removeBfsResources(cca *CookedCopyCmdArgs) (err error) {
	ctx := context.WithValue(context.Background(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	sourceURL, _ := cca.Source.String()
	var reauthTok *common.ScopedAuthenticator
	if at, ok := cca.credentialInfo.OAuthTokenInfo.TokenCredential.(common.AuthenticateToken); ok { // We don't need two different tokens here since it gets passed in just the same either way.
		// This will cause a reauth with StorageScope, which is fine, that's the original Authenticate call as it stands.
		reauthTok = (*common.ScopedAuthenticator)(common.NewScopedCredential(at, common.ECredentialType.OAuthToken()))
	}

	options := traverser.CreateClientOptions(common.AzcopyCurrentJobLogger, nil, reauthTok)

	targetServiceClient, err := common.GetServiceClientForLocation(cca.FromTo.From(), cca.Source, cca.credentialInfo.CredentialType, cca.credentialInfo.OAuthTokenInfo.TokenCredential, &options, nil)
	if err != nil {
		return err
	}

	dsc, _ := targetServiceClient.DatalakeServiceClient() // We've just created client above, need not verify error here.

	transferProcessor := newRemoveTransferProcessor(cca, azcopy.NumOfFilesPerDispatchJobPart, common.EFolderPropertiesOption.AllFolders(), targetServiceClient)

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
			err := transferProcessor.ScheduleCopyTransfer(traverser.NewStoredObject(
				nil,
				path.Base(datalakeURLParts.PathName),
				"",
				common.EEntityType.File(), // blobfs deleter doesn't differentiate
				time.Now(),
				0,
				traverser.NoContentProps,
				traverser.NoContentProps,
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
				err := transferProcessor.ScheduleCopyTransfer(traverser.NewStoredObject(
					nil,
					path.Base(datalakeURLParts.PathName),
					childPath,
					common.EEntityType.File(), // blobfs deleter doesn't differentiate
					time.Now(),
					0,
					traverser.NoContentProps,
					traverser.NoContentProps,
					nil,
					"",
				))

				if err != nil {
					return err
				}
			}
		}
	}

	_, err = transferProcessor.DispatchFinalPart()
	return err
}

func dryrunRemoveSingleDFSResource(ctx context.Context, dsc *service.Client, datalakeURLParts azdatalake.URLParts, recursive bool) error {
	//deleting a filesystem
	if datalakeURLParts.PathName == "" {
		glcm.Dryrun(func(of OutputFormat) string {
			switch of {
			case of.Text():
				return fmt.Sprintf("DRYRUN: remove %s", dsc.NewFileSystemClient(datalakeURLParts.FileSystemName).DFSURL())
			case of.Json():
				tx := DryrunTransfer{
					EntityType: common.EEntityType.Folder(),
					FromTo:     common.EFromTo.BlobFSTrash(),
					Source:     dsc.NewFileSystemClient(datalakeURLParts.FileSystemName).DFSURL(),
				}

				buf, _ := json.Marshal(tx)
				return string(buf)
			default:
				panic("unsupported output format " + of.String())
			}
		})
		return nil
	}
	// we do not know if the source is a file or a directory
	// we assume it is a directory and get its properties
	directoryClient := dsc.NewFileSystemClient(datalakeURLParts.FileSystemName).NewDirectoryClient(datalakeURLParts.PathName)
	props, err := directoryClient.GetProperties(ctx, nil)
	if err != nil {
		return fmt.Errorf("cannot verify resource due to error: %s", err)
	}

	// if the source URL is actually a file
	// then we should short-circuit and simply remove that file
	resourceType := common.IffNotNil(props.ResourceType, "")
	if strings.EqualFold(resourceType, "file") {
		glcm.Dryrun(func(of OutputFormat) string {
			switch of {
			case of.Text():
				return fmt.Sprintf("DRYRUN: remove %s", directoryClient.DFSURL())
			case of.Json():
				tx := DryrunTransfer{
					EntityType: common.EEntityType.File(),
					FromTo:     common.EFromTo.BlobFSTrash(),
					Source:     directoryClient.DFSURL(),
				}

				buf, _ := json.Marshal(tx)
				return string(buf)
			default:
				panic("unsupported output format " + of.String())
			}
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

			glcm.Dryrun(func(of OutputFormat) string {
				uri := dsc.NewFileSystemClient(datalakeURLParts.FileSystemName).NewFileClient(*v.Name).DFSURL()

				switch of {
				case of.Text():
					return fmt.Sprintf("DRYRUN: remove %s", uri)
				case of.Json():
					tx := DryrunTransfer{
						EntityType: common.Iff(entityType == "directory", common.EEntityType.Folder(), common.EEntityType.File()),
						FromTo:     common.EFromTo.BlobFSTrash(),
						Source:     uri,
					}

					buf, _ := json.Marshal(tx)
					return string(buf)
				default:
					panic("unsupported output format " + of.String())
				}
			})
		}
	}
	return nil
}
