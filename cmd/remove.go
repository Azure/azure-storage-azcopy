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
	"fmt"
	"strings"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/spf13/cobra"
)

func init() {
	raw := rawCopyCmdArgs{}
	// deleteCmd represents the delete command
	var deleteCmd = &cobra.Command{
		Use:        "remove [resourceURL]",
		Aliases:    []string{"rm", "r"},
		SuggestFor: []string{"delete", "del"},
		Short:      removeCmdShortDescription,
		Long:       removeCmdLongDescription,
		Example:    removeCmdExample,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("remove command only takes 1 arguments. Passed %d arguments", len(args))
			}

			// the resource to delete is set as the source
			raw.src = args[0]

			if raw.fromTo == "" {
				srcLocationType := InferArgumentLocation(raw.src)
				switch srcLocationType {
				case common.ELocation.Blob():
					raw.fromTo = common.EFromTo.BlobTrash().String()
				case common.ELocation.File(), common.ELocation.FileNFS():
					raw.fromTo = common.EFromTo.FileTrash().String()
				case common.ELocation.BlobFS():
					raw.fromTo = common.EFromTo.BlobFSTrash().String()
				default:
					return fmt.Errorf("invalid source type %s to delete. azcopy support removing blobs/files/adls gen2", srcLocationType.String())
				}
			} else if raw.fromTo != "" {
				err := strings.Contains(raw.fromTo, "Trash")
				if !err {
					return fmt.Errorf("invalid destination. please enter a valid destination, i.e. BlobTrash, FileTrash, BlobFSTrash")
				}
			}
			raw.setMandatoryDefaults()

			// in the case of remove, we are fairly certain that the user wants all the blobs to be removed
			// and this includes the stubs that represent directories (with metadata 'hdi_isfolder = true')
			raw.includeDirectoryStubs = true

			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			glcm.EnableInputWatcher()
			if cancelFromStdin {
				glcm.EnableCancelFromStdIn()
			}

			cooked, err := raw.cook()
			if err != nil {
				glcm.Error("failed to parse user input due to error: " + err.Error())
			}

			if cooked.permanentDeleteOption != common.EPermanentDeleteOption.None() {
				glcm.Info("Permanent delete is a PREVIEW feature and soft-deleted snapshots/versions will be deleted PERMANENTLY. Please proceed with caution.")
			}

			cooked.commandString = copyHandlerUtil{}.ConstructCommandStringFromArgs()
			err = cooked.process()
			if err != nil {
				glcm.Error("failed to perform remove command due to error: " + err.Error() + getErrorCodeUrl(err))
			}

			if cooked.dryrunMode {
				glcm.Exit(nil, common.EExitCode.Success())
			}

			glcm.SurrenderControl()
		},
	}
	rootCmd.AddCommand(deleteCmd)

	deleteCmd.PersistentFlags().BoolVar(&raw.recursive, "recursive", false, "False by default. Look into sub-directories recursively when syncing between directories.")
	deleteCmd.PersistentFlags().StringVar(&raw.include, "include-pattern", "", "Include only files where the name matches the pattern list. For example: *.jpg;*.pdf;exactName")
	deleteCmd.PersistentFlags().StringVar(&raw.includePath, "include-path", "", "Include only these paths when removing. "+
		"This option does not support wildcard characters (*). Checks relative path prefix. For example: myFolder;myFolder/subDirName/file.pdf")
	deleteCmd.PersistentFlags().StringVar(&raw.exclude, "exclude-pattern", "", "Exclude files where the name matches the pattern list. For example: *.jpg;*.pdf;exactName")
	deleteCmd.PersistentFlags().StringVar(&raw.excludePath, "exclude-path", "", "Exclude these paths when removing. "+
		"This option does not support wildcard characters (*). Checks relative path prefix. For example: myFolder;myFolder/subDirName/file.pdf")
	deleteCmd.PersistentFlags().BoolVar(&raw.forceIfReadOnly, "force-if-read-only", false, "False by default. When deleting an Azure Files file or folder, force the deletion to work even if the existing object is has its read-only attribute set")
	deleteCmd.PersistentFlags().StringVar(&raw.listOfFilesToCopy, "list-of-files", "", "Defines the location of a text file which contains the list of files and directories to be deleted. The relative paths should be delimited by line breaks, and the paths should NOT be URL-encoded.")
	deleteCmd.PersistentFlags().StringVar(&raw.deleteSnapshotsOption, "delete-snapshots", "", "By default, the delete operation fails if a blob has snapshots. Specify 'include' to remove the root blob and all its snapshots; alternatively specify 'only' to remove only the snapshots but keep the root blob.")
	deleteCmd.PersistentFlags().StringVar(&raw.listOfVersionIDs, "list-of-versions", "", "Specifies a text file where each version id is listed on a separate line. Ensure that the source must point to a single blob and all the version ids specified in the file using this flag must belong to the source blob only. Specified version ids of the given blob will get deleted from Azure Storage.")
	deleteCmd.PersistentFlags().BoolVar(&raw.dryrun, "dry-run", false, "False by default. Prints the path files that would be removed by the command. This flag does not trigger the removal of the files.")
	deleteCmd.PersistentFlags().StringVar(&raw.fromTo, "from-to", "", "Optionally specifies the source destination combination. For Example: BlobTrash, FileTrash, BlobFSTrash")
	deleteCmd.PersistentFlags().StringVar(&raw.permanentDeleteOption, "permanent-delete", "none", "This is a preview feature that PERMANENTLY deletes soft-deleted snapshots/versions. Possible values include "+strings.Join(common.ValidPermanentDeleteOptions(), ", ")+". Default is 'none'.")
	deleteCmd.PersistentFlags().StringVar(&raw.includeBefore, common.IncludeBeforeFlagName, "", "Include only those files modified before or on the given date/time. The value should be in ISO8601 format. If no timezone is specified, the value is assumed to be in the local timezone of the machine running AzCopy. E.g. '2020-08-19T15:04:00Z' for a UTC time, or '2020-08-19' for midnight (00:00) in the local timezone. As of AzCopy 10.7, this flag applies only to files, not folders, so folder properties won't be copied when using this flag with --preserve-info or --preserve-permissions.")
	deleteCmd.PersistentFlags().StringVar(&raw.includeAfter, common.IncludeAfterFlagName, "", "Include only those files modified on or after the given date/time. The value should be in ISO8601 format. If no timezone is specified, the value is assumed to be in the local timezone of the machine running AzCopy. E.g. '2020-08-19T15:04:00Z' for a UTC time, or '2020-08-19' for midnight (00:00) in the local timezone. As of AzCopy 10.5, this flag applies only to files, not folders, so folder properties won't be copied when using this flag with --preserve-info or --preserve-permissions.")
	deleteCmd.PersistentFlags().StringVar(&raw.trailingDot, "trailing-dot", "", "'Enable' by default to treat file share related operations in a safe manner. Available options: "+strings.Join(common.ValidTrailingDotOptions(), ", ")+". "+
		"Choose 'Disable' to go back to legacy (potentially unsafe) treatment of trailing dot files where the file service will trim any trailing dots in paths. This can result in potential data corruption if the transfer contains two paths that differ only by a trailing dot (ex: mypath and mypath.). If this flag is set to 'Disable' and AzCopy encounters a trailing dot file, it will warn customers in the scanning log but will not attempt to abort the operation."+
		"If the destination does not support trailing dot files (Windows or Blob Storage), AzCopy will fail if the trailing dot file is the root of the transfer and skip any trailing dot paths encountered during enumeration.")
	// Public Documentation: https://docs.microsoft.com/en-us/azure/storage/blobs/encryption-customer-provided-keys
	// Clients making requests against Azure Blob storage have the option to provide an encryption key on a per-request basis.
	// Including the encryption key on the request provides granular control over encryption settings for Blob storage operations.
	// Customer-provided keys can be stored in Azure Key Vault or in another key store linked to storage account.
	deleteCmd.PersistentFlags().StringVar(&raw.cpkScopeInfo, "cpk-by-name", "", "Client provided key by name let clients making requests against Azure Blob storage an option to provide an encryption key on a per-request basis. Provided key name will be fetched from Azure Key Vault and will be used to encrypt the data.")
	deleteCmd.PersistentFlags().BoolVar(&raw.cpkInfo, "cpk-by-value", false, "False by default. Client provided key by name let clients making requests against Azure Blob storage an option to provide an encryption key on a per-request basis. Provided key and its hash will be fetched from environment variables CPK_ENCRYPTION_KEY and CPK_ENCRYPTION_KEY_SHA256 must be set).")
}
