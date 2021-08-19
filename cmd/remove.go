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

			// infer the location of the delete
			srcLocationType := InferArgumentLocation(raw.src)
			if srcLocationType == common.ELocation.Blob() {
				raw.fromTo = common.EFromTo.BlobTrash().String()
			} else if srcLocationType == common.ELocation.File() {
				raw.fromTo = common.EFromTo.FileTrash().String()
			} else if srcLocationType == common.ELocation.BlobFS() {
				raw.fromTo = common.EFromTo.BlobFSTrash().String()
			} else {
				return fmt.Errorf("invalid source type %s to delete. azcopy support removing blobs/files/adls gen2", srcLocationType.String())
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
			cooked.commandString = copyHandlerUtil{}.ConstructCommandStringFromArgs()
			err = cooked.process()
			if err != nil {
				glcm.Error("failed to perform remove command due to error: " + err.Error())
			}

			glcm.SurrenderControl()
		},
	}
	rootCmd.AddCommand(deleteCmd)

	deleteCmd.PersistentFlags().BoolVar(&raw.recursive, "recursive", false, "Look into sub-directories recursively when syncing between directories.")
	deleteCmd.PersistentFlags().StringVar(&raw.logVerbosity, "log-level", "INFO", "Define the log verbosity for the log file. Available levels include: INFO(all requests/responses), WARNING(slow responses), ERROR(only failed requests), and NONE(no output logs). (default 'INFO')")
	deleteCmd.PersistentFlags().StringVar(&raw.include, "include-pattern", "", "Include only files where the name matches the pattern list. For example: *.jpg;*.pdf;exactName")
	deleteCmd.PersistentFlags().StringVar(&raw.includePath, "include-path", "", "Include only these paths when removing. "+
		"This option does not support wildcard characters (*). Checks relative path prefix. For example: myFolder;myFolder/subDirName/file.pdf")
	deleteCmd.PersistentFlags().StringVar(&raw.exclude, "exclude-pattern", "", "Exclude files where the name matches the pattern list. For example: *.jpg;*.pdf;exactName")
	deleteCmd.PersistentFlags().StringVar(&raw.excludePath, "exclude-path", "", "Exclude these paths when removing. "+
		"This option does not support wildcard characters (*). Checks relative path prefix. For example: myFolder;myFolder/subDirName/file.pdf")
	deleteCmd.PersistentFlags().BoolVar(&raw.forceIfReadOnly, "force-if-read-only", false, "When deleting an Azure Files file or folder, force the deletion to work even if the existing object is has its read-only attribute set")
	deleteCmd.PersistentFlags().StringVar(&raw.listOfFilesToCopy, "list-of-files", "", "Defines the location of a file which contains the list of files and directories to be deleted. The relative paths should be delimited by line breaks, and the paths should NOT be URL-encoded.")
	deleteCmd.PersistentFlags().StringVar(&raw.deleteSnapshotsOption, "delete-snapshots", "", "By default, the delete operation fails if a blob has snapshots. Specify 'include' to remove the root blob and all its snapshots; alternatively specify 'only' to remove only the snapshots but keep the root blob.")
	deleteCmd.PersistentFlags().StringVar(&raw.listOfVersionIDs, "list-of-versions", "", "Specifies a file where each version id is listed on a separate line. Ensure that the source must point to a single blob and all the version ids specified in the file using this flag must belong to the source blob only. Specified version ids of the given blob will get deleted from Azure Storage.")
}
