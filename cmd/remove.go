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

	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/spf13/cobra"
)

func init() {
	// set the block-blob-tier and page-blob-tier to None since Parse fails for "" string
	// while parsing block-blob and page-blob tier.
	raw := rawCopyCmdArgs{blockBlobTier: common.EBlockBlobTier.None().String(), pageBlobTier: common.EPageBlobTier.None().String()}
	// deleteCmd represents the delete command
	var deleteCmd = &cobra.Command{
		Use:        "remove [resourceURL]",
		Aliases:    []string{"rm", "r"},
		SuggestFor: []string{"delete", "del"},
		Short:      removeCmdShortDescription,
		Long:       removeCmdLongDescription,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("remove command only takes 1 arguments. Passed %d arguments", len(args))
			}

			// the resource to delete is set as the source
			raw.src = args[0]

			// infer the location of the delete
			srcLocationType := inferArgumentLocation(raw.src)
			if srcLocationType == common.ELocation.Blob() {
				raw.fromTo = common.EFromTo.BlobTrash().String()
			} else if srcLocationType == common.ELocation.File() {
				raw.fromTo = common.EFromTo.FileTrash().String()
			} else {
				return fmt.Errorf("invalid source type %s pased to delete. azcopy support removing blobs and files only", srcLocationType.String())
			}

			// Since remove uses the copy command arguments cook, set the blobType to None and validation option
			// else parsing the arguments will fail.
			raw.blobType = common.EBlobType.None().String()
			raw.md5ValidationOption = common.DefaultHashValidationOption.String()
			raw.s2sInvalidMetadataHandleOption = common.DefaultInvalidMetadataHandleOption.String()
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			cooked, err := raw.cook()
			if err != nil {
				glcm.Error("failed to parse user input due to error " + err.Error())
			}
			cooked.commandString = copyHandlerUtil{}.ConstructCommandStringFromArgs()
			err = cooked.process()
			if err != nil {
				glcm.Error("failed to perform copy command due to error " + err.Error())
			}

			glcm.SurrenderControl()
		},
	}
	rootCmd.AddCommand(deleteCmd)

	deleteCmd.PersistentFlags().BoolVar(&raw.recursive, "recursive", false, "Filter: Look into sub-directories recursively when deleting from container.")
	deleteCmd.PersistentFlags().StringVar(&raw.logVerbosity, "log-level", "WARNING", "define the log verbosity for the log file, available levels: INFO(all requests/responses), WARNING(slow responses), and ERROR(only failed requests).")
	deleteCmd.PersistentFlags().StringVar(&raw.include, "include", "", "only include files whose name matches the pattern list. Example: *.jpg;*.pdf;exactName")
	deleteCmd.PersistentFlags().StringVar(&raw.exclude, "exclude", "", "exclude files whose name matches the pattern list. Example: *.jpg;*.pdf;exactName")
}
