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
		Short:      "Deletes blobs or containers in Azure Storage",
		Long:       `Deletes blobs or containers in Azure Storage.`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("remove command only takes 1 arguments. Passed %d arguments", len(args))
			}
			raw.src = args[0]
			srcLocationType := inferArgumentLocation(raw.src)
			if srcLocationType == common.ELocation.Blob() {
				raw.fromTo = common.EFromTo.BlobTrash().String()
			} else if srcLocationType == common.ELocation.File() {
				raw.fromTo = common.EFromTo.FileTrash().String()
			} else {
				return fmt.Errorf("invalid source type %s pased to delete. azcopy support removing blobs and files only", srcLocationType.String())
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			cooked, err := raw.cook()
			if err != nil {
				glcm.Exit("failed to parse user input due to error "+err.Error(), common.EExitCode.Error())
			}
			cooked.commandString = copyHandlerUtil{}.ConstructCommandStringFromArgs()
			err = cooked.process()
			if err != nil {
				glcm.Exit("failed to perform copy command due to error "+err.Error(), common.EExitCode.Error())
			}

			glcm.SurrenderControl()
		},
	}
	rootCmd.AddCommand(deleteCmd)

	deleteCmd.PersistentFlags().BoolVar(&raw.recursive, "recursive", false, "Filter: Look into sub-directories recursively when deleting from container.")
	deleteCmd.PersistentFlags().StringVar(&raw.logVerbosity, "log-level", "WARNING", "defines the log verbosity to be saved to log file")
	deleteCmd.PersistentFlags().StringVar(&raw.output, "output", "text", "format of the command's output, the choices include: text, json")
}
