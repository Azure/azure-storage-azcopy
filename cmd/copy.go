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
	"errors"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/handlers"
	"github.com/spf13/cobra"
)

// TODO check file size, max is 4.75TB
func init() {
	commandLineInput := common.CopyCmdArgsAndFlags{}

	// cpCmd represents the cp command
	cpCmd := &cobra.Command{
		Use:        "copy",
		Aliases:    []string{"cp", "c"},
		SuggestFor: []string{"cpy", "cy", "mv"}, //TODO why does message appear twice on the console
		Short:      "copy(cp) moves data between two places.",
		Long: `copy(cp) moves data between two places. The most common cases are:
  - Upload local files/directories into Azure Storage.
  - Download blobs/container from Azure Storage to local file system.
  - Coming soon: Transfer files from Amazon S3 to Azure Storage.
  - Coming soon: Transfer files from Azure Storage to Amazon S3.
  - Coming soon: Transfer files from Google Storage to Azure Storage.
  - Coming soon: Transfer files from Azure Storage to Google Storage.

Usage:
  - azcopy cp <source> <destination> --flags
    - Source and destination can either be local file/directory path, or blob/container URL with a SAS token.
  - <command which pumps data to stdout> | azcopy cp <blob_url> --flags
    - This command accepts data from stdin and uploads it to a blob.
  - azcopy cp <blob_url> --flags > <destination_file_path>
    - This command downloads a blob and outputs it on stdout.
`,
		Args: func(cmd *cobra.Command, args []string) error {
			fmt.Print("number of arguments ", len(args))
			if len(args) == 1 { // redirection
				sourceType := validator{}.determineLocationType(args[0])
				if sourceType != common.Blob {
					return errors.New("the provided blob URL for redirection is not valid")
				}
				commandLineInput.BlobUrlForRedirection = args[0]

			} else if len(args) == 2 { // normal copy
				sourceType := validator{}.determineLocationType(args[0])
				if sourceType == common.Unknown {
					return errors.New("the provided source is invalid")
				}

				destinationType := validator{}.determineLocationType(args[1])
				if destinationType == common.Unknown {
					return errors.New("the provided destination is invalid")
				}

				if sourceType == common.Blob && destinationType == common.Blob || sourceType == common.Local && destinationType == common.Local {
					return errors.New("the provided source/destination pair is invalid")
				}

				commandLineInput.Source = args[0]
				commandLineInput.Destination = args[1]
				commandLineInput.SourceType = sourceType
				commandLineInput.DestinationType = destinationType

			} else { // wrong number of arguments
				return errors.New("wrong number of arguments, please refer to help page on usage of this command")
			}

			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 1 {
				handlers.HandleRedirectionCommand(commandLineInput)
			} else {
				handlers.HandleCopyCommand(commandLineInput)
			}
		},
	}

	rootCmd.AddCommand(cpCmd)

	// define the flags relevant to the cp command

	// filters
	cpCmd.PersistentFlags().StringVar(&commandLineInput.Include, "include", "", "Filter: Include these files when copying. Support use of *.")
	cpCmd.PersistentFlags().StringVar(&commandLineInput.Exclude, "exclude", "", "Filter: Exclude these files when copying. Support use of *.")
	cpCmd.PersistentFlags().BoolVar(&commandLineInput.Recursive, "recursive", false, "Filter: Look into sub-directories recursively when uploading from local file system.")
	cpCmd.PersistentFlags().BoolVar(&commandLineInput.FollowSymlinks, "follow-symlinks", false, "Filter: Follow symbolic links when uploading from local file system.")
	cpCmd.PersistentFlags().BoolVar(&commandLineInput.WithSnapshots, "with-snapshots", false, "Filter: Include the snapshots. Only valid when the source is blobs.")

	// options
	cpCmd.PersistentFlags().Uint32Var(&commandLineInput.BlockSize, "block-size", 0, "Use this block size when uploading to Azure Storage.")
	cpCmd.PersistentFlags().StringVar(&commandLineInput.BlobType, "blob-type", "block", "Upload to Azure Storage using this blob type.")
	cpCmd.PersistentFlags().StringVar(&commandLineInput.BlobTier, "blob-tier", "", "Upload to Azure Storage using this blob tier.")
	cpCmd.PersistentFlags().StringVar(&commandLineInput.Metadata, "metadata", "", "Upload to Azure Storage with these key-value pairs as metadata.")
	cpCmd.PersistentFlags().StringVar(&commandLineInput.ContentType, "content-type", "", "Specifies content type of the file. Implies no-guess-mime-type.")
	cpCmd.PersistentFlags().StringVar(&commandLineInput.ContentEncoding, "content-encoding", "", "Upload to Azure Storage using this content encoding.")
	cpCmd.PersistentFlags().BoolVar(&commandLineInput.NoGuessMimeType, "no-guess-mime-type", false, "This sets the content-type based on the extension of the file.")
	cpCmd.PersistentFlags().BoolVar(&commandLineInput.PreserveLastModifiedTime, "preserve-last-modified-time", false, "Only available when destination is file system.")
	cpCmd.PersistentFlags().BoolVar(&commandLineInput.IsaBackgroundOp, "background-op", false, "true if user has to perform the operations as a background operation")
	cpCmd.PersistentFlags().StringVar(&commandLineInput.Acl, "acl", "", "Access conditions to be used when uploading/downloading from Azure Storage.")
	cpCmd.PersistentFlags().Uint8Var(&commandLineInput.LogVerbosity, "Logging", uint8(common.LogWarning), "defines the log verbosity to be saved to log file")
}
