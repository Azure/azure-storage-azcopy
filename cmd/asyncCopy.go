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
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/spf13/cobra"
)

func (raw *rawCopyCmdArgs) setMandatoryDefaultsForSetProperties() {
	raw.blobType = common.EBlobType.Detect().String()
	raw.md5ValidationOption = common.DefaultHashValidationOption.String()
	raw.s2sInvalidMetadataHandleOption = common.DefaultInvalidMetadataHandleOption.String()
	raw.forceWrite = common.EOverwriteOption.True().String()
	raw.preserveOwner = common.PreserveOwnerDefault
}

func init() {
	raw := rawCopyCmdArgs{}

	// cpCmd represents the cp command
	asyncCopyCmd := &cobra.Command{
		Use:        "copy [source] [destination]",
		Aliases:    []string{"cp", "c"},
		SuggestFor: []string{"cpy", "cy", "mv"}, // TODO why does message appear twice on the console
		Short:      copyCmdShortDescription,     //TODO tiverma changes
		Long:       copyCmdLongDescription,
		Example:    copyCmdExample,
		Args: func(cmd *cobra.Command, args []string) error {
			//TODO tiverma what's this?
			if len(args) == 1 { // redirection
				// Enforce the usage of from-to flag when pipes are involved
				if raw.fromTo == "" {
					return fmt.Errorf("fatal: from-to argument required, PipeBlob (upload) or BlobPipe (download) is acceptable")
				}
				var userFromTo common.FromTo
				err := userFromTo.Parse(raw.fromTo) // TODO if user has not given a fromto, still compute right values. Make changes in cook method
				if err != nil || (userFromTo != common.EFromTo.PipeBlob() && userFromTo != common.EFromTo.BlobPipe()) {
					return fmt.Errorf("fatal: invalid from-to argument passed: %s", raw.fromTo)
				}

				if userFromTo == common.EFromTo.PipeBlob() {
					// Case 1: PipeBlob. Check for the std input pipe
					stdinPipeIn, err := isStdinPipeIn()
					if stdinPipeIn == false || err != nil {
						return fmt.Errorf("fatal: failed to read from Stdin due to error: %s", err)
					}
					raw.src = pipeLocation
					raw.dst = args[0]
				} else {
					// Case 2: BlobPipe. In this case if pipe is missing, content will be echoed on the terminal
					raw.src = args[0]
					raw.dst = pipeLocation
				}
			} else if len(args) == 2 { // normal copy
				raw.src = args[0]
				raw.dst = args[1]

				// under normal copy, we may ask the user questions such as whether to overwrite a file
				glcm.EnableInputWatcher()
				if cancelFromStdin {
					glcm.EnableCancelFromStdIn()
				}
			} else {
				return errors.New("wrong number of arguments, please refer to the help page on usage of this command")
			}

			raw.setMandatoryDefaultsForSetProperties()
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			cooked, err := raw.cook()
			if err != nil {
				glcm.Error("failed to parse user input due to error: " + err.Error())
			}

			glcm.Info("Scanning...")
			// TODO tiverma check if the source can be copied to destination or not in frontend

			cooked.commandString = copyHandlerUtil{}.ConstructCommandStringFromArgs()
			err = cooked.process()
			if err != nil {
				glcm.Error("failed to perform copy command due to error: " + err.Error())
			}

			if cooked.dryrunMode {
				glcm.Exit(nil, common.EExitCode.Success())
			}

			glcm.SurrenderControl()
		},
	}
	asyncCmd.AddCommand(asyncCopyCmd)

	// -- flags --
	asyncCopyCmd.PersistentFlags().StringVar(&raw.metadata, "metadata", "", "Set the given location with these key-value pairs (separated by ';') as metadata.")
	asyncCopyCmd.PersistentFlags().StringVar(&raw.fromTo, "from-to", "", "Optionally specifies the source destination combination. Valid values : BlobNone, FileNone, BlobFSNone")
	asyncCopyCmd.PersistentFlags().StringVar(&raw.logVerbosity, "log-level", "INFO", "Define the log verbosity for the log file. Available levels include: INFO(all requests/responses), WARNING(slow responses), ERROR(only failed requests), and NONE(no output logs). (default 'INFO')")
	asyncCopyCmd.PersistentFlags().StringVar(&raw.include, "include-pattern", "", "Include only files where the name matches the pattern list. For example: *.jpg;*.pdf;exactName")
	asyncCopyCmd.PersistentFlags().StringVar(&raw.includePath, "include-path", "", "Include only these paths when setting property. "+
		"This option does not support wildcard characters (*). Checks relative path prefix. For example: myFolder;myFolder/subDirName/file.pdf")
	asyncCopyCmd.PersistentFlags().StringVar(&raw.exclude, "exclude-pattern", "", "Exclude files where the name matches the pattern list. For example: *.jpg;*.pdf;exactName")
	asyncCopyCmd.PersistentFlags().StringVar(&raw.excludePath, "exclude-path", "", "Exclude these paths when removing. "+
		"This option does not support wildcard characters (*). Checks relative path prefix. For example: myFolder;myFolder/subDirName/file.pdf")
	asyncCopyCmd.PersistentFlags().StringVar(&raw.listOfFilesToCopy, "list-of-files", "", "Defines the location of text file which has the list of only files to be copied.")
	asyncCopyCmd.PersistentFlags().StringVar(&raw.blockBlobTier, "block-blob-tier", "None", "Changes the access tier of the blobs to the given tier")
	asyncCopyCmd.PersistentFlags().StringVar(&raw.pageBlobTier, "page-blob-tier", "None", "Upload page blob to Azure Storage using this blob tier. (default 'None').")
	asyncCopyCmd.PersistentFlags().BoolVar(&raw.recursive, "recursive", false, "Look into sub-directories recursively when uploading from local file system.")
	asyncCopyCmd.PersistentFlags().BoolVar(&raw.dryrun, "dry-run", false, "Prints the file paths that would be affected by this command. This flag does not affect the actual files.")
	asyncCopyCmd.PersistentFlags().StringVar(&raw.blobTags, "blob-tags", "", "Set tags on blobs to categorize data in your storage account (separated by '&')")
}
