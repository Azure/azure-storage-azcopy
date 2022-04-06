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
	"strings"
)

func (raw *rawCopyCmdArgs) setMandatoryDefaultsForSetProperties() {
	raw.blobType = common.EBlobType.Detect().String()
	//raw.blockBlobTier = common.EBlockBlobTier.None().String()
	//raw.pageBlobTier = common.EPageBlobTier.None().String()
	raw.md5ValidationOption = common.DefaultHashValidationOption.String()
	raw.s2sInvalidMetadataHandleOption = common.DefaultInvalidMetadataHandleOption.String()
	raw.forceWrite = common.EOverwriteOption.True().String()
	raw.preserveOwner = common.PreserveOwnerDefault
}

func init() {
	raw := rawCopyCmdArgs{}

	setPropCmd := &cobra.Command{
		Use:        "set-properties",
		Aliases:    []string{},
		SuggestFor: []string{},
		// TODO: t-iverma: short and long descriptions
		Short: "Change properties of given blob",
		Long:  "",
		Args: func(cmd *cobra.Command, args []string) error {
			// we only want one arg, which is the source
			if len(args) != 1 {
				return fmt.Errorf("setproperties command only takes 1 arguments (src). Passed %d arguments", len(args))
			}

			//the resource to set properties of is set as src
			raw.src = args[0]

			//TODO what is the purpose of these lines?
			srcLocationType := InferArgumentLocation(raw.src)
			if raw.fromTo == "" {
				switch srcLocationType {
				case common.ELocation.Blob():
					raw.fromTo = common.EFromTo.BlobNone().String()
				case common.ELocation.BlobFS():
					raw.fromTo = common.EFromTo.BlobFSTrash().String()
				default:
					return fmt.Errorf("invalid source type %s to delete. azcopy support removing blobs/files/adls gen2", srcLocationType.String())
				}
			} else if raw.fromTo != "" {
				err := strings.Contains(raw.fromTo, "Trash")
				if !err {
					return fmt.Errorf("Invalid destination. Please enter a valid destination, i.e. BlobTrash, FileTrash, BlobFSTrash")
				}
			}
			raw.setMandatoryDefaultsForSetProperties()
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

			// TODO -> what is the purpose of these lines?
			if err != nil {
				glcm.Error("failed to perform set-properties command due to error: " + err.Error())
			}

			//if cooked.dryrunMode {
			//	glcm.Exit(nil, common.EExitCode.Success())
			//}
			//
			glcm.SurrenderControl()
		},
	}

	rootCmd.AddCommand(setPropCmd)

	//TODO setPropCmd.PersistentFlags().StringVar(&raw.dst, "dst", "", "The destination location of the copy operation")
	setPropCmd.PersistentFlags().StringVar(&raw.logVerbosity, "log-level", "INFO", "Define the log verbosity for the log file. Available levels include: INFO(all requests/responses), WARNING(slow responses), ERROR(only failed requests), and NONE(no output logs). (default 'INFO')")
	setPropCmd.PersistentFlags().StringVar(&raw.include, "include-pattern", "", "Include only files where the name matches the pattern list. For example: *.jpg;*.pdf;exactName")
	setPropCmd.PersistentFlags().StringVar(&raw.includePath, "include-path", "", "Include only these paths when setting property. "+
		"This option does not support wildcard characters (*). Checks relative path prefix. For example: myFolder;myFolder/subDirName/file.pdf")
	setPropCmd.PersistentFlags().StringVar(&raw.exclude, "exclude-pattern", "", "Exclude files where the name matches the pattern list. For example: *.jpg;*.pdf;exactName")
	setPropCmd.PersistentFlags().StringVar(&raw.excludePath, "exclude-path", "", "Exclude these paths when removing. "+
		"This option does not support wildcard characters (*). Checks relative path prefix. For example: myFolder;myFolder/subDirName/file.pdf")
	setPropCmd.PersistentFlags().StringVar(&raw.listOfFilesToCopy, "list-of-files", "", "Defines the location of text file which has the list of only files to be copied.")
	setPropCmd.PersistentFlags().StringVar(&raw.blockBlobTier, "block-blob-tier", "None", "Changes the access tier of the blobs to the given tier")
	setPropCmd.PersistentFlags().StringVar(&raw.pageBlobTier, "page-blob-tier", "None", "Upload page blob to Azure Storage using this blob tier. (default 'None').")
	setPropCmd.PersistentFlags().BoolVar(&raw.recursive, "recursive", false, "Look into sub-directories recursively when uploading from local file system.")
}
