// Copyright © Microsoft <wastore@microsoft.com>
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
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/cobra/doc"
)

var docCmdInput = struct {
	outputLocation string
	format         string
}{}

// docCmd represents the doc command
var docCmd = &cobra.Command{
	Use:   "doc",
	Short: docCmdShortDescription,
	Long:  docCmdLongDescription,
	Run: func(cmd *cobra.Command, args []string) {
		// verify the output location
		f, err := os.Stat(docCmdInput.outputLocation)
		if err != nil && os.IsNotExist(err) {
			// create the output location if it does not exist yet
			if err = os.MkdirAll(docCmdInput.outputLocation, os.ModePerm); err != nil {
				glcm.Error("Unable to create output location due to error: " + err.Error())
			}
		} else if err != nil {
			glcm.Error("Cannot access the output location due to error: " + err.Error())
		} else if !f.IsDir() {
			glcm.Error("The output location is invalid as it is pointing to a file.")
		}

		switch docCmdInput.format {
		case "wiki":
			// Generate GitHub Wiki style documentation
			generator := NewWikiGenerator(docCmdInput.outputLocation)
			err = generator.Generate(rootCmd)
			if err != nil {
				glcm.Error(fmt.Sprintf("Cannot generate wiki doc due to error %s, please contact the dev team.", err))
			}
			glcm.Info(fmt.Sprintf("GitHub Wiki documentation generated in: %s", docCmdInput.outputLocation))
		default:
			// dump the entire command tree's doc into the folder
			// it will include this command too, which is intended
			err = doc.GenMarkdownTree(rootCmd, docCmdInput.outputLocation)
			if err != nil {
				glcm.Error(fmt.Sprintf("Cannot generate doc due to error %s, please contact the dev team.", err))
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(docCmd)
	docCmd.PersistentFlags().StringVar(&docCmdInput.outputLocation, "output-location", "./doc",
		"where to put the generated markdown files")
	docCmd.PersistentFlags().StringVar(&docCmdInput.format, "format", "default",
		"output format: 'default' (cobra standard) or 'wiki' (GitHub Wiki style with tables)")
}
