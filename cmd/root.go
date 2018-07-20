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
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/spf13/cobra"
)

var azcopyAppPathFolder string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "azcopy",
	Short: "AzCopy is a CLI tool that moves data into/out of Azure Storage.",
	Long: "AzCopy " + common.AzcopyVersion +
		`
Project: github.com/Azure/azure-storage-azcopy

AzCopy is a CLI tool that moves data into/out of Azure Storage.
For this preview release, only the Azure Data Lake Storage Gen2 service is supported.
Please refer to the Github README for more information.
If you encounter any issue, please report it on Github.

The general format of the commands is: 'azcopy [command] [arguments] --[flag-name]=[flag-value]'.
`,
}

// hold a pointer to the global lifecycle controller so that commands could output messages and exit properly
var glcm = common.GetLifecycleMgr()

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute(azsAppPathFolder string) {
	azcopyAppPathFolder = azsAppPathFolder

	if err := rootCmd.Execute(); err != nil {
		glcm.ExitWithError(err.Error(), common.EExitCode.Error())
	}
}
