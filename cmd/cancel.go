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
	"github.com/Azure/azure-storage-azcopy/handlers"
	"github.com/spf13/cobra"
)

func init() {
	var commandLineInput  = ""

	// cancelCmd represents the pause command
	cancelCmd := &cobra.Command{
		Use:        "cancel",
		SuggestFor: []string{"cancl", "ancl", "cacl"},
		Short:      "cancel cancels the existing job for given JobId",
		Long:       `cancel cancels the existing job for given JobId`,
		Args: func(cmd *cobra.Command, args []string) error {
			// the cancel command requires necessarily to have an argument
			// cancel jobId -- cancel all the parts of an existing job for given jobId

			// If no argument is passed then it is not valid
			if len(args) != 1 {
				return errors.New("this command only requires jobId")
			}
			commandLineInput = args[0]
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			handlers.HandleCancelCommand(commandLineInput)
		},
	}
	rootCmd.AddCommand(cancelCmd)
}
