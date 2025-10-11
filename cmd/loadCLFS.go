//go:build linux
// +build linux

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
	"github.com/spf13/cobra"
)

// this command is deprecated
var loadClfsCmd = &cobra.Command{
	Use:    "clfs",
	Short:  "This command has been deprecated.",
	Long:   "This command has been deprecated.",
	Hidden: true,
	Run: func(cmd *cobra.Command, args []string) {
		// in case the user has been running this command as part of their automated workflow, fail it so that they notice the deprecation
		glcm.Error(`CLFSLoad was a tool that could pre-stage a POSIX filesystem tree in Azure Blob, in a format that AvereOS can read and present as POSIX.
Since the tool has been deprecated, the only remaining access-point is through the NFSv3 service that AvereOS presents, and thus any data copying activities would be performed no differently than copying data to/from a network share.
`)
	},
}

func init() {
	loadCmd.AddCommand(loadClfsCmd)
}
