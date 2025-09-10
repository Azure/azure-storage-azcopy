// Copyright © 2025 Microsoft <dphulkar@microsoft.com>
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
	"runtime"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/spf13/cobra"
)

func areBothLocationsNFSAware(fromTo common.FromTo) bool {
	// 1. Upload (Linux -> Azure File)
	// 2. Download (Azure File -> Linux)
	// 3. S2S (Azure File -> Azure File) (Works on Windows,Linux,Mac)
	if (runtime.GOOS == "linux") &&
		(fromTo == common.EFromTo.LocalFileNFS() || fromTo == common.EFromTo.FileNFSLocal()) {
		common.SetNFSFlag(true)
		return true
	} else if fromTo == common.EFromTo.FileNFSFileNFS() {
		common.SetNFSFlag(true)
		return true
	} else {
		return false
	}
}

func areBothLocationsSMBAware(fromTo common.FromTo) bool {
	// 1. Upload (Windows/Linux -> Azure File)
	// 2. Download (Azure File -> Windows/Linux)
	// 3. S2S (Azure File -> Azure File)
	if (runtime.GOOS == "windows" || runtime.GOOS == "linux") &&
		(fromTo == common.EFromTo.LocalFile() || fromTo == common.EFromTo.FileLocal()) {
		return true
	} else if fromTo == common.EFromTo.FileFile() {
		return true
	} else {
		return false
	}
}

// GetPreserveInfoFlagDefault returns the default value for the 'preserve-info' flag
// based on the operating system and the copy type (NFS or SMB).
// The default value is:
// - true if it's an NFS copy on Linux or share to share copy on windows or mac and an SMB copy on Windows.
// - false otherwise.
//
// This default behavior ensures that file preservation logic is aligned with the OS and copy type.
func GetPreserveInfoFlagDefault(cmd *cobra.Command, fromTo common.FromTo) bool {
	// For Linux systems, if it's an NFS copy, we set the default value of preserveInfo to true.
	// For Windows systems, if it's an SMB copy, we set the default value of preserveInfo to true.
	// These default values are important to set here for the logic of file preservation based on the system and copy type.
	return (areBothLocationsNFSAware(fromTo)) ||
		(runtime.GOOS == "windows" && areBothLocationsSMBAware(fromTo))
}

// ComputePreserveFlags determines the final preserveInfo and preservePermissions flag values
// based on user inputs, deprecated flags, and validation rules.
func ComputePreserveFlags(cmd *cobra.Command, userFromTo common.FromTo, preserveInfo, preserveSMBInfo, preservePermissions, preserveSMBPermissions bool) (bool, bool) {
	// Compute default value
	preserveInfoDefaultVal := GetPreserveInfoFlagDefault(cmd, userFromTo)

	// Final preserveInfo logic
	var finalPreserveInfo bool
	if cmd.Flags().Changed(PreserveInfoFlag) && cmd.Flags().Changed(PreserveSMBInfoFlag) || cmd.Flags().Changed(PreserveInfoFlag) {
		finalPreserveInfo = preserveInfo
	} else if cmd.Flags().Changed(PreserveSMBInfoFlag) {
		finalPreserveInfo = preserveSMBInfo
	} else {
		finalPreserveInfo = preserveInfoDefaultVal
	}

	// Final preservePermissions logic
	finalPreservePermissions := preservePermissions
	if !common.IsNFSCopy() {
		finalPreservePermissions = preservePermissions || preserveSMBPermissions
	}

	if common.IsNFSCopy() && ((preserveSMBInfo && runtime.GOOS == "linux") || preserveSMBPermissions) {
		glcm.Error(InvalidFlagsForNFSMsg)
	}

	return finalPreserveInfo, finalPreservePermissions
}
