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
	"context"
	"errors"
	"fmt"
	"runtime"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
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

func validateShareProtocolCompatibility(
	ctx context.Context,
	resource common.ResourceString,
	serviceClient *common.ServiceClient,
	isSource bool,
	protocol string,
) error {
	if protocol == "" {
		return nil
	}

	direction := "from"
	if !isSource {
		direction = "to"
	}

	// We can ignore the error if we fail to get the share properties.
	shareProtocol, _ := getShareProtocolType(ctx, serviceClient, resource, protocol)

	if shareProtocol == "SMB" && common.IsNFSCopy() {
		return fmt.Errorf("The %s share has SMB protocol enabled. To copy %s a SMB share, use the appropriate --from-to flag value", direction, direction)
	}

	if shareProtocol == "NFS" && !common.IsNFSCopy() {
		return fmt.Errorf("The %s share has NFS protocol enabled. To copy %s a NFS share, use the appropriate --from-to flag value", direction, direction)
	}

	return nil
}

// getShareProtocolType returns "SMB", "NFS", or "UNKNOWN" based on the share's enabled protocols.
// If retrieval fails, it logs a warning and returns the fallback givenValue ("SMB" or "NFS").
func getShareProtocolType(
	ctx context.Context,
	serviceClient *common.ServiceClient,
	resource common.ResourceString,
	givenValue string,
) (string, error) {

	fileURLParts, err := file.ParseURL(resource.Value)
	if err != nil {
		return "UNKNOWN", fmt.Errorf("failed to parse resource URL: %w", err)
	}
	shareName := fileURLParts.ShareName

	fileServiceClient, err := serviceClient.FileServiceClient()
	if err != nil {
		return "UNKNOWN", fmt.Errorf("failed to create file service client: %w", err)
	}

	shareClient := fileServiceClient.NewShareClient(shareName)
	properties, err := shareClient.GetProperties(ctx, nil)
	if err != nil {
		glcm.Info(fmt.Sprintf("Warning: Failed to fetch share properties for '%s'. Assuming the share uses '%s' protocol based on --from-to flag.", shareName, givenValue))
		return givenValue, err
	}

	if properties.EnabledProtocols == nil {
		return "SMB", nil // Default assumption
	}

	return *properties.EnabledProtocols, nil
}

// Protocol compatibility validation for SMB and NFS transfers
func validateProtocolCompatibility(ctx context.Context, fromTo common.FromTo, src, dst common.ResourceString, srcClient, dstClient *common.ServiceClient) error {

	getUploadDownloadProtocol := func(fromTo common.FromTo) string {
		switch fromTo {
		case common.EFromTo.LocalFile(), common.EFromTo.FileLocal():
			return "SMB"
		case common.EFromTo.LocalFileNFS(), common.EFromTo.FileNFSLocal():
			return "NFS"
		default:
			return ""
		}
	}

	var protocol string

	// S2S Transfers
	if fromTo.IsS2S() {
		switch fromTo {
		case common.EFromTo.FileFile():
			protocol = "SMB"
		case common.EFromTo.FileNFSFileNFS():
			protocol = "NFS"
		default:
			if common.IsNFSCopy() {
				return errors.New("NFS copy is not supported for cross-protocol transfers, i.e., Files SMB to Files NFS or vice versa")
			}
		}

		// Validate both source and destination
		if err := validateShareProtocolCompatibility(ctx, src, srcClient, true, protocol); err != nil {
			return err
		}
		return validateShareProtocolCompatibility(ctx, dst, dstClient, false, protocol)
	}

	// Uploads to File Shares
	if fromTo.IsUpload() {
		protocol = getUploadDownloadProtocol(fromTo)
		return validateShareProtocolCompatibility(ctx, dst, dstClient, false, protocol)
	}

	// Downloads from File Shares
	if fromTo.IsDownload() {
		protocol = getUploadDownloadProtocol(fromTo)
		return validateShareProtocolCompatibility(ctx, src, srcClient, true, protocol)
	}

	return nil
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
