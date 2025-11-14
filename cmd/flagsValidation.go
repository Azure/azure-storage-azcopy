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
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"
	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/spf13/cobra"
)

// performSMBSpecificValidation performs validation specific to SMB (Server Message Block) configurations
// for a synchronization command. It checks SMB-related flags and settings, and ensures that necessary
// properties are set correctly for SMB copy operations.
//
// The function performs the following checks:
// - Validates the "preserve-info" flag to ensure both source and destination are SMB-aware.
// - Validates the "preserve-posix-properties" flag, ensuring both locations are POSIX-aware if set.
// - Ensures that the "preserve-permissions" flag is correctly set if SMB information is preserved.
// - Validates the preservation of file owner information based on user flags.
//
// Returns:
// - An error if any validation fails, otherwise nil indicating successful validation.

// validateAndAdjustHardlinksFlag validates and adjusts the --hardlinks option based on OS,
// transfer direction (upload, download, S2S), and source/destination types (NFS, SMB, local).
// Returns an error if the configuration is unsupported.
// This function will be added as part of Phase-3 which targets to support hardlinks for NFS copy.
// func validateAndAdjustHardlinksFlag(option *common.HardlinkHandlingType, fromTo common.FromTo) error {
// 	if !fromTo.IsNFS() {
// 		return nil
// 	}

// 	// NFS<->SMB special case: force skip
// 	if (fromTo == common.EFromTo.FileNFSFileSMB() || fromTo == common.EFromTo.FileSMBFileNFS()) &&
// 		*option != common.SkipHardlinkHandlingType {
// 		return fmt.Errorf(
// 			"For NFS->SMB and SMB->NFS transfers, '--hardlinks' must be set to 'skip'. " +
// 				"Hardlinked files are not supported between NFS and SMB and will always be skipped. " +
// 				"Please re-run with '--hardlinks=skip'.",
// 		)
// 	}

// 	// OS check: hardlinks handling only supported on Linux in case of upload and download
// 	if runtime.GOOS != "linux" && !fromTo.IsS2S() {
// 		return fmt.Errorf("The --hardlinks option is only supported on Linux.")
// 	}

// 	switch {
// 	case fromTo.IsDownload():
// 		// Must be NFS -> Local Linux
// 		if fromTo.From() != common.ELocation.FileNFS() {
// 			return fmt.Errorf("For downloads, '--hardlinks' is only supported from an NFS file share to a Linux filesystem.")
// 		}

// 	case fromTo.IsUpload():
// 		// Must be Local Linux -> NFS
// 		if fromTo.To() != common.ELocation.FileNFS() {
// 			return fmt.Errorf("For uploads, '--hardlinks' is only supported from a Linux filesystem to an NFS file share.")
// 		}

// 	case fromTo.IsS2S():
// 		// Allowed: NFS<->NFS, NFS->SMB, SMB->NFS
// 		validPairs := map[common.FromTo]bool{
// 			common.EFromTo.FileNFSFileNFS(): true,
// 			common.EFromTo.FileNFSFileSMB(): true,
// 			common.EFromTo.FileSMBFileNFS(): true,
// 		}
// 		if !validPairs[fromTo] {
// 			return fmt.Errorf("For S2S transfers, '--hardlinks' is only supported for NFS<->NFS, NFS->SMB, and SMB->NFS.")
// 		}
// 	}

// 	// Info messages
// 	switch *option {
// 	case common.SkipHardlinkHandlingType:
// 		glcm.Info("The --hardlinks option is set to 'skip'. Hardlinked files will be skipped.")
// 	case common.DefaultHardlinkHandlingType:
// 		glcm.Info("The --hardlinks option is set to 'follow'. Hardlinked files will be copied as regular files.")
// 	}

// 	return nil
// }

func validateShareProtocolCompatibility(
	ctx context.Context,
	resource common.ResourceString,
	serviceClient *common.ServiceClient,
	isSource bool,
	protocol common.Location,
	fromTo common.FromTo,
) error {

	// We can ignore the error if we fail to get the share properties.
	fileURLParts, err := file.ParseURL(resource.Value)
	if err != nil {
		return fmt.Errorf("failed to parse resource URL: %w", err)
	}
	shareName := fileURLParts.ShareName

	if serviceClient == nil {
		return fmt.Errorf("service client is nil")
	}

	fileServiceClient, err := serviceClient.FileServiceClient()
	if err != nil {
		return fmt.Errorf("failed to create file service client: %w", err)
	}
	shareClient := fileServiceClient.NewShareClient(shareName)
	shareProtocol, _ := getShareProtocolType(ctx, shareName, shareClient, protocol)

	if shareProtocol == common.ELocation.File() {
		if isSource && fromTo.From() != common.ELocation.File() {
			return errors.New("the source share has SMB protocol enabled. " +
				"To copy from a SMB share, use the appropriate --from-to flag value")
		}
		if !isSource && fromTo.To() != common.ELocation.File() {
			return errors.New("the destination share has NFS protocol enabled. " +
				"To copy to a NFS share, use the appropriate --from-to flag value")
		}
	}

	if shareProtocol == common.ELocation.FileNFS() {
		if isSource && fromTo.From() != common.ELocation.FileNFS() {
			return errors.New("the source share has NFS protocol enabled. " +
				"To copy from a NFS share, use the appropriate --from-to flag value")
		}
		if !isSource && fromTo.To() != common.ELocation.FileNFS() {
			return errors.New("the destination share has NFS protocol enabled. " +
				"To copy to a NFS share, use the appropriate --from-to flag value")
		}
	}
	return nil
}

// getShareProtocolType returns "SMB", "NFS", or "UNKNOWN" based on the share's enabled protocols.
// If retrieval fails, it logs a warning and returns the fallback givenValue ("SMB" or "NFS").
func getShareProtocolType(
	ctx context.Context,
	shareName string,
	shareClient *share.Client,
	givenValue common.Location,
) (common.Location, error) {
	properties, err := shareClient.GetProperties(ctx, nil)
	if err != nil {
		glcm.Info(fmt.Sprintf("Warning: Failed to fetch share properties for '%s'. Assuming the share uses '%s' protocol based on --from-to flag.", shareName, givenValue))
		return givenValue, err
	}

	if properties.EnabledProtocols == nil || *properties.EnabledProtocols == "SMB" {
		return common.ELocation.File(), nil // Default assumption
	}

	return common.ELocation.FileNFS(), nil
}

// Protocol compatibility validation for SMB and NFS transfers
func validateProtocolCompatibility(ctx context.Context, fromTo common.FromTo, src, dst common.ResourceString, srcClient, dstClient *common.ServiceClient) error {

	if fromTo.From().IsFile() {
		if err := validateShareProtocolCompatibility(ctx, src, srcClient, true, fromTo.From(), fromTo); err != nil {
			return err
		}
	}

	if fromTo.To().IsFile() {
		if err := validateShareProtocolCompatibility(ctx, dst, dstClient, false, fromTo.To(), fromTo); err != nil {
			return err
		}
	}

	return nil
}

// ComputePreserveFlags determines the final preserveInfo and preservePermissions flag values
// based on user inputs, deprecated flags, and validation rules.
func ComputePreserveFlags(cmd *cobra.Command, userFromTo common.FromTo, preserveInfo, preserveSMBInfo, preservePermissions, preserveSMBPermissions bool) (bool, bool) {
	// Compute default value
	preserveInfoDefaultVal := azcopy.GetPreserveInfoFlagDefault(cmd, userFromTo)

	// Final preserveInfo logic
	var finalPreserveInfo bool
	if cmd.Flags().Changed(azcopy.PreserveInfoFlag) && cmd.Flags().Changed(PreserveSMBInfoFlag) || cmd.Flags().Changed(azcopy.PreserveInfoFlag) {
		finalPreserveInfo = preserveInfo
	} else if cmd.Flags().Changed(PreserveSMBInfoFlag) {
		finalPreserveInfo = preserveSMBInfo
	} else {
		finalPreserveInfo = preserveInfoDefaultVal
	}

	// Final preservePermissions logic
	finalPreservePermissions := preservePermissions
	if !userFromTo.IsNFS() {
		finalPreservePermissions = preservePermissions || preserveSMBPermissions
	}

	if userFromTo.IsNFS() && ((preserveSMBInfo && runtime.GOOS == "linux") || preserveSMBPermissions) {
		glcm.Error(InvalidFlagsForNFSMsg)
	}

	return finalPreserveInfo, finalPreservePermissions
}
