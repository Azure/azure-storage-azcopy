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

var nfsPermPreserveXfers = map[common.FromTo]bool{
	common.EFromTo.LocalFileNFS():   true,
	common.EFromTo.FileNFSLocal():   true,
	common.EFromTo.FileNFSFileNFS(): true,
	common.EFromTo.FileNFSFileSMB(): true,
	common.EFromTo.FileSMBFileNFS(): true,
}

func validatePreserveNFSPropertyOption(toPreserve bool, fromTo common.FromTo, flagName string) error {
	// preserverInfo will be true by default for NFS-aware locations unless specified false.
	// 1. Upload (Linux -> Azure File)
	// 2. Download (Azure File -> Linux)
	// 3. S2S (Azure File -> Azure File)

	if toPreserve {
		// The user cannot preserve permissions between SMB->NFS or NFS->SMB transfers.
		if flagName == PreservePermissionsFlag && (fromTo == common.EFromTo.FileNFSFileSMB() || fromTo == common.EFromTo.FileSMBFileNFS()) {
			return fmt.Errorf("--preserve-permissions flag is not supported for cross-protocol transfers (i,e. SMB->NFS, NFS->SMB). Please remove this flag and try again.")
		} else if !nfsPermPreserveXfers[fromTo] {
			return fmt.Errorf("%s is set but the job is not between %s-aware resources", flagName, common.Iff(flagName == PreserveInfoFlag, "permission", "NFS"))
		} else if (fromTo.IsUpload() || fromTo.IsDownload()) && runtime.GOOS != "linux" {
			return fmt.Errorf("%s is set but persistence for up/downloads is supported only in Linux", flagName)
		}
	}
	return nil
}

var smbPermPreserveXfers = map[common.FromTo]bool{
	common.EFromTo.LocalFile():      true,
	common.EFromTo.FileLocal():      true,
	common.EFromTo.FileFile():       true,
	common.EFromTo.LocalFileSMB():   true,
	common.EFromTo.FileSMBLocal():   true,
	common.EFromTo.FileSMBFileSMB(): true,
}

func validatePreserveSMBPropertyOption(toPreserve bool, fromTo common.FromTo, flagName string) error {
	// preserverInfo will be true by default for SMB-aware locations unless specified false.
	// 1. Upload (Windows/Linux -> Azure File)
	// 2. Download (Azure File -> Windows/Linux)
	// 3. S2S (Azure File -> Azure File)
	if toPreserve {
		if flagName == PreservePermissionsFlag &&
			(fromTo == common.EFromTo.BlobBlob() || fromTo == common.EFromTo.BlobFSBlob() ||
				fromTo == common.EFromTo.BlobBlobFS() || fromTo == common.EFromTo.BlobFSBlobFS()) {
			// the user probably knows what they're doing if they're trying to persist permissions between blob-type endpoints.
			return nil
		} else if !smbPermPreserveXfers[fromTo] {
			return fmt.Errorf("%s is set but the job is not between %s-aware resources", flagName, common.Iff(flagName == PreservePermissionsFlag, "permission", "SMB"))
		} else if (fromTo.IsUpload() || fromTo.IsDownload()) &&
			runtime.GOOS != "windows" && runtime.GOOS != "linux" {
			return fmt.Errorf("%s is set but persistence for up/downloads is supported only in Windows and Linux", flagName)
		}
	}
	return nil
}

func areBothLocationsNFSAware(fromTo common.FromTo) bool {
	// 1. Upload (Linux -> Azure File)
	// 2. Download (Azure File -> Linux)
	// 3. S2S (Azure File -> Azure File) (Works on Windows,Linux,Mac)

	var s2sNFSXfers = map[common.FromTo]bool{
		common.EFromTo.FileNFSFileNFS(): true,
		common.EFromTo.FileNFSFileSMB(): true,
		common.EFromTo.FileSMBFileNFS(): true,
	}

	if (runtime.GOOS == "linux") &&
		(fromTo == common.EFromTo.LocalFileNFS() || fromTo == common.EFromTo.FileNFSLocal()) {
		return true
	} else if s2sNFSXfers[fromTo] {
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

// performNFSSpecificValidation performs validation specific to NFS (Network File System) configurations
// for a synchronization command. It checks NFS-related flags and settings and ensures that the necessary
// properties are set correctly for NFS copy operations.
//
// The function checks the following:
//   - Validates the "preserve-info" flag to ensure it is set correctly for NFS-aware locations.
//   - Validates the "preserve-permissions" flag, ensuring that user input is correct and provides feedback
//     if the flag is set to false and NFS info is being preserved.
//   - Ensures that both source and destination locations are NFS-aware for relevant operations.
//
// Returns:
// - An error if any validation fails, otherwise nil indicating successful validation.
func performNFSSpecificValidation(fromTo common.FromTo,
	preservePermissions common.PreservePermissionsOption,
	preserveInfo bool,
	hardlinkHandling *common.HardlinkHandlingType,
	symlinkHandling common.SymlinkHandlingType) (err error) {

	// check for unsupported NFS behavior
	if isUnsupported, err := isUnsupportedPlatformForNFS(fromTo); isUnsupported {
		return err
	}

	if err = validatePreserveNFSPropertyOption(preserveInfo,
		fromTo,
		PreserveInfoFlag); err != nil {
		return err
	}
	if err = validatePreserveNFSPropertyOption(preservePermissions.IsTruthy(),
		fromTo,
		PreservePermissionsFlag); err != nil {
		return err
	}
	// TODO: Add this check in Phase-3 which targets to support hardlinks for NFS copy.
	// if err = validateAndAdjustHardlinksFlag(hardlinkHandling, fromTo); err != nil {
	// 	return err
	// }

	if err = validateSymlinkFlag(symlinkHandling == common.ESymlinkHandlingType.Follow(),
		fromTo); err != nil {
		return err
	}
	return nil
}

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

func performSMBSpecificValidation(fromTo common.FromTo,
	preservePermissions common.PreservePermissionsOption,
	preserveInfo bool,
	preservePOSIXProperties bool) (err error) {

	if err = validatePreserveSMBPropertyOption(preserveInfo,
		fromTo,
		PreserveInfoFlag); err != nil {
		return err
	}
	if preservePOSIXProperties && !areBothLocationsPOSIXAware(fromTo) {
		return errors.New(PreservePOSIXPropertiesIncompatibilityMsg)
	}
	if err = validatePreserveSMBPropertyOption(preservePermissions.IsTruthy(),
		fromTo,
		PreservePermissionsFlag); err != nil {
		return err
	}

	// TODO: Add this check in Phase-3 which targets to support hardlinks for NFS copy.
	// if err = validateAndAdjustHardlinksFlag(hardlinkHandling, fromTo); err != nil {
	// 	return err
	// }
	return nil
}

// validateSymlinkFlag checks if the --follow-symlink flag is valid for uploading from local filesystem.
func validateSymlinkFlag(followSymlinks bool, fromTo common.FromTo) error {

	if followSymlinks {
		if fromTo.From() != common.ELocation.Local() {
			return fmt.Errorf("The '--follow-symlink' flag is only applicable when uploading from local filesystem.")
		}
	}
	return nil
}

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

func isUnsupportedPlatformForNFS(fromTo common.FromTo) (bool, error) {
	// upload and download is not supported for NFS on non-linux systems
	if (fromTo.IsUpload() || fromTo.IsDownload()) && runtime.GOOS != "linux" {
		op := "operation"
		if fromTo.IsUpload() {
			op = "upload"
		} else if fromTo.IsDownload() {
			op = "download"
		}
		return true, fmt.Errorf(
			"NFS %s is not supported on %s. This functionality is only available on Linux.",
			op,
			runtime.GOOS,
		)
	}
	return false, nil
}

func validateShareProtocolCompatibility(
	ctx context.Context,
	resource common.ResourceString,
	serviceClient *common.ServiceClient,
	isSource bool,
	protocol common.Location,
	fromTo common.FromTo,
) error {

	// We can ignore the error if we fail to get the share properties.
	shareProtocol, _ := getShareProtocolType(ctx, serviceClient, resource, protocol)

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
	serviceClient *common.ServiceClient,
	resource common.ResourceString,
	givenValue common.Location,
) (common.Location, error) {

	fileURLParts, err := file.ParseURL(resource.Value)
	if err != nil {
		return common.ELocation.Unknown(), fmt.Errorf("failed to parse resource URL: %w", err)
	}
	shareName := fileURLParts.ShareName

	fileServiceClient, err := serviceClient.FileServiceClient()
	if err != nil {
		return common.ELocation.Unknown(), fmt.Errorf("failed to create file service client: %w", err)
	}

	shareClient := fileServiceClient.NewShareClient(shareName)
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

	getUploadDownloadProtocol := func(fromTo common.FromTo) common.Location {
		switch fromTo {
		case common.EFromTo.LocalFile(), common.EFromTo.FileLocal():
			return common.ELocation.File()
		case common.EFromTo.LocalFileNFS(), common.EFromTo.FileNFSLocal():
			return common.ELocation.FileNFS()
		default:
			return common.ELocation.Unknown()
		}
	}

	var srcProtocol, dstProtocol common.Location

	// S2S Transfers
	if fromTo.IsS2S() {
		switch fromTo {
		case common.EFromTo.FileFile():
			srcProtocol, dstProtocol = common.ELocation.File(), common.ELocation.File()
		case common.EFromTo.FileNFSFileNFS():
			srcProtocol, dstProtocol = common.ELocation.FileNFS(), common.ELocation.FileNFS()
		case common.EFromTo.FileNFSFileSMB():
			srcProtocol, dstProtocol = common.ELocation.FileNFS(), common.ELocation.File()
		case common.EFromTo.FileSMBFileNFS():
			srcProtocol, dstProtocol = common.ELocation.File(), common.ELocation.FileNFS()
		}

		// Validate both source and destination
		if err := validateShareProtocolCompatibility(ctx, src, srcClient, true, srcProtocol, fromTo); err != nil {
			return err
		}
		return validateShareProtocolCompatibility(ctx, dst, dstClient, false, dstProtocol, fromTo)
	}

	// Uploads to File Shares
	if fromTo.IsUpload() {
		dstProtocol = getUploadDownloadProtocol(fromTo)
		return validateShareProtocolCompatibility(ctx, dst, dstClient, false, dstProtocol, fromTo)
	}

	// Downloads from File Shares
	if fromTo.IsDownload() {
		srcProtocol = getUploadDownloadProtocol(fromTo)
		return validateShareProtocolCompatibility(ctx, src, srcClient, true, srcProtocol, fromTo)
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
	if !userFromTo.IsNFS() {
		finalPreservePermissions = preservePermissions || preserveSMBPermissions
	}

	if userFromTo.IsNFS() && ((preserveSMBInfo && runtime.GOOS == "linux") || preserveSMBPermissions) {
		glcm.Error(InvalidFlagsForNFSMsg)
	}

	return finalPreserveInfo, finalPreservePermissions
}
