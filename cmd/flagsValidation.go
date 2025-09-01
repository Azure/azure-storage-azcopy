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

func validatePreserveNFSPropertyOption(toPreserve bool, fromTo common.FromTo, flagName string) error {
	// preserverInfo will be true by default for NFS-aware locations unless specified false.
	// 1. Upload (Windows/Linux -> Azure File)
	// 2. Download (Azure File -> Windows/Linux)
	// 3. S2S (Azure File -> Azure File)
	// TODO: More combination checks to be added later
	if toPreserve && !(fromTo == common.EFromTo.LocalFileNFS() ||
		fromTo == common.EFromTo.FileNFSLocal() ||
		fromTo == common.EFromTo.FileNFSFileNFS()) {
		return fmt.Errorf("%s is set but the job is not between %s-aware resources", flagName, common.Iff(flagName == PreserveInfoFlag, "permission", "NFS"))
	}

	if toPreserve && (fromTo.IsUpload() || fromTo.IsDownload()) &&
		runtime.GOOS != "windows" && runtime.GOOS != "linux" {
		return fmt.Errorf("%s is set but persistence for up/downloads is supported only in Windows and Linux", flagName)
	}

	return nil
}

func validatePreserveSMBPropertyOption(toPreserve bool, fromTo common.FromTo, flagName string) error {
	// preserverInfo will be true by default for SMB-aware locations unless specified false.
	// 1. Upload (Windows/Linux -> Azure File)
	// 2. Download (Azure File -> Windows/Linux)
	// 3. S2S (Azure File -> Azure File)
	if toPreserve && flagName == PreservePermissionsFlag &&
		(fromTo == common.EFromTo.BlobBlob() || fromTo == common.EFromTo.BlobFSBlob() || fromTo == common.EFromTo.BlobBlobFS() || fromTo == common.EFromTo.BlobFSBlobFS()) {
		// the user probably knows what they're doing if they're trying to persist permissions between blob-type endpoints.
		return nil
	} else if toPreserve && !(fromTo == common.EFromTo.LocalFileSMB() ||
		fromTo == common.EFromTo.FileSMBLocal() ||
		fromTo == common.EFromTo.FileSMBFileSMB()) {
		return fmt.Errorf("%s is set but the job is not between %s-aware resources", flagName, common.Iff(flagName == PreservePermissionsFlag, "permission", "SMB"))
	}

	if toPreserve && (fromTo.IsUpload() || fromTo.IsDownload()) &&
		runtime.GOOS != "windows" && runtime.GOOS != "linux" {
		return fmt.Errorf("%s is set but persistence for up/downloads is supported only in Windows and Linux", flagName)
	}

	return nil
}

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
		(fromTo == common.EFromTo.LocalFileSMB() || fromTo == common.EFromTo.FileSMBLocal()) {
		return true
	} else if fromTo == common.EFromTo.FileSMBFileSMB() {
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
	symlinkHandling common.SymlinkHandlingType,
	hardlinkHandling common.HardlinkHandlingType) (err error) {

	// check for unsupported NFS behavior
	if isUnsupported, err := isUnsupportedPlatformForNFS(fromTo); isUnsupported {
		return err
	}

	// If we are not preserving original file permissions (raw.preservePermissions == false),
	// and the operation is a file copy from azure file NFS to local linux (FromTo == FileLocal),
	// and the current OS is Linux, then we require root privileges to proceed.
	//
	// This is because modifying file ownership or permissions on Linux
	// typically requires elevated privileges. To safely handle permission
	// changes during the local file operation, we enforce that the process
	// must be running as root.
	if !preservePermissions.IsTruthy() && fromTo == common.EFromTo.FileNFSLocal() {
		if err := common.EnsureRunningAsRoot(); err != nil {
			return fmt.Errorf("failed to copy source to destination without preserving permissions: operation not permitted. Please retry with root privileges or use the default option (--preserve-permissions=true)")
		}
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

	if err = validateSymlinkFlag(symlinkHandling == common.ESymlinkHandlingType.Follow(), symlinkHandling == common.ESymlinkHandlingType.Preserve()); err != nil {
		return err
	}

	if err = validateHardlinksFlag(hardlinkHandling, fromTo); err != nil {
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
	preservePOSIXProperties bool,
	hardlinkHandling common.HardlinkHandlingType) (err error) {

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
	if err = validateHardlinksFlag(hardlinkHandling, fromTo); err != nil {
		return err
	}
	return nil
}

// validateSymlinkFlag checks whether the '--follow-symlink' or '--preserve-symlink' flags
// are set for an NFS copy operation. Since symlink support is not available for NFS,
// the function returns an error if either flag is enabled.
// By default, symlink files will be skipped during NFS copy.
func validateSymlinkFlag(followSymlinks, preserveSymlinks bool) error {

	if followSymlinks {
		return fmt.Errorf("The '--follow-symlink' flag is not supported for NFS copy. Symlink files will be skipped by default.")

	}
	if preserveSymlinks {
		return fmt.Errorf("the --preserve-symlink flag is not support for NFS copy. Symlink files will be skipped by default.")
	}
	return nil
}

func validateHardlinksFlag(option common.HardlinkHandlingType, fromTo common.FromTo) error {

	// Validate for Download: Only allowed when downloading from an NFS share to a Linux filesystem
	if common.IsNFSCopy() {
		if runtime.GOOS == "linux" && fromTo.IsDownload() && (fromTo.From() != common.ELocation.FileNFS()) {
			return fmt.Errorf("The --hardlinks option, when downloading, is only supported from a NFS file share to a Linux filesystem.")
		}

		// Validate for Upload or S2S: Only allowed when uploading *to* a local file system
		if runtime.GOOS == "linux" && (fromTo.IsUpload() || fromTo.IsS2S()) && (fromTo.To() != common.ELocation.FileNFS()) {
			return fmt.Errorf("The --hardlinks option, when uploading, is only supported from a NFS file share to a Linux filesystem or between NFS file shares.")
		}
	}

	if common.IsNFSCopy() && option == common.DefaultHardlinkHandlingType {
		glcm.Info("The --hardlinks option is set to 'follow'. Hardlinked files will be copied as a regular file at the destination.")
	}
	return nil
}

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
	protocol string,
) error {
	if protocol == "" {
		return nil
	}

	location, direction := "source", "from"
	if !isSource {
		location, direction = "destination", "to"
	}

	// We can ignore the error if we fail to get the share properties.
	shareProtocol, _ := getShareProtocolType(ctx, serviceClient, resource, protocol)

	if shareProtocol == "SMB" && common.IsNFSCopy() {
		return fmt.Errorf("The %s share has SMB protocol enabled. To copy %s a SMB share, use the appropriate --from-to flag value", location, direction)
	}

	if shareProtocol == "NFS" && !common.IsNFSCopy() {
		return fmt.Errorf("The %s share has NFS protocol enabled. To copy %s a NFS share, use the appropriate --from-to flag value", location, direction)
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
		case common.EFromTo.LocalFileSMB(), common.EFromTo.FileSMBLocal():
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
		case common.EFromTo.FileSMBFileSMB():
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
