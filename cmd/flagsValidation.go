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
	if toPreserve && !(fromTo == common.EFromTo.LocalFile() ||
		fromTo == common.EFromTo.FileLocal() ||
		fromTo == common.EFromTo.FileFile()) {
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
	} else if toPreserve && !(fromTo == common.EFromTo.LocalFile() ||
		fromTo == common.EFromTo.FileLocal() ||
		fromTo == common.EFromTo.FileFile()) {
		return fmt.Errorf("%s is set but the job is not between %s-aware resources", flagName, common.Iff(flagName == PreservePermissionsFlag, "permission", "SMB"))
	}

	if toPreserve && (fromTo.IsUpload() || fromTo.IsDownload()) &&
		runtime.GOOS != "windows" && runtime.GOOS != "linux" {
		return fmt.Errorf("%s is set but persistence for up/downloads is supported only in Windows and Linux", flagName)
	}

	return nil
}

func areBothLocationsNFSAware(fromTo common.FromTo) bool {
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

// validateProtocolCompatibility checks whether the target Azure Files share
// supports the correct protocol (NFS or SMB) based on the transfer direction
// and the presence of the --nfs flag. It attempts to fetch the share's properties,
// and falls back to an assumption (with an info message) if the check fails.
// func validateProtocolCompatibility(ctx context.Context,
// 	fromTo common.FromTo,
// 	resource common.ResourceString,
// 	serviceClient *common.ServiceClient,
// 	isNFSCopy bool) error {

// 	fileURLParts, err := file.ParseURL(resource.Value)
// 	if err != nil {
// 		return err
// 	}
// 	shareName := fileURLParts.ShareName

// 	fileServiceClient, err := serviceClient.FileServiceClient()
// 	if err != nil {
// 		return err
// 	}

// 	direction := "to"
// 	if fromTo.IsDownload() {
// 		direction = "from"
// 	}

// 	shareClient := fileServiceClient.NewShareClient(shareName)
// 	properties, err := shareClient.GetProperties(ctx, nil)
// 	if err != nil {
// 		if isNFSCopy {
// 			glcm.Info(fmt.Sprintf("Failed to fetch share properties. Assuming the transfer %s Azure Files NFS", direction))
// 		} else {
// 			glcm.Info(fmt.Sprintf("Failed to fetch share properties. Assuming the transfer %s Azure Files SMB", direction))
// 		}
// 		return nil
// 	}

// 	// For older accounts, EnabledProtocols may be nil
// 	if properties.EnabledProtocols == nil || *properties.EnabledProtocols == "SMB" {
// 		if isNFSCopy {
// 			return fmt.Errorf("The %s share has SMB protocol enabled. If you want to perform a copy %s an SMB share, do not use the --nfs flag", shareName, direction)
// 		}
// 	} else {
// 		if !isNFSCopy {
// 			return fmt.Errorf("The %s share has NFS protocol enabled. If you want to perform a copy %s an NFS share, please provide the --nfs flag", shareName, direction)
// 		}
// 	}

// 	return nil
// }

// GetPreserveInfoFlagDefault returns the default value for the 'preserve-info' flag
// based on the operating system and the copy type (NFS or SMB).
// The default value is:
// - true if it's an NFS copy on Linux or an SMB copy on Windows.
// - false otherwise.
//
// This default behavior ensures that file preservation logic is aligned with the OS and copy type.
func GetPreserveInfoFlagDefault(cmd *cobra.Command, isNFSCopy bool) bool {
	// For Linux systems, if it's an NFS copy, we set the default value of preserveInfo to true.
	// For Windows systems, if it's an SMB copy, we set the default value of preserveInfo to true.
	// These default values are important to set here for the logic of file preservation based on the system and copy type.
	return (runtime.GOOS == "linux" && isNFSCopy) || (runtime.GOOS == "windows" && !isNFSCopy)
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
	isNFSCopy,
	preserveInfo,
	preservePermissions,
	preserveSMBInfo,
	preserveSMBPermissions bool) (isNFSCopyVal bool, preserveInfoVal bool, preservePermissionsVal common.PreservePermissionsOption, err error) {

	if (preserveSMBInfo && runtime.GOOS == "linux") || preserveSMBPermissions {
		err = errors.New(InvalidFlagsForNFSMsg)
		return
	}
	isNFSCopyVal = isNFSCopy
	preserveInfoVal = preserveInfo && areBothLocationsNFSAware(fromTo)
	if err = validatePreserveNFSPropertyOption(preserveInfoVal,
		fromTo,
		PreserveInfoFlag); err != nil {
		return
	}

	isUserPersistingPermissions := preservePermissions
	if preserveInfoVal && !isUserPersistingPermissions {
		glcm.Info(PreserveNFSPermissionsDisabledMsg)
	}
	if err = validatePreserveNFSPropertyOption(isUserPersistingPermissions,
		fromTo,
		PreservePermissionsFlag); err != nil {
		return
	}
	//TBD: We will be preserving ACLs and ownership info in case of NFS. (UserID,GroupID and FileMode)
	// Using the same EPreservePermissionsOption that we have today for NFS as well
	// Please provide the feedback if we should introduce new EPreservePermissionsOption instead.
	preservePermissionsVal = common.NewPreservePermissionsOption(isUserPersistingPermissions,
		true,
		fromTo)
	return
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
	isNFSCopy,
	preserveInfo,
	preservePOSIXProperties,
	preservePermissions,
	preserveOwner,
	preserveSMBPermissions bool) (isNFSCopyVal bool, preserveInfoVal, preservePOSIXPropertiesVal bool, preservePermissionsVal common.PreservePermissionsOption, err error) {

	preserveInfoVal = preserveInfo && areBothLocationsSMBAware(fromTo)
	if err = validatePreserveSMBPropertyOption(preserveInfoVal,
		fromTo,
		PreserveInfoFlag); err != nil {
		return
	}

	preservePOSIXPropertiesVal = preservePOSIXProperties
	if preservePOSIXPropertiesVal && !areBothLocationsPOSIXAware(fromTo) {
		err = errors.New(PreservePOSIXPropertiesIncompatibilityMsg)
		return
	}

	isUserPersistingPermissions := preservePermissions || preserveSMBPermissions
	if preserveInfoVal && !isUserPersistingPermissions {
		glcm.Info(PreservePermissionsDisabledMsg)
	}

	if err = validatePreserveSMBPropertyOption(isUserPersistingPermissions,
		fromTo,
		PreservePermissionsFlag); err != nil {
		return
	}

	preservePermissionsVal = common.NewPreservePermissionsOption(isUserPersistingPermissions,
		preserveOwner,
		fromTo)
	return
}

// validateSymlinkFlag checks whether the '--follow-symlink' or '--preserve-symlink' flags
// are set for an NFS copy operation. Since symlink support is not available for NFS,
// the function returns an error if either flag is enabled.
// By default, symlink files will be skipped during NFS copy.
func validateSymlinkFlag(followSymlinks, preserveSymlinks bool) error {

	if followSymlinks == true {
		return fmt.Errorf("The '--follow-symlink' flag is not supported for NFS copy. Symlink files will be skipped by default.")

	}
	if preserveSymlinks == true {
		return fmt.Errorf("the --preserve-symlink flag is not support for NFS copy. Symlink files will be skipped by default.")
	}
	return nil
}

func validatePreserveHardlinks(option common.PreserveHardlinksOption, fromTo common.FromTo, isNFSCopy bool) error {

	// Validate for Download: Only allowed when downloading from a local file system
	if runtime.GOOS == "linux" && fromTo.IsDownload() && fromTo.From() != common.ELocation.File() {
		return fmt.Errorf("The --preserve-hardlinks option, when downloading, is only supported from a NFS file share to a Linux filesystem.")
	}

	// Validate for Upload or S2S: Only allowed when uploading *to* a local file system
	if runtime.GOOS == "linux" && (fromTo.IsUpload() || fromTo.IsS2S()) && fromTo.To() != common.ELocation.File() {
		return fmt.Errorf("The --preserve-hardlinks option, when uploading, is only supported from a NFS file share to a Linux filesystem or between NFS file shares.")
	}

	if option == common.DefaultPreserveHardlinksOption {
		glcm.Info("The --preserve-hardlinks option is set to 'follow'. Hardlinked files will be copied as a regular file at the destination.")
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

func isUnsupportedScenarioForNFS(ctx context.Context, fromTo common.FromTo, source common.ResourceString, serviceClient *common.ServiceClient) (bool, error) {
	// Check platform compatibility
	if (fromTo.IsUpload() || fromTo.IsDownload()) && runtime.GOOS != "linux" {
		return true, fmt.Errorf("NFS %s is not supported on %s. This functionality is only available on Linux.",
			fromTo.String(), runtime.GOOS)
	}

	// Check S2S: only valid if both ends are NFS shares
	if fromTo.IsS2S() {
		protocol, err := getShareProtocolType(ctx, serviceClient, source, "NFS")
		if err != nil {
			return true, err
		}
		if protocol != "NFS" {
			return true, fmt.Errorf("Service-to-service NFS transfer is only supported from NFS shares. Found %s protocol.", protocol)
		}
	}

	return false, nil
}

// getShareProtocolType returns "SMB", "NFS", or "UNKNOWN" based on the share's enabled protocols.
// If retrieval fails, it returns fallbackValue ("SMB" or "NFS" depending on context).
func getShareProtocolType(ctx context.Context,
	serviceClient *common.ServiceClient,
	resource common.ResourceString,
	fallbackValue string) (string, error) {

	fileURLParts, err := file.ParseURL(resource.Value)
	if err != nil {
		return "UNKNOWN", err
	}
	shareName := fileURLParts.ShareName

	fileServiceClient, err := serviceClient.FileServiceClient()
	if err != nil {
		return "UNKNOWN", err
	}

	shareClient := fileServiceClient.NewShareClient(shareName)
	properties, err := shareClient.GetProperties(ctx, nil)
	if err != nil {
		glcm.Info(fmt.Sprintf("Failed to fetch share properties. Assuming the share uses %s protocol.", fallbackValue))
		return fallbackValue, nil
	}

	if properties.EnabledProtocols == nil {
		return "SMB", nil // Default assumption
	}

	return *properties.EnabledProtocols, nil
}

func validateProtocolCompatibility(
	ctx context.Context,
	fromTo common.FromTo,
	resource common.ResourceString,
	serviceClient *common.ServiceClient,
	isNFSCopy bool,
) error {

	direction := "to"
	if fromTo.IsDownload() {
		direction = "from"
	}

	// Use the helper function to get protocol type
	fallback := "NFS"
	if !isNFSCopy {
		fallback = "SMB"
	}

	protocol, err := getShareProtocolType(ctx, serviceClient, resource, fallback)
	if err != nil {
		return err
	}

	if protocol == "SMB" && isNFSCopy {
		return fmt.Errorf(
			"The target share has SMB protocol enabled. To copy %s an SMB share, do not use the --nfs flag",
			direction,
		)
	}

	if protocol == "NFS" && !isNFSCopy {
		return fmt.Errorf(
			"The target share has NFS protocol enabled. To copy %s an NFS share, please provide the --nfs flag",
			direction,
		)
	}

	// Otherwise, protocol and --nfs flag are compatible
	return nil
}
