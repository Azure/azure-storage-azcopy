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

// validateProtocolCompatibility checks if the destination share protocol (SMB or NFS) is compatible with the --nfs flag
// with the type of copy operation (NFS or SMB) requested. It returns an error if there is a protocol mismatch with the --nfs flag.
func validateProtocolCompatibility(ctx context.Context,
	fromTo common.FromTo,
	destination common.ResourceString,
	dstServiceClient *common.ServiceClient,
	isNFSCopy bool) error {

	if fromTo.To() != common.ELocation.File() {
		return nil
	}

	fileURLParts, err := file.ParseURL(destination.Value)
	if err != nil {
		return err
	}
	shareName := fileURLParts.ShareName
	fileServiceClient, err := dstServiceClient.FileServiceClient()
	if err != nil {
		return err
	}

	shareClient := fileServiceClient.NewShareClient(shareName)
	properties, err := shareClient.GetProperties(ctx, nil)
	if err != nil {
		if isNFSCopy {
			glcm.Info("Failed to fetch share properties. Assuming the transfer to Azure Files NFS share")
		} else {
			glcm.Info("Failed to fetch share properties. Assuming the transfer to Azure Files SMB share")
		}
		return nil
	}

	// for older account the EnablesProtocols will be nil
	if properties.EnabledProtocols == nil || *properties.EnabledProtocols == "SMB" {
		if isNFSCopy {
			return errors.New("the destination share has SMB protocol enabled. If you want to perform copy for SMB share do not use --nfs flag")
		}
	} else {
		if !isNFSCopy {
			return errors.New("the destination share has NFS protocol enabled. If you want to perform copy for NFS share please provide --nfs flag")
		}
	}
	return nil
}

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
<<<<<<< HEAD
		err = fmt.Errorf(PreservePOSIXPropertiesIncompatibilityMsg)
=======
		err = errors.New(PreservePOSIXPropertiesIncompatibilityMsg)
>>>>>>> 2f1780e1c943f30b152e87645a826df53cd54594
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
