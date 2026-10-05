package cmd

import (
	"runtime"

	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/spf13/cobra"
)

const PreserveInfoFlag = azcopy.PreserveInfoFlag
const PreservePermissionsFlag = azcopy.PreservePermissionsFlag

var validatePreserveNFSPropertyOption = azcopy.ValidatePreserveNFSPropertyOption
var validatePreserveSMBPropertyOption = azcopy.ValidatePreserveSMBPropertyOption
var areBothLocationsNFSAware = azcopy.AreBothLocationsNFSAware
var areBothLocationsSMBAware = azcopy.AreBothLocationsSMBAware
var performNFSSpecificValidation = azcopy.PerformNFSSpecificValidation
var performSMBSpecificValidation = azcopy.PerformSMBSpecificValidation
var validateHardlinksFlag = azcopy.ValidateHardlinksFlag
var validateSymlinkFlag = azcopy.ValidateSymlinkFlag
var isUnsupportedPlatformForNFS = azcopy.IsUnsupportedPlatformForNFS
var validateProtocolCompatibility = azcopy.ValidateProtocolCompatibility
var validateShareProtocolCompatibility = azcopy.ValidateShareProtocolCompatibility
var getShareProtocolType = azcopy.GetShareProtocolType

func GetPreserveInfoFlagDefault(_ *cobra.Command, fromTo common.FromTo) bool {
	return azcopy.GetPreserveInfoDefault(fromTo)
}

func ComputePreserveFlags(cmd *cobra.Command, fromTo common.FromTo, preserveInfo, preserveSMBInfo, preservePermissions, preserveSMBPermissions bool) (bool, bool) {
	finalInfo := azcopy.GetPreserveInfoDefault(fromTo)
	if cmd.Flags().Changed(azcopy.PreserveInfoFlag) {
		finalInfo = preserveInfo
	} else if cmd.Flags().Changed(PreserveSMBInfoFlag) {
		finalInfo = preserveSMBInfo
	}
	if fromTo.IsNFS() {
		if (preserveSMBInfo && runtime.GOOS == "linux") || preserveSMBPermissions {
			glcm.Error(InvalidFlagsForNFSMsg)
		}
		return finalInfo, preservePermissions
	}
	return finalInfo, preservePermissions || preserveSMBPermissions
}
