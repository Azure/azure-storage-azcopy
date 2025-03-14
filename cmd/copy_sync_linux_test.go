//go:build linux

package cmd

import (
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
)

func TestCopy_NFSSpecificValidationForFlags(t *testing.T) {
	a := assert.New(t)

	tests := []struct {
		testScenario string
		rawCommand   rawCopyCmdArgs
		expected     func(*CookedCopyCmdArgs)
	}{
		{
			"If NFS flag is not set we will not preserve anything and assume its an SMB copy",
			rawCopyCmdArgs{
				preserveInfo: false, // this will be false by default
				isNFSCopy:    false,
				fromTo:       "LocalFile",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
				a.Equal(false, cooked.isNFSCopy)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If NFS flag is set to true we will only preserve info for linux",
			rawCopyCmdArgs{
				isNFSCopy:    true,
				preserveInfo: true, // this will be true by default as the OS is linux and nfs flag is also true
				fromTo:       "LocalFile",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(true, cooked.isNFSCopy)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If only preserve-permissions flag is set to true and NFS flag is not set it will assume a SMB copy",
			rawCopyCmdArgs{
				isNFSCopy:           false,
				preservePermissions: true,
				fromTo:              "LocalFile",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
				a.Equal(false, cooked.isNFSCopy)
				a.Equal(common.PreservePermissionsOption(2), cooked.preservePermissions)
			},
		},
		{
			"If NFS and preserve-permissions flag is set to true we will preserve info and permissions",
			rawCopyCmdArgs{
				isNFSCopy:           true,
				preservePermissions: true,
				preserveInfo:        true, // this is set to true by default as OS is linux and nfs flag is also set
				fromTo:              "LocalFile",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(true, cooked.isNFSCopy)
				a.Equal(common.PreservePermissionsOption(2), cooked.preservePermissions)
			},
		},
		{
			"If NFS flag is set to true and preserve-info is set to false we will not preserve anything",
			rawCopyCmdArgs{
				isNFSCopy:           true,
				preservePermissions: false,
				preserveInfo:        false, // by user
				fromTo:              "LocalFile",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
				a.Equal(true, cooked.isNFSCopy)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
	}

	// Loop over the test cases
	for _, tt := range tests {
		t.Run(tt.testScenario, func(t *testing.T) {
			tt.rawCommand.setMandatoryDefaults()
			cooked, _ := tt.rawCommand.cook()
			tt.expected(&cooked)
		})
	}
}

func TestCopy_SMBSpecificValidationForFlags(t *testing.T) {
	a := assert.New(t)

	tests := []struct {
		testScenario string
		rawCommand   rawCopyCmdArgs
		expected     func(*CookedCopyCmdArgs)
	}{
		{
			"If no flag is provided we will not preserve anything for linux",
			rawCopyCmdArgs{
				preserveInfo: false, // default value will be false as OS is linux but nfs flag is not set
				fromTo:       "LocalFile",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If we set preserve-smb-info to true we will preserve info only.",
			rawCopyCmdArgs{
				preserveSMBInfo: true,
				fromTo:          "LocalFile",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If preserve-info flag is set to true we will preserve info only",
			rawCopyCmdArgs{
				preserveSMBInfo: false, // default value is false for linux
				preserveInfo:    true,
				fromTo:          "LocalFile",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If preserve-permissions flag is set to true we will set the permissions flag only but eventually the job would fail as we dont support this in linux",
			rawCopyCmdArgs{
				preserveSMBInfo:     false, // default value is false for linux
				preservePermissions: true,
				fromTo:              "LocalFile",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
				a.Equal(common.PreservePermissionsOption(2), cooked.preservePermissions)
			},
		},
		{
			"If preserve-smb-info and preserve-permissions is set to true we will set both these flags but eventually the job would fail as we dont support fetching attributes in linux.",
			rawCopyCmdArgs{
				preserveSMBInfo:     true,
				preservePermissions: true,
				preserveInfo:        false, //default value is false for linux
				fromTo:              "LocalFile",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.PreservePermissionsOption(2), cooked.preservePermissions)
			},
		},
	}

	// Loop over the test cases
	for _, tt := range tests {
		t.Run(tt.testScenario, func(t *testing.T) {
			tt.rawCommand.setMandatoryDefaults()
			cooked, _ := tt.rawCommand.cook()
			tt.expected(&cooked)
		})
	}
}

func TestSync_NFSSpecificValidationForFlags(t *testing.T) {
	a := assert.New(t)

	tests := []struct {
		testScenario string
		rawCommand   rawSyncCmdArgs
		expected     func(*cookedSyncCmdArgs)
	}{
		{
			"If NFS flag is not set we will not preserve anything and assume its an SMB copy",
			rawSyncCmdArgs{
				preserveInfo:      false, // this will be false by default
				isNFSCopy:         false,
				fromTo:            "LocalFile",
				dst:               "https://test.file.core.windows.net/testcontainer",
				deleteDestination: "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
				a.Equal(false, cooked.isNFSCopy)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If NFS flag is set to true we will only preserve info for linux",
			rawSyncCmdArgs{
				isNFSCopy:         true,
				preserveInfo:      true, // this will be true by default as the OS is linux and nfs flag is also true
				fromTo:            "LocalFile",
				dst:               "https://test.file.core.windows.net/testcontainer",
				deleteDestination: "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(true, cooked.isNFSCopy)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If only preserve-permissions flag is set to true and NFS flag is not set it will assume a SMB copy",
			rawSyncCmdArgs{
				isNFSCopy:           false,
				preservePermissions: true,
				fromTo:              "LocalFile",
				dst:                 "https://test.file.core.windows.net/testcontainer",
				deleteDestination:   "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
				a.Equal(false, cooked.isNFSCopy)
				a.Equal(common.PreservePermissionsOption(2), cooked.preservePermissions)
			},
		},
		{
			"If NFS and preserve-permissions flag is set to true we will preserve info and permissions",
			rawSyncCmdArgs{
				isNFSCopy:           true,
				preservePermissions: true,
				preserveInfo:        true, // this is set to true by default as OS is linux and nfs flag is also set
				fromTo:              "LocalFile",
				dst:                 "https://test.file.core.windows.net/testcontainer",
				deleteDestination:   "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(true, cooked.isNFSCopy)
				a.Equal(common.PreservePermissionsOption(2), cooked.preservePermissions)
			},
		},
		{
			"If NFS flag is set to true and preserve-info is set to false we will not preserve anything",
			rawSyncCmdArgs{
				isNFSCopy:           true,
				preservePermissions: false,
				preserveInfo:        false, // by user
				fromTo:              "LocalFile",
				dst:                 "https://test.file.core.windows.net/testcontainer",
				deleteDestination:   "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
				a.Equal(true, cooked.isNFSCopy)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
	}

	// Loop over the test cases
	for _, tt := range tests {
		t.Run(tt.testScenario, func(t *testing.T) {
			cooked, _ := tt.rawCommand.cook()
			tt.expected(&cooked)
		})
	}
}

func TestSync_SMBSpecificValidationForFlags(t *testing.T) {
	a := assert.New(t)

	tests := []struct {
		testScenario string
		rawCommand   rawSyncCmdArgs
		expected     func(*cookedSyncCmdArgs)
	}{
		{
			"If no flag is provided we will not preserve anything for linux",
			rawSyncCmdArgs{
				preserveInfo:      false, // default value will be false as OS is linux but nfs flag is not set
				fromTo:            "LocalFile",
				dst:               "https://test.file.core.windows.net/testcontainer",
				deleteDestination: "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If we set preserve-smb-info to true we will preserve info only.",
			rawSyncCmdArgs{
				preserveSMBInfo:   true,
				fromTo:            "LocalFile",
				dst:               "https://test.file.core.windows.net/testcontainer",
				deleteDestination: "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If preserve-info flag is set to true we will preserve info only",
			rawSyncCmdArgs{
				preserveSMBInfo:   false, // default value is false for linux
				preserveInfo:      true,
				fromTo:            "LocalFile",
				dst:               "https://test.file.core.windows.net/testcontainer",
				deleteDestination: "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If preserve-permissions flag is set to true we will set the permissions flag only but eventually the job would fail as we dont support this in linux",
			rawSyncCmdArgs{
				preserveSMBInfo:     false, // default value is false for linux
				preservePermissions: true,
				fromTo:              "LocalFile",
				dst:                 "https://test.file.core.windows.net/testcontainer",
				deleteDestination:   "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
				a.Equal(common.PreservePermissionsOption(2), cooked.preservePermissions)
			},
		},
		{
			"If preserve-smb-info and preserve-permissions is set to true we will set both these flags but eventually the job would fail as we dont support fetching attributes in linux.",
			rawSyncCmdArgs{
				preserveSMBInfo:     true,
				preservePermissions: true,
				preserveInfo:        false, //default value is false for linux
				fromTo:              "LocalFile",
				dst:                 "https://test.file.core.windows.net/testcontainer",
				deleteDestination:   "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.PreservePermissionsOption(2), cooked.preservePermissions)
			},
		},
	}

	// Loop over the test cases
	for _, tt := range tests {
		t.Run(tt.testScenario, func(t *testing.T) {
			cooked, _ := tt.rawCommand.cook()
			tt.expected(&cooked)
		})
	}
}
