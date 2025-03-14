//go:build windows

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
			"If NFS flag and preserve-info flag is set to true we will preserve info",
			rawCopyCmdArgs{
				preserveSMBInfo: true, // by default
				preserveInfo:    true, // by the user
				isNFSCopy:       true, // by the user
				fromTo:          "LocalFile",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(true, cooked.isNFSCopy)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If NFS flag is set to true we will not preserve anything for windows",
			rawCopyCmdArgs{
				preserveSMBInfo: true,  // by default
				preserveInfo:    false, // by default
				isNFSCopy:       true,
				fromTo:          "LocalFile",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
				a.Equal(true, cooked.isNFSCopy)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If NFS and preserve-permissions flag is set to true we will preserve permissions only",
			rawCopyCmdArgs{
				preserveSMBInfo:     true,  // by default
				preserveInfo:        false, // by default
				isNFSCopy:           true,
				preservePermissions: true,
				fromTo:              "LocalFile",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
				a.Equal(true, cooked.isNFSCopy)
				a.Equal(common.PreservePermissionsOption(2), cooked.preservePermissions)
			},
		},
		{
			"If NFS preserve-info and preserve-permissions flag is set to true we will preserve info and permissions",
			rawCopyCmdArgs{
				preserveSMBInfo:     true, // by default
				isNFSCopy:           true,
				preservePermissions: true,
				preserveInfo:        true,
				fromTo:              "LocalFile",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(true, cooked.isNFSCopy)
				a.Equal(common.PreservePermissionsOption(2), cooked.preservePermissions)
			},
		},
		{
			"If NFS flag is not set we will preserve info and permissions by considering destination as SMB but the job might fail",
			rawCopyCmdArgs{
				preserveSMBInfo:     true, // by default
				isNFSCopy:           false,
				preservePermissions: true,
				preserveInfo:        false,
				fromTo:              "LocalFile",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
				a.Equal(false, cooked.isNFSCopy)
				a.Equal(common.PreservePermissionsOption(2), cooked.preservePermissions)
			},
		},
		{
			"If NFS flag is not set we will preserve info by considering destination as SMB but the job might fail",
			rawCopyCmdArgs{
				preserveSMBInfo:     true,  // by default
				isNFSCopy:           false, // by default
				preservePermissions: false, // by default
				preserveInfo:        true,  // by default
				fromTo:              "LocalFile",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(false, cooked.isNFSCopy)
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
			"If no flag is provided we will preserve info by default for windows",
			rawCopyCmdArgs{
				preserveSMBInfo:     true,  //default value
				preserveInfo:        true,  //default value
				preservePermissions: false, //default value
				fromTo:              "LocalFile",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If the user explicitly set preserve-smb-info value to false we will not preserve info for windows",
			rawCopyCmdArgs{
				preserveSMBInfo: false,
				fromTo:          "LocalFile",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If preserve-permissions flag is set to true we will preserve info and permissions only",
			rawCopyCmdArgs{
				preserveInfo:        true, //default value
				preserveSMBInfo:     true, //default value
				preservePermissions: true,
				fromTo:              "LocalFile",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.PreservePermissionsOption(2), cooked.preservePermissions)
			},
		},
		{
			"If the user explicitly set the preserve-info flag is set to false we will not preserve anything in this case",
			rawCopyCmdArgs{
				preserveSMBInfo: true, //default value
				preserveInfo:    false,
				fromTo:          "LocalFile",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
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

func TestSync_NFSSpecificValidationForFlags(t *testing.T) {
	a := assert.New(t)

	tests := []struct {
		testScenario string
		rawCommand   rawSyncCmdArgs
		expected     func(*cookedSyncCmdArgs)
	}{
		{
			"If NFS flag and preserve-info flag is set to true we will preserve info",
			rawSyncCmdArgs{
				preserveSMBInfo:   true, // by default
				preserveInfo:      true, // by the user
				isNFSCopy:         true, // by the user
				fromTo:            "LocalFile",
				dst:               "https://test.blob.core.windows.net/testcontainer",
				deleteDestination: "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(true, cooked.isNFSCopy)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If NFS flag is set to true we will not preserve anything for windows",
			rawSyncCmdArgs{
				preserveSMBInfo:   true,  // by default
				preserveInfo:      false, // by default
				isNFSCopy:         true,  // by user
				fromTo:            "LocalFile",
				dst:               "https://test.blob.core.windows.net/testcontainer",
				deleteDestination: "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
				a.Equal(true, cooked.isNFSCopy)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If NFS and preserve-permissions flag is set to true we will preserve permissions only",
			rawSyncCmdArgs{
				preserveSMBInfo:     true,  // by default
				preserveInfo:        false, // by default
				isNFSCopy:           true,  // by user
				preservePermissions: true,  // by user
				fromTo:              "LocalFile",
				dst:                 "https://test.blob.core.windows.net/testcontainer",
				deleteDestination:   "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
				a.Equal(true, cooked.isNFSCopy)
				a.Equal(common.PreservePermissionsOption(2), cooked.preservePermissions)
			},
		},
		{
			"If NFS preserve-info and preserve-permissions flag is set to true we will preserve info and permissions",
			rawSyncCmdArgs{
				preserveSMBInfo:     true, // by default
				isNFSCopy:           true, // by user
				preservePermissions: true, // by user
				preserveInfo:        true, // by user
				fromTo:              "LocalFile",
				dst:                 "https://test.blob.core.windows.net/testcontainer",
				deleteDestination:   "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(true, cooked.isNFSCopy)
				a.Equal(common.PreservePermissionsOption(2), cooked.preservePermissions)
			},
		},
		{
			"If NFS flag is not set we will preserve info and permissions by considering destination as SMB but the job might fail",
			rawSyncCmdArgs{
				preserveSMBInfo:     true,  // by default
				isNFSCopy:           false, // by default
				preservePermissions: true,  // by user
				preserveInfo:        false, // by default
				fromTo:              "LocalFile",
				dst:                 "https://test.blob.core.windows.net/testcontainer",
				deleteDestination:   "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
				a.Equal(false, cooked.isNFSCopy)
				a.Equal(common.PreservePermissionsOption(2), cooked.preservePermissions)
			},
		},
		{
			"If NFS flag is not set we will preserve info by considering destination as SMB but the job might fail",
			rawSyncCmdArgs{
				preserveSMBInfo:     true,  // by default
				isNFSCopy:           false, // by default
				preservePermissions: false, // by default
				preserveInfo:        true,  // by default
				fromTo:              "LocalFile",
				dst:                 "https://test.blob.core.windows.net/testcontainer",
				deleteDestination:   "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(false, cooked.isNFSCopy)
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
			"If no flag is provided we will preserve info by default for windows",
			rawSyncCmdArgs{
				preserveSMBInfo:     true,  //default value
				preserveInfo:        true,  //default value
				preservePermissions: false, //default value
				fromTo:              "LocalFile",
				dst:                 "https://test.file.core.windows.net/testcontainer",
				deleteDestination:   "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If the user explicitly set preserve-smb-info value to false we will not preserve info for windows",
			rawSyncCmdArgs{
				preserveSMBInfo:   false,
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
			"If preserve-permissions flag is set to true we will preserve info and permissions only",
			rawSyncCmdArgs{
				preserveInfo:        true, //default value
				preserveSMBInfo:     true, //default value
				preservePermissions: true,
				fromTo:              "LocalFile",
				dst:                 "https://test.file.core.windows.net/testcontainer",
				deleteDestination:   "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.PreservePermissionsOption(2), cooked.preservePermissions)
			},
		},
		{
			"If the user explicitly set the preserve-info flag is set to false we will not preserve anything in this case",
			rawSyncCmdArgs{
				preserveSMBInfo:   true, //default value
				preserveInfo:      false,
				fromTo:            "LocalFile",
				dst:               "https://test.file.core.windows.net/testcontainer",
				deleteDestination: "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
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
