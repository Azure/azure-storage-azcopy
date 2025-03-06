package cmd

import (
	"runtime"
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
				preserveInfo: true,
				isNFSCopy:    true,
				fromTo:       "LocalFile",
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
				isNFSCopy: true,
				fromTo:    "LocalFile",
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
		// {
		// 	"If NFS preserve-info and preserve-permissions flag is set to true we will preserve info and permissions",
		// 	rawCopyCmdArgs{
		// 		isNFSCopy:           true,
		// 		preservePermissions: true,
		// 		preserveInfo:        true,
		// 		fromTo:              "LocalFile",
		// 	},
		// 	func(cooked *CookedCopyCmdArgs) {
		// 		a.Equal(true, cooked.preserveInfo)
		// 		a.Equal(true, cooked.isNFSCopy)
		// 		a.Equal(common.PreservePermissionsOption(2), cooked.preservePermissions)
		// 	},
		// },
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
				preserveSMBInfo: (runtime.GOOS == "windows"), //default value
				fromTo:          "LocalFile",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If we set preserve-smb-info to false we will preserve info for windows",
			rawCopyCmdArgs{
				preserveSMBInfo: false,
				fromTo:          "LocalFile",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If preserve-permissions flag is set to true we will preserve info and permissions only",
			rawCopyCmdArgs{
				preserveSMBInfo:     (runtime.GOOS == "windows"), //default value
				preservePermissions: true,
				fromTo:              "LocalFile",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.PreservePermissionsOption(2), cooked.preservePermissions)
			},
		},
		{
			"If NFS preserve-info and preserve-permissions flag is set to true we will preserve info and permissions",
			rawCopyCmdArgs{
				preserveSMBInfo:     false,
				preservePermissions: true,
				fromTo:              "LocalFile",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.PreservePermissionsOption(2), cooked.preservePermissions)
			},
		},
		{
			"If NFS preserve-info is set to false we will preserve info.",
			rawCopyCmdArgs{
				preserveSMBInfo: (runtime.GOOS == "windows"), //default value
				preserveInfo:    false,
				fromTo:          "LocalFile",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
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
				preserveInfo:      true,
				isNFSCopy:         true,
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
				isNFSCopy:         true,
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
				isNFSCopy:           true,
				preservePermissions: true,
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
				isNFSCopy:           true,
				preservePermissions: true,
				preserveInfo:        true,
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
				preserveSMBInfo:   (runtime.GOOS == "windows"), //default value
				fromTo:            "LocalFile",
				dst:               "https://test.blob.core.windows.net/testcontainer",
				deleteDestination: "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If we set preserve-smb-info to false we will preserve info for windows",
			rawSyncCmdArgs{
				preserveSMBInfo:   false,
				fromTo:            "LocalFile",
				dst:               "https://test.blob.core.windows.net/testcontainer",
				deleteDestination: "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If preserve-permissions flag is set to true we will preserve info and permissions only",
			rawSyncCmdArgs{
				preserveSMBInfo:     (runtime.GOOS == "windows"), //default value
				preservePermissions: true,
				fromTo:              "LocalFile",
				dst:                 "https://test.blob.core.windows.net/testcontainer",
				deleteDestination:   "false",
				preserveOwner:       common.PreserveOwnerDefault,
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.PreservePermissionsOption(2), cooked.preservePermissions)
			},
		},
		{
			"If NFS preserve-info and preserve-permissions flag is set to true we will preserve info and permissions",
			rawSyncCmdArgs{
				preserveSMBInfo:     false,
				preservePermissions: true,
				fromTo:              "LocalFile",
				dst:                 "https://test.blob.core.windows.net/testcontainer",
				deleteDestination:   "false",
				preserveOwner:       common.PreserveOwnerDefault,
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.PreservePermissionsOption(2), cooked.preservePermissions)
			},
		},
		{
			"If NFS preserve-info is set to false we will preserve info.",
			rawSyncCmdArgs{
				preserveSMBInfo:   (runtime.GOOS == "windows"), //default value
				preserveInfo:      false,
				fromTo:            "LocalFile",
				dst:               "https://test.blob.core.windows.net/testcontainer",
				deleteDestination: "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
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
