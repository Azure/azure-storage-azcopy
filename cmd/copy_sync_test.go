//go:build windows

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
		targetOS     string
		rawCommand   rawCopyCmdArgs
		expected     func(*CookedCopyCmdArgs)
	}{
		{
			"If NFS flag and preserve-info flag is set to true we will preserve info",
			"windows",
			rawCopyCmdArgs{
				preserveSMBInfo: true, // by default
				preserveInfo:    true, // by the user
				fromTo:          "LocalFileNFS",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.EFromTo.LocalFileNFS(), cooked.FromTo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If NFS flag is set to true we will not preserve anything for windows",
			"windows",
			rawCopyCmdArgs{
				preserveSMBInfo: true,  // by default
				preserveInfo:    false, // by default
				fromTo:          "LocalFileNFS",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
				a.Equal(common.EFromTo.LocalFileNFS(), cooked.FromTo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If NFS and preserve-permissions flag is set to true we will preserve permissions only",
			"windows",
			rawCopyCmdArgs{
				preserveSMBInfo:     true,  // by default
				preserveInfo:        false, // by default
				preservePermissions: true,
				fromTo:              "LocalFileNFS",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
				a.Equal(common.EFromTo.LocalFileNFS(), cooked.FromTo)
				a.Equal(common.PreservePermissionsOption(2), cooked.preservePermissions)
			},
		},
		{
			"If NFS preserve-info and preserve-permissions flag is set to true we will preserve info and permissions",
			"windows",
			rawCopyCmdArgs{
				preserveSMBInfo:     true, // by default
				preservePermissions: true,
				preserveInfo:        true,
				fromTo:              "LocalFileNFS",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.EFromTo.LocalFileNFS(), cooked.FromTo)
				a.Equal(common.PreservePermissionsOption(2), cooked.preservePermissions)
			},
		},
		{
			"If NFS flag is not set we will preserve info and permissions by considering destination as SMB but the job might fail",
			"windows",
			rawCopyCmdArgs{
				preserveSMBInfo:     true, // by default
				preservePermissions: true,
				preserveInfo:        false,
				fromTo:              "LocalFile",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
				a.Equal(common.EFromTo.LocalFile(), cooked.FromTo)
				a.Equal(common.PreservePermissionsOption(2), cooked.preservePermissions)
			},
		},
		{
			"If NFS flag is not set we will preserve info by considering destination as SMB but the job might fail",
			"windows",
			rawCopyCmdArgs{
				preserveSMBInfo:     true,  // by default
				preservePermissions: false, // by default
				preserveInfo:        true,  // by default
				fromTo:              "LocalFile",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.EFromTo.LocalFile(), cooked.FromTo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If NFS flag is not set we will not preserve anything and assume its an SMB copy",
			"linux",
			rawCopyCmdArgs{
				preserveInfo:    false, // by default
				preserveSMBInfo: false, // by default
				fromTo:          "LocalFile",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
				a.Equal(common.EFromTo.LocalFile(), cooked.FromTo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If NFS flag is set to true we will only preserve info for linux",
			"linux",
			rawCopyCmdArgs{
				preserveSMBInfo: false, // by default
				preserveInfo:    true,  // by default as the OS is linux and nfs flag is also true
				fromTo:          "LocalFileNFS",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.EFromTo.LocalFileNFS(), cooked.FromTo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If only preserve-permissions flag is set to true and NFS flag is not set it will assume a SMB copy",
			"linux",
			rawCopyCmdArgs{
				preserveSMBInfo:     false, // by default
				preservePermissions: true,  // set by user
				fromTo:              "LocalFile",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
				a.Equal(common.EFromTo.LocalFile(), cooked.FromTo)
				a.Equal(common.PreservePermissionsOption(2), cooked.preservePermissions)
			},
		},
		{
			"If NFS and preserve-permissions flag is set to true we will preserve info and permissions",
			"linux",
			rawCopyCmdArgs{
				preservePermissions: true, // set by user
				preserveInfo:        true, // this is set to true by default as OS is linux and nfs flag is also set
				fromTo:              "LocalFileNFS",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.EFromTo.LocalFileNFS(), cooked.FromTo)
				a.Equal(common.PreservePermissionsOption(2), cooked.preservePermissions)
			},
		},
		{
			"If NFS flag is set to true and preserve-info is set to false we will not preserve anything",
			"linux",
			rawCopyCmdArgs{
				preservePermissions: false, // by default
				preserveInfo:        false, // by user
				fromTo:              "LocalFileNFS",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
				a.Equal(common.EFromTo.LocalFileNFS(), cooked.FromTo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If NFS flag and preserve-smb-info flag is set to true it will fail",
			"linux",
			rawCopyCmdArgs{
				preserveSMBInfo: true, // by user
				preserveInfo:    true, // by default
				fromTo:          "LocalFileNFS",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.EFromTo.LocalFileNFS(), cooked.FromTo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If NFS flag and preserve-smb-permissions flag is set to true it will fail",
			"linux",
			rawCopyCmdArgs{
				preserveSMBPermissions: true, // by user
				preserveInfo:           true, // by default
				fromTo:                 "LocalFileNFS",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.EFromTo.LocalFileNFS(), cooked.FromTo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
	}

	// Loop over the test cases
	for _, tt := range tests {
		t.Run(tt.testScenario, func(t *testing.T) {
			if runtime.GOOS == tt.targetOS {
				tt.rawCommand.setMandatoryDefaults()
				cooked, _ := tt.rawCommand.cook()
				tt.expected(&cooked)
			}
		})
	}
}

func TestCopy_SMBSpecificValidationForFlags(t *testing.T) {
	a := assert.New(t)

	tests := []struct {
		testScenario string
		targetOS     string
		rawCommand   rawCopyCmdArgs
		expected     func(*CookedCopyCmdArgs)
		shouldFail   bool
	}{
		{
			"If no flag is provided we will preserve info by default for windows",
			"windows",
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
			false,
		},
		{
			"If the user explicitly set preserve-smb-info value to false we will not preserve info for windows",
			"windows",
			rawCopyCmdArgs{
				preserveSMBInfo: false,
				fromTo:          "LocalFile",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
			false,
		},
		{
			"If preserve-permissions flag is set to true we will preserve info and permissions only",
			"windows",
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
			false,
		},
		{
			"If the user explicitly set the preserve-info flag is set to false we will not preserve anything in this case",
			"windows",
			rawCopyCmdArgs{
				preserveSMBInfo: true, //default value
				preserveInfo:    false,
				fromTo:          "LocalFile",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
			false,
		},
		{
			"If no flag is provided we will not preserve anything for linux",
			"linux",
			rawCopyCmdArgs{
				preserveSMBInfo: false, //by default
				preserveInfo:    false, // default value will be false as OS is linux but nfs flag is not set
				fromTo:          "LocalFile",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
			false,
		},
		{
			"If we set preserve-smb-info to true we will preserve info only.",
			"linux",
			rawCopyCmdArgs{
				preserveInfo:    true, //default will be true but presedence will be given to the flag explicitly set by user.
				preserveSMBInfo: true, // set by user
				fromTo:          "LocalFile",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
			false,
		},
		{
			"If preserve-info flag is set to true we will preserve info only",
			"linux",
			rawCopyCmdArgs{
				preserveSMBInfo: false, // default value is false for linux
				preserveInfo:    true,  // set by user
				fromTo:          "LocalFile",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
			false,
		},
		{
			"If preserve-permissions flag is set to true we will set the permissions flag only but eventually the job would fail as we dont support this in linux",
			"linux",
			rawCopyCmdArgs{
				preserveSMBInfo:     false, // default value is false for linux
				preservePermissions: true,  // set by user
				fromTo:              "LocalFile",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
				a.Equal(common.PreservePermissionsOption(2), cooked.preservePermissions)
			},
			false,
		},
		{
			"If preserve-smb-info and preserve-permissions is set to true we will set both these flags but eventually the job would fail as we dont support fetching attributes in linux.",
			"linux",
			rawCopyCmdArgs{
				preserveSMBInfo:     true, // set by user
				preservePermissions: true, // set by user
				preserveInfo:        true, //default will be true but presedence will be given to the flag explicitly set by user.
				fromTo:              "LocalFile",
			},
			func(cooked *CookedCopyCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.PreservePermissionsOption(2), cooked.preservePermissions)
			},
			false,
		},
	}

	// Loop over the test cases
	for _, tt := range tests {
		t.Run(tt.testScenario, func(t *testing.T) {
			if runtime.GOOS == tt.targetOS {
				tt.rawCommand.setMandatoryDefaults()
				cooked, _ := tt.rawCommand.cook()
				tt.expected(&cooked)
			}
		})
	}
}

func TestSync_NFSSpecificValidationForFlags(t *testing.T) {
	a := assert.New(t)

	tests := []struct {
		testScenario string
		targetOS     string
		rawCommand   rawSyncCmdArgs
		expected     func(*cookedSyncCmdArgs)
	}{
		{
			"If NFS flag and preserve-info flag is set to true we will preserve info",
			"windows",
			rawSyncCmdArgs{
				preserveSMBInfo:   true, // by default
				preserveInfo:      true, // by the user
				fromTo:            "LocalFileNFS",
				dst:               "https://test.blob.core.windows.net/testcontainer",
				deleteDestination: "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.EFromTo.LocalFileNFS(), cooked.fromTo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If NFS flag is set to true we will not preserve anything for windows",
			"windows",
			rawSyncCmdArgs{
				preserveSMBInfo:   true,  // by default
				preserveInfo:      false, // by default
				fromTo:            "LocalFileNFS",
				dst:               "https://test.blob.core.windows.net/testcontainer",
				deleteDestination: "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
				a.Equal(common.EFromTo.LocalFileNFS(), cooked.fromTo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If NFS and preserve-permissions flag is set to true we will preserve permissions only",
			"windows",
			rawSyncCmdArgs{
				preserveSMBInfo:     true,  // by default
				preserveInfo:        false, // by default
				preservePermissions: true,  // by user
				fromTo:              "LocalFileNFS",
				dst:                 "https://test.blob.core.windows.net/testcontainer",
				deleteDestination:   "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
				a.Equal(common.EFromTo.LocalFileNFS(), cooked.fromTo)
				a.Equal(common.PreservePermissionsOption(2), cooked.preservePermissions)
			},
		},
		{
			"If NFS preserve-info and preserve-permissions flag is set to true we will preserve info and permissions",
			"windows",
			rawSyncCmdArgs{
				preserveSMBInfo:     true, // by default
				preservePermissions: true, // by user
				preserveInfo:        true, // by user
				fromTo:              "LocalFileNFS",
				dst:                 "https://test.blob.core.windows.net/testcontainer",
				deleteDestination:   "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.EFromTo.LocalFileNFS(), cooked.fromTo)
				a.Equal(common.PreservePermissionsOption(2), cooked.preservePermissions)
			},
		},
		{
			"If NFS flag is not set we will preserve info and permissions by considering destination as SMB but the job might fail",
			"windows",
			rawSyncCmdArgs{
				preserveSMBInfo:     true,  // by default
				preservePermissions: true,  // by user
				preserveInfo:        false, // by default
				fromTo:              "LocalFile",
				dst:                 "https://test.blob.core.windows.net/testcontainer",
				deleteDestination:   "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
				a.Equal(common.EFromTo.LocalFile(), cooked.fromTo)
				a.Equal(common.PreservePermissionsOption(2), cooked.preservePermissions)
			},
		},
		{
			"If NFS flag is not set we will preserve info by considering destination as SMB but the job might fail",
			"windows",
			rawSyncCmdArgs{
				preserveSMBInfo:     true,  // by default
				preservePermissions: false, // by default
				preserveInfo:        true,  // by default
				fromTo:              "LocalFile",
				dst:                 "https://test.blob.core.windows.net/testcontainer",
				deleteDestination:   "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.EFromTo.LocalFile(), cooked.fromTo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If NFS flag is not set we will not preserve anything and assume its an SMB copy",
			"linux",
			rawSyncCmdArgs{
				preserveSMBInfo:   false, //by default
				preserveInfo:      false, // this will be false by default
				fromTo:            "LocalFile",
				dst:               "https://test.file.core.windows.net/testcontainer",
				deleteDestination: "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
				a.Equal(common.EFromTo.LocalFile(), cooked.fromTo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If NFS flag is set to true we will only preserve info for linux",
			"linux",
			rawSyncCmdArgs{
				preserveSMBInfo:   false, //by default
				preserveInfo:      true,  // this will be true by default as the OS is linux and nfs flag is also true
				fromTo:            "LocalFileNFS",
				dst:               "https://test.file.core.windows.net/testcontainer",
				deleteDestination: "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.EFromTo.LocalFileNFS(), cooked.fromTo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If only preserve-permissions flag is set to true and NFS flag is not set it will assume a SMB copy",
			"linux",
			rawSyncCmdArgs{
				preserveSMBInfo:     false, //by default
				preservePermissions: true,  // set by user
				fromTo:              "LocalFile",
				dst:                 "https://test.file.core.windows.net/testcontainer",
				deleteDestination:   "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
				a.Equal(common.EFromTo.LocalFile(), cooked.fromTo)
				a.Equal(common.PreservePermissionsOption(2), cooked.preservePermissions)
			},
		},
		{
			"If NFS and preserve-permissions flag is set to true we will preserve info and permissions",
			"linux",
			rawSyncCmdArgs{
				preserveSMBInfo:     false, //by default
				preservePermissions: true,  // set by user
				preserveInfo:        true,  // this is set to true by default as OS is linux and nfs flag is also set
				fromTo:              "LocalFileNFS",
				dst:                 "https://test.file.core.windows.net/testcontainer",
				deleteDestination:   "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.EFromTo.LocalFileNFS(), cooked.fromTo)
				a.Equal(common.PreservePermissionsOption(2), cooked.preservePermissions)
			},
		},
		{
			"If NFS flag is set to true and preserve-info is set to false we will not preserve anything",
			"linux",
			rawSyncCmdArgs{
				preserveSMBInfo:     false, //by default
				preservePermissions: false,
				preserveInfo:        false, // by user
				fromTo:              "LocalFileNFS",
				dst:                 "https://test.file.core.windows.net/testcontainer",
				deleteDestination:   "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
				a.Equal(common.EFromTo.LocalFileNFS(), cooked.fromTo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If NFS flag is not set we will not preserve anything and assume its an SMB copy",
			"linux",
			rawSyncCmdArgs{
				preserveSMBInfo:   false, //by default
				preserveInfo:      false, // this will be false by default
				fromTo:            "LocalFile",
				dst:               "https://test.file.core.windows.net/testcontainer",
				deleteDestination: "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
				a.Equal(common.EFromTo.LocalFile(), cooked.fromTo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If NFS flag is set to true we will only preserve info for linux",
			"linux",
			rawSyncCmdArgs{
				preserveSMBInfo:   false, //by default
				preserveInfo:      true,  // this will be true by default as the OS is linux and nfs flag is also true
				fromTo:            "LocalFileNFS",
				dst:               "https://test.file.core.windows.net/testcontainer",
				deleteDestination: "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.EFromTo.LocalFileNFS(), cooked.fromTo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If only preserve-permissions flag is set to true and NFS flag is not set it will assume a SMB copy",
			"linux",
			rawSyncCmdArgs{
				preserveSMBInfo:     false, //by default
				preservePermissions: true,  // set by user
				fromTo:              "LocalFile",
				dst:                 "https://test.file.core.windows.net/testcontainer",
				deleteDestination:   "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
				a.Equal(common.EFromTo.LocalFile(), cooked.fromTo)
				a.Equal(common.PreservePermissionsOption(2), cooked.preservePermissions)
			},
		},
		{
			"If NFS and preserve-permissions flag is set to true we will preserve info and permissions",
			"linux",
			rawSyncCmdArgs{
				preserveSMBInfo:     false, //by default
				preservePermissions: true,  // set by user
				preserveInfo:        true,  // this is set to true by default as OS is linux and nfs flag is also set
				fromTo:              "LocalFileNFS",
				dst:                 "https://test.file.core.windows.net/testcontainer",
				deleteDestination:   "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.EFromTo.LocalFileNFS(), cooked.fromTo)
				a.Equal(common.PreservePermissionsOption(2), cooked.preservePermissions)
			},
		},
		{
			"If NFS flag is set to true and preserve-info is set to false we will not preserve anything",
			"linux",
			rawSyncCmdArgs{
				preserveSMBInfo:     false, //by default
				preservePermissions: false,
				preserveInfo:        false, // by user
				fromTo:              "LocalFileNFS",
				dst:                 "https://test.file.core.windows.net/testcontainer",
				deleteDestination:   "false",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(false, cooked.preserveInfo)
				a.Equal(common.EFromTo.LocalFileNFS(), cooked.fromTo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If NFS flag and preserve-smb-info flag is set to true it will fail",
			"linux",
			rawSyncCmdArgs{
				preserveSMBInfo: true, // by user
				preserveInfo:    true, // by default
				fromTo:          "LocalFileNFS",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.EFromTo.LocalFileNFS(), cooked.fromTo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
		{
			"If NFS flag and preserve-smb-permissions flag is set to true it will fail",
			"linux",
			rawSyncCmdArgs{
				preserveSMBPermissions: true, // by user
				preserveInfo:           true, // by default
				fromTo:                 "LocalFileNFS",
			},
			func(cooked *cookedSyncCmdArgs) {
				a.Equal(true, cooked.preserveInfo)
				a.Equal(common.EFromTo.LocalFileNFS(), cooked.fromTo)
				a.Equal(common.PreservePermissionsOption(0), cooked.preservePermissions)
			},
		},
	}

	// Loop over the test cases
	for _, tt := range tests {
		t.Run(tt.testScenario, func(t *testing.T) {
			if runtime.GOOS == tt.targetOS {
				cooked, _ := tt.rawCommand.cook()
				tt.expected(&cooked)
			}
		})
	}
}

func TestSync_SMBSpecificValidationForFlags(t *testing.T) {
	a := assert.New(t)

	tests := []struct {
		testScenario string
		targetOS     string
		rawCommand   rawSyncCmdArgs
		expected     func(*cookedSyncCmdArgs)
	}{
		{
			"If no flag is provided we will preserve info by default for windows",
			"windows",
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
			"windows",
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
			"windows",
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
			"windows",
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
		{
			"If no flag is provided we will not preserve anything for linux",
			"linux",
			rawSyncCmdArgs{
				preserveSMBInfo:   false, //by default
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
			"linux",
			rawSyncCmdArgs{
				preserveInfo:      true, //default will be true but presedence will be given to the flag explicitly set by user.
				preserveSMBInfo:   true, // set by user
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
			"linux",
			rawSyncCmdArgs{
				preserveSMBInfo:   false, // default value is false for linux
				preserveInfo:      true,  // by user
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
			"linux",
			rawSyncCmdArgs{
				preserveInfo:        false, //by default
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
			"linux",
			rawSyncCmdArgs{
				preserveSMBInfo:     true, // set by user
				preservePermissions: true, // set by user
				preserveInfo:        true, //default will be true but presedence will be given to the flag explicitly set by user.
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
			if runtime.GOOS == tt.targetOS {
				cooked, _ := tt.rawCommand.cook()
				tt.expected(&cooked)
			}
		})
	}
}
