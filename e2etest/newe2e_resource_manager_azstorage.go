package e2etest

import (
	"fmt"
	blobservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	blobfscommon "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake"
	blobfsservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/service"
	fileservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type AzureAccountResourceManager struct {
	accountName string
	accountKey  string
	accountType AccountType

	armClient *ARMStorageAccount
}

// ManagementClient returns the parent management client for this storage account.
// If this was created raw from key+name, this will return nil.
// If the account is a "modern" ARM storage account, ARMStorageAccount will be returned.
// If the account is a "classic" storage account, ARMClassicStorageAccount (not yet implemented) will be returned.
func (acct *AzureAccountResourceManager) ManagementClient() *ARMStorageAccount {
	return acct.armClient
}

func (acct *AzureAccountResourceManager) AccountName() string {
	return acct.accountName
}

func (acct *AzureAccountResourceManager) AccountType() AccountType {
	return acct.accountType
}

func (acct *AzureAccountResourceManager) AvailableServices() []common.Location {
	return []common.Location{
		common.ELocation.Blob(),
		common.ELocation.BlobFS(),
		common.ELocation.File(),
	}
}

func (acct *AzureAccountResourceManager) getServiceURL(a Asserter, service common.Location) string {
	switch service {
	case common.ELocation.Blob():
		return fmt.Sprintf("https://%s.blob.core.windows.net/", acct.accountName)
	case common.ELocation.File():
		return fmt.Sprintf("https://%s.file.core.windows.net/", acct.accountName)
	case common.ELocation.BlobFS():
		return fmt.Sprintf("https://%s.dfs.core.windows.net/", acct.accountName)
	default:
		a.Error(fmt.Sprintf("Service %s is not supported by this resource manager.", service))
		return ""
	}
}

func (acct *AzureAccountResourceManager) GetService(a Asserter, location common.Location) ServiceResourceManager {
	uri := acct.getServiceURL(a, location)

	switch location {
	case common.ELocation.Blob():
		sharedKey, err := blobservice.NewSharedKeyCredential(acct.accountName, acct.accountKey)
		a.NoError("Create shared key", err)
		client, err := blobservice.NewClientWithSharedKeyCredential(uri, sharedKey, nil)
		a.NoError("Create Blob client", err)

		return &BlobServiceResourceManager{
			internalAccount: acct,
			internalClient:  client,
		}
	case common.ELocation.File():
		sharedKey, err := fileservice.NewSharedKeyCredential(acct.accountName, acct.accountKey)
		a.NoError("Create shared key", err)
		client, err := fileservice.NewClientWithSharedKeyCredential(uri, sharedKey, nil)
		a.NoError("Create File client", err)

		return &FileServiceResourceManager{
			internalAccount: acct,
			internalClient:  client,
		}
	case common.ELocation.BlobFS():
		sharedKey, err := blobfscommon.NewSharedKeyCredential(acct.accountName, acct.accountKey)
		client, err := blobfsservice.NewClientWithSharedKeyCredential(uri, sharedKey, nil)
		a.NoError("Create BlobFS client", err)

		return &BlobFSServiceResourceManager{
			internalAccount: acct,
			internalClient:  client,
		}
	default:
		return nil // GetServiceURL already covered the error
	}
}
